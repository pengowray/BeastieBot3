using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Microsoft.Data.Sqlite;
using BeastieBot3.Taxonomy;
using BeastieBot3.WikipediaLists;

// Builds, saves and loads the Catalogue of Life placement for one IUCN database (see
// TaxonPlacement.cs for what a placement is). Reads one sample per IUCN species (the canonical
// global species-level predicate, TaxonFilterSql.GlobalSpeciesPredicate), matches each to CoL with
// ColLineageMatcher (through the caches in TaxonPlacementStore), votes with TaxonPlacementBuilder
// and stores the result in the placement file beside the CoL database.
//
// Entry points: TaxonPlacementStore.LoadOrBuild for list generation, TaxonPlacementBuild.Run for the
// `col build-placement` command (it also returns the diagnostics for the report), and
// TaxonPlacementStore.Status, which only reads.

namespace BeastieBot3.Col;

/// <summary>Progress callbacks for a placement build. Totals of 0 or less mean the size is unknown.</summary>
internal interface IPlacementBuildProgress {
    void Phase(string description, int total);
    void Advance(int count);
}

internal enum PlacementState {
    Current,
    NoFile,
    NotBuilt,
    IucnChanged,
    ColChanged,
    RulesChanged,
    ThresholdsChanged,
    Unreadable,
}

/// <summary>Whether a current placement exists for an IUCN database and a CoL database.</summary>
/// <param name="Source">The stored build for this IUCN file, or for an earlier version of it.</param>
/// <param name="Error">The SQLite error when the placement file could not be read.</param>
internal sealed record TaxonPlacementStatus(PlacementState State, string SidecarPath, string SourceKey, PlacementSourceRow? Source, string? Error = null) {
    public bool IsCurrent => State == PlacementState.Current;
}

internal sealed record PlacementRunOptions {
    /// <summary>Build even when a current placement is stored.</summary>
    public bool Force { get; init; }

    /// <summary>Build (from the caches when they are warm) so the diagnostics are available.</summary>
    public bool WantDiagnostics { get; init; }

    public PlacementBuilderOptions Builder { get; init; } = PlacementBuilderOptions.Default;
}

/// <summary>How the IUCN species were matched to CoL in one build.</summary>
/// <param name="CutShort">Matched species whose CoL chain ends at a parent row missing from the CoL database.</param>
/// <param name="FromCache">Species whose match came from the match cache.</param>
/// <param name="ColQueries">Queries sent to the CoL database (0 when everything came from the caches).</param>
internal sealed record PlacementMatchStats(
    int Species,
    int Matched,
    IReadOnlyDictionary<ColMatchKind, int> ByKind,
    int CutShort,
    int FromCache,
    int ColQueries,
    bool CachesReset);

internal sealed record PlacementRunResult(
    TaxonPlacementIndex Index,
    bool Built,
    PlacementBuildOutput? Output,
    PlacementMatchStats? Matching,
    TaxonPlacementStatus StatusBefore,
    PlacementSourceRow? Source,
    TimeSpan Elapsed);

internal static class TaxonPlacementBuild {
    /// <summary>
    /// Reads the placement file, if any, and says whether it holds a placement built from this
    /// IUCN file, this CoL file, the current algorithm and these thresholds. Opens the file
    /// read-only and changes nothing.
    /// </summary>
    public static TaxonPlacementStatus Status(string iucnDatabasePath, string colDatabasePath, PlacementBuilderOptions? options = null) {
        options ??= PlacementBuilderOptions.Default;
        var sidecar = TaxonPlacementStore.SidecarPath(colDatabasePath);
        var sourceKey = TaxonPlacementStore.SourceKey(iucnDatabasePath, TaxonPlacementStore.IucnStamp(iucnDatabasePath));
        if (!File.Exists(sidecar)) {
            return new TaxonPlacementStatus(PlacementState.NoFile, sidecar, sourceKey, null);
        }
        try {
            using var connection = OpenReadOnly(sidecar);
            if (!HasTable(connection, "placement_source")) {
                return new TaxonPlacementStatus(PlacementState.NotBuilt, sidecar, sourceKey, null);
            }
            var source = TaxonPlacementStore.ReadSource(connection, sourceKey);
            if (source is null) {
                var earlier = TaxonPlacementStore.ReadSourceForPath(connection, Path.GetFullPath(iucnDatabasePath));
                return new TaxonPlacementStatus(earlier is null ? PlacementState.NotBuilt : PlacementState.IucnChanged, sidecar, sourceKey, earlier);
            }
            var state = source.ColStamp != TaxonPlacementStore.ColStamp(colDatabasePath) ? PlacementState.ColChanged
                : source.AlgorithmVersion != TaxonPlacementStore.AlgorithmVersion ? PlacementState.RulesChanged
                : !SameThresholds(source, options) ? PlacementState.ThresholdsChanged
                : PlacementState.Current;
            return new TaxonPlacementStatus(state, sidecar, sourceKey, source);
        } catch (SqliteException ex) {
            return new TaxonPlacementStatus(PlacementState.Unreadable, sidecar, sourceKey, null, ex.Message);
        }
    }

    /// <summary>
    /// Loads the stored placement when it is current; otherwise builds it, saves it and returns it.
    /// A build with warm caches reads only the placement file and the IUCN database.
    /// </summary>
    public static PlacementRunResult Run(
        string iucnDatabasePath,
        string colDatabasePath,
        PlacementRunOptions options,
        IPlacementBuildProgress? progress,
        CancellationToken cancellationToken) {
        var clock = Stopwatch.StartNew();
        var status = Status(iucnDatabasePath, colDatabasePath, options.Builder);
        if (status.IsCurrent && !options.Force && !options.WantDiagnostics) {
            using var connection = OpenReadOnly(status.SidecarPath);
            var loaded = TaxonPlacementStore.LoadPlacement(connection, status.SourceKey);
            return new PlacementRunResult(loaded, false, null, null, status, status.Source, clock.Elapsed);
        }

        // Stamps are taken before reading, so a file that changes during the build reads as changed next time.
        var colStamp = TaxonPlacementStore.ColStamp(colDatabasePath);
        var iucnStamp = TaxonPlacementStore.IucnStamp(iucnDatabasePath);
        var sourceKey = TaxonPlacementStore.SourceKey(iucnDatabasePath, iucnStamp);

        using var store = TaxonPlacementStore.Open(status.SidecarPath);
        var cachesReset = store.ResetCachesUnlessFor(colStamp);

        progress?.Phase("Reading IUCN species", 0);
        var species = ReadIucnSpecies(iucnDatabasePath, cancellationToken);
        if (species.Count == 0) {
            throw new InvalidOperationException(
                $"No species found in {iucnDatabasePath}. The placement needs IUCN species-level global assessments.");
        }

        var (samples, stats, matcher) = MatchAll(species, store, colDatabasePath, cachesReset, progress, cancellationToken);
        using (matcher) {
            progress?.Phase("Saving the match cache", 0);
            store.SaveNodes(matcher.NewNodes);
        }

        progress?.Phase("Choosing CoL groups by vote", 0);
        var output = TaxonPlacementBuilder.Build(samples, options.Builder);

        progress?.Phase("Saving the placement", 0);
        var source = new PlacementSourceRow(
            sourceKey,
            Path.GetFullPath(iucnDatabasePath),
            iucnStamp,
            Path.GetFullPath(colDatabasePath),
            colStamp,
            TaxonPlacementStore.AlgorithmVersion,
            options.Builder.VoteThreshold,
            options.Builder.ContainmentThreshold,
            DateTime.UtcNow,
            output.SampleCount,
            output.MatchedCount,
            output.Index.Count,
            clock.Elapsed.TotalSeconds);
        store.SavePlacement(source, output);
        return new PlacementRunResult(output.Index, true, output, stats, status, source, clock.Elapsed);
    }

    private static (List<PlacementSample> Samples, PlacementMatchStats Stats, ColLineageMatcher Matcher) MatchAll(
        IReadOnlyList<IucnSpeciesKey> species,
        TaxonPlacementStore store,
        string colDatabasePath,
        bool cachesReset,
        IPlacementBuildProgress? progress,
        CancellationToken cancellationToken) {
        var cached = store.LoadSpeciesMatches();
        var matcher = new ColLineageMatcher(() => ColLineageMatcher.OpenReadOnly(colDatabasePath), store.LoadNodes());
        try {
            var samples = new List<PlacementSample>(species.Count);
            var newMatches = new List<(IucnSpeciesKey, ColSpeciesMatch)>();
            var byKind = new Dictionary<ColMatchKind, int>();
            int matched = 0, cutShort = 0, fromCache = 0, pending = 0;

            progress?.Phase("Matching IUCN species to Catalogue of Life", species.Count);
            foreach (var s in species) {
                cancellationToken.ThrowIfCancellationRequested();
                var key = TaxonPlacementStore.MatchKey(s.Kingdom, s.Genus, s.Species);
                ColSpeciesMatch match;
                if (cached.TryGetValue(key, out var hit) && StillValid(hit, s)) {
                    match = hit.Match;
                    fromCache++;
                } else {
                    match = matcher.Match(s);
                    newMatches.Add((s, match));
                    cached[key] = new CachedSpeciesMatch(match, s.ClassName, s.OrderName, s.FamilyName);
                }
                byKind[match.Kind] = byKind.GetValueOrDefault(match.Kind) + 1;

                IReadOnlyList<ColLineageNode>? nodes = null;
                if (match.AcceptedId is { } id) {
                    var lineage = matcher.LineageOf(id);
                    if (lineage.Nodes.Count > 0) {
                        nodes = lineage.Nodes;
                        matched++;
                        if (lineage.CutShort) {
                            cutShort++;
                        }
                    }
                }
                samples.Add(new PlacementSample(s.Kingdom, s.ClassName, s.OrderName, s.FamilyName, s.Genus, nodes));

                if (++pending == 500) {
                    progress?.Advance(pending);
                    pending = 0;
                }
            }
            progress?.Advance(pending);

            if (newMatches.Count > 0) {
                store.SaveSpeciesMatches(newMatches);
            }
            var stats = new PlacementMatchStats(species.Count, matched, byKind, cutShort, fromCache, matcher.ColQueries, cachesReset);
            return (samples, stats, matcher);
        } catch {
            matcher.Dispose();
            throw;
        }
    }

    // A cached match stands unless it was a choice between several CoL targets and the IUCN
    // ranks that broke the tie have changed since.
    private static bool StillValid(CachedSpeciesMatch hit, IucnSpeciesKey s) =>
        hit.Match.Candidates <= 1
        || (SameRank(hit.ClassName, s.ClassName) && SameRank(hit.OrderName, s.OrderName) && SameRank(hit.FamilyName, s.FamilyName));

    private static bool SameRank(string? a, string? b) =>
        string.Equals((a ?? string.Empty).Trim(), (b ?? string.Empty).Trim(), StringComparison.OrdinalIgnoreCase);

    private static bool SameThresholds(PlacementSourceRow source, PlacementBuilderOptions options) =>
        Math.Abs(source.VoteThreshold - options.VoteThreshold) < 1e-9
        && Math.Abs(source.ContainmentThreshold - options.ContainmentThreshold) < 1e-9;

    /// <summary>One entry per distinct IUCN species (global, species-level assessments only).</summary>
    internal static List<IucnSpeciesKey> ReadIucnSpecies(string iucnDatabasePath, CancellationToken cancellationToken) {
        using var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = iucnDatabasePath,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT DISTINCT v.kingdomName, v.className, v.orderName, v.familyName, v.genusName, v.speciesName
            FROM view_assessments_html_taxonomy_html v
            WHERE {TaxonFilterSql.GlobalSpeciesPredicate("v")}
            """;
        var result = new List<IucnSpeciesKey>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            string? Text(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
            result.Add(new IucnSpeciesKey(
                Text(0) ?? string.Empty,
                Text(1),
                Text(2),
                Text(3),
                Text(4) ?? string.Empty,
                Text(5) ?? string.Empty));
        }
        return result;
    }

    private static SqliteConnection OpenReadOnly(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        connection.Open();
        return connection;
    }

    private static bool HasTable(SqliteConnection connection, string table) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name";
        command.Parameters.AddWithValue("@name", table);
        return command.ExecuteScalar() is not null;
    }
}
