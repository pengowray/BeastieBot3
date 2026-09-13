using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Threading;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

// Streams the latest global assessment of every species and subspecies in the IUCN API cache
// (Datastore:IUCN_api_cache_sqlite), for the Wikidata IUCN status dry run.
//
// One pass over `taxa`. Each /taxa payload lists the taxon's assessments with their `latest` flag
// and scopes, so the pick needs no second table; the chosen assessment's full payload is then one
// lookup on the unique assessment_id index. Opened read-only with no schema work: this may run
// while a download is writing to the same file.
//
// "Latest" is not "global": a regional assessment (Europe, Mediterranean, ...) is also flagged
// latest, so a taxon can have several. The pick is the latest one whose scopes include Global.
// Taxa with no latest assessment at all were delisted or merged (none of them are in the 2026-1
// CSV export); taxa whose latest assessments are all regional have no global assessment to cite.
// Both are counted, never emitted.
//
// Eligible: species and subspecies. Varieties and subpopulations are skipped, matching the rest of
// the tool; the rank comes from IucnAssessmentJsonParser.ResolveInfraType so "variety" means the
// same thing here as in the CSV projection.
//
// Checked against 2026-1 (Sep 2026): 178,220 taxa emitted in about 20 seconds with a warm disk
// cache, and every one is the assessment id the 2026-1 CSV export lists as that taxon's Global
// assessment. Species match the CSV exactly (175,909).

namespace BeastieBot3.WikidataEdits;

/// What one ReadAll pass saw. Filled as the enumeration runs, complete once it finishes.
internal sealed class IucnGlobalAssessmentReadCounts {
    /// Rows in `taxa`: one per taxon fetched as its own record.
    public int TaxaSeen { get; set; }
    public int EmittedSpecies { get; set; }
    public int EmittedSubspecies { get; set; }
    public int Emitted => EmittedSpecies + EmittedSubspecies;

    public int SkippedVarieties { get; set; }
    public int SkippedSubpopulations { get; set; }

    /// No assessment flagged latest: the taxon was delisted, merged or renamed.
    public int NoLatestAssessment { get; set; }
    /// Latest assessments exist but none is Global (regional-only taxa).
    public int NoGlobalAssessment { get; set; }
    /// Of NoGlobalAssessment: at least one latest assessment has an empty scopes list.
    public int NoGlobalWithBlankScope { get; set; }

    /// More than one latest Global assessment; the most recently published was used.
    public int MultipleLatestGlobal { get; set; }
    /// The picked assessment's payload isn't in the cache yet.
    public int AssessmentNotCached { get; set; }
    /// The taxa payload, or the picked assessment's payload, couldn't be read.
    public int Unparseable { get; set; }
    /// The assessment payload names a different taxon than the taxa row it was picked from.
    public int TaxonIdMismatch { get; set; }
    /// Not emitted: the taxa payload calls the assessment latest but the assessment's own (newer)
    /// payload says latest=false. In 2026-1 all four were subspecies missing from the CSV export,
    /// with taxa rows fetched before the release. Re-downloading the taxon settles it.
    public int StaleTaxaPayload { get; set; }

    public TimeSpan Elapsed { get; set; }
}

internal sealed class IucnGlobalAssessmentReader : IDisposable {
    private readonly SqliteConnection _connection;

    private IucnGlobalAssessmentReader(SqliteConnection connection) {
        _connection = connection;
    }

    public static IucnGlobalAssessmentReader Open(string databasePath) {
        if (!File.Exists(databasePath)) {
            throw new FileNotFoundException("IUCN API cache database not found.", databasePath);
        }
        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = databasePath,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString;
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        return new IucnGlobalAssessmentReader(connection);
    }

    public IEnumerable<IucnGlobalAssessment> ReadAll(CancellationToken cancellationToken) =>
        ReadAll(new IucnGlobalAssessmentReadCounts(), cancellationToken);

    public IEnumerable<IucnGlobalAssessment> ReadAll(IucnGlobalAssessmentReadCounts counts, CancellationToken cancellationToken) {
        var started = System.Diagnostics.Stopwatch.StartNew();

        using var taxaCommand = _connection.CreateCommand();
        taxaCommand.CommandText = "SELECT root_sis_id, json FROM taxa ORDER BY id";

        using var assessmentCommand = _connection.CreateCommand();
        assessmentCommand.CommandText = "SELECT downloaded_at, json FROM assessments WHERE assessment_id = @id";
        var idParameter = assessmentCommand.Parameters.Add("@id", SqliteType.Integer);

        using var taxaReader = taxaCommand.ExecuteReader();
        while (taxaReader.Read()) {
            cancellationToken.ThrowIfCancellationRequested();
            counts.TaxaSeen++;

            var rootSisId = taxaReader.GetInt64(0);
            var pick = PickFromTaxaPayload(taxaReader.GetString(1));
            switch (pick.Outcome) {
                case TaxonPickOutcome.Unparseable: counts.Unparseable++; continue;
                case TaxonPickOutcome.Subpopulation: counts.SkippedSubpopulations++; continue;
                case TaxonPickOutcome.Variety: counts.SkippedVarieties++; continue;
                case TaxonPickOutcome.NoLatest: counts.NoLatestAssessment++; continue;
                case TaxonPickOutcome.NoGlobal:
                    counts.NoGlobalAssessment++;
                    if (pick.LatestHasBlankScope) counts.NoGlobalWithBlankScope++;
                    continue;
            }
            if (pick.LatestGlobalCount > 1) counts.MultipleLatestGlobal++;

            idParameter.Value = pick.AssessmentId;
            string? downloadedAt = null;
            string? json = null;
            using (var assessmentReader = assessmentCommand.ExecuteReader()) {
                if (assessmentReader.Read()) {
                    downloadedAt = assessmentReader.GetString(0);
                    json = assessmentReader.GetString(1);
                }
            }
            if (json is null) {
                counts.AssessmentNotCached++;
                continue;
            }

            var parsed = ParseAssessment(json, downloadedAt);
            if (parsed.Assessment is null) {
                counts.Unparseable++;
                continue;
            }
            if (parsed.Assessment.TaxonId != rootSisId) {
                counts.TaxonIdMismatch++;
                continue;
            }
            if (!parsed.LatestFlag) {
                counts.StaleTaxaPayload++;
                continue;
            }
            if (pick.Outcome == TaxonPickOutcome.Subspecies) counts.EmittedSubspecies++;
            else counts.EmittedSpecies++;

            counts.Elapsed = started.Elapsed;
            yield return parsed.Assessment;
        }
        counts.Elapsed = started.Elapsed;
    }

    /// Runs a full pass and returns only the counts.
    public IucnGlobalAssessmentReadCounts ReadSummary(CancellationToken cancellationToken) {
        var counts = new IucnGlobalAssessmentReadCounts();
        foreach (var _ in ReadAll(counts, cancellationToken)) {
        }
        return counts;
    }

    private static (IucnGlobalAssessment? Assessment, bool LatestFlag) ParseAssessment(string json, string? downloadedAt) {
        var downloadedAtUtc = IucnApiCacheStore.ParseStoredUtc(downloadedAt);
        if (downloadedAtUtc is null) return (null, false);
        JsonDocument document;
        try {
            document = JsonDocument.Parse(json);
        } catch (JsonException) {
            return (null, false);
        }
        using (document) {
            var root = document.RootElement;
            var assessment = IucnAssessmentCitationParser.Parse(root, downloadedAtUtc.Value);
            var latest = root.ValueKind == JsonValueKind.Object
                && root.TryGetProperty("latest", out var flag)
                && flag.ValueKind == JsonValueKind.True;
            return (assessment, latest);
        }
    }

    // ------------------------------------------------------------ the pick (pure)

    internal enum TaxonPickOutcome { Unparseable, Subpopulation, Variety, NoLatest, NoGlobal, Species, Subspecies }

    internal readonly record struct TaxonPick(
        TaxonPickOutcome Outcome,
        long AssessmentId = 0,
        int LatestGlobalCount = 0,
        bool LatestHasBlankScope = false);

    /// Classifies one /taxa payload and, for an eligible taxon, picks its latest Global assessment.
    /// Several latest Global assessments (none in 2026-1) resolve to the most recently published,
    /// then the highest assessment id.
    internal static TaxonPick PickFromTaxaPayload(string taxaJson) {
        JsonDocument document;
        try {
            document = JsonDocument.Parse(taxaJson);
        } catch (JsonException) {
            return new TaxonPick(TaxonPickOutcome.Unparseable);
        }
        using (document) {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("taxon", out var taxon) || taxon.ValueKind != JsonValueKind.Object) {
                return new TaxonPick(TaxonPickOutcome.Unparseable);
            }

            if (IsTrue(taxon, "subpopulation") || !string.IsNullOrWhiteSpace(StringOf(taxon, "subpopulation_name"))) {
                return new TaxonPick(TaxonPickOutcome.Subpopulation);
            }
            var infraType = IucnAssessmentJsonParser.ResolveInfraType(taxon, StringOf(taxon, "scientific_name"));
            if (string.Equals(infraType, "variety", StringComparison.Ordinal)) {
                return new TaxonPick(TaxonPickOutcome.Variety);
            }
            var eligible = infraType is null ? TaxonPickOutcome.Species : TaxonPickOutcome.Subspecies;

            if (!root.TryGetProperty("assessments", out var assessments) || assessments.ValueKind != JsonValueKind.Array) {
                return new TaxonPick(TaxonPickOutcome.NoLatest);
            }

            var latestCount = 0;
            var latestGlobalCount = 0;
            var blankScope = false;
            long bestId = 0;
            var bestYear = int.MinValue;
            foreach (var header in assessments.EnumerateArray()) {
                if (header.ValueKind != JsonValueKind.Object || !IsTrue(header, "latest")) continue;
                latestCount++;
                if (IucnAssessmentCitationParser.HasBlankScope(header)) blankScope = true;
                if (!IucnAssessmentCitationParser.HasGlobalScope(header)) continue;
                if (!TryLong(header, "assessment_id", out var id)) continue;

                latestGlobalCount++;
                var year = int.TryParse(StringOf(header, "year_published"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var y) ? y : int.MinValue;
                if (latestGlobalCount == 1 || year > bestYear || (year == bestYear && id > bestId)) {
                    bestYear = year;
                    bestId = id;
                }
            }

            if (latestCount == 0) return new TaxonPick(TaxonPickOutcome.NoLatest);
            if (latestGlobalCount == 0) return new TaxonPick(TaxonPickOutcome.NoGlobal, LatestHasBlankScope: blankScope);
            return new TaxonPick(eligible, bestId, latestGlobalCount, blankScope);
        }
    }

    private static bool IsTrue(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value)
        && (value.ValueKind == JsonValueKind.True
            || (value.ValueKind == JsonValueKind.String && bool.TryParse(value.GetString(), out var b) && b));

    private static string? StringOf(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) return null;
        return value.ValueKind switch {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            _ => null,
        };
    }

    private static bool TryLong(JsonElement element, string property, out long result) {
        result = 0;
        if (!element.TryGetProperty(property, out var value)) return false;
        return value.ValueKind switch {
            JsonValueKind.Number => value.TryGetInt64(out result),
            JsonValueKind.String => long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out result),
            _ => false,
        };
    }

    public void Dispose() {
        _connection.Dispose();
    }
}
