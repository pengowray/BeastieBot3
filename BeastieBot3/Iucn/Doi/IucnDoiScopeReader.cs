using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using BeastieBot3.Iucn.Gbif;
using BeastieBot3.SiteBuild;
using Microsoft.Data.Sqlite;
using TaxaHeader = BeastieBot3.SiteBuild.IucnAssessmentHeader;

// Finds the assessments `iucn resolve-dois` works on: those in the chosen scope that have no DOI from
// IUCN's citation text, GBIF's checklist or Wikidata (the sources `site build-db` uses, checked the
// same way by IucnDoiSelector). Every source database is opened read-only.
//
// Scopes:
//   latest-global    the CSV export's assessments whose scopes include Global, including
//                    subspecies, varieties and subpopulations (179,494 in 2026-1).
//   latest-regional  the CSV export's assessments with scopes and no Global (17,793 in 2026-1).
//   all-latest       every assessment in the CSV export, including the few with no scope.
//   history          assessments the API cache's taxon records list that are not in the CSV
//                    export: earlier assessments (168,971 in 2026-1) and a few for taxa the export
//                    leaves out. Unpublished drafts (no year published) are left out.
//
// Reading order keeps the cost down: an assessment whose GBIF or Wikidata DOI names its own ids is
// settled without reading its payload. The others' cached /api/v4/assessment payloads are read in
// row order and parsed (IucnCitationPartsParser) for the citation's DOI, the year published and the
// errata and amended years. An assessment with no cached payload (the CSV's subpopulations) keeps
// the CSV's year and has no errata or amended year.
//
// The DOI's language suffix comes from the CSV export's language column: the current release's row,
// else the previous release's row (an earlier assessment that was current then), else the taxon's
// current row, else English. An assessment in the current release's CSV and not the previous
// release's was new in the current release, which pins its DOI's release.

namespace BeastieBot3.Iucn.Doi;

internal enum DoiScope {
    LatestGlobal,
    LatestRegional,
    AllLatest,
    History,
}

internal sealed record DoiScopeSources(
    string IucnDatabase,
    string? PreviousIucnDatabase,
    string ApiCache,
    string? GbifChecklist,
    string? WikidataCache);

internal sealed class DoiScopeCounts {
    public int InScope { get; set; }
    public int FromCitation { get; set; }
    public int FromGbif { get; set; }
    public int FromWikidata { get; set; }
    public int NoPayload { get; set; }
    public int CitationUnreadable { get; set; }
    public int HistoryUnpublished { get; set; }
    public int Targets { get; set; }
}

internal sealed class DoiScopeResult {
    public required string Release { get; init; }
    public string? PreviousRelease { get; init; }
    public required List<DoiTarget> Targets { get; init; }
    public required DoiScopeCounts Counts { get; init; }
    public List<string> Skipped { get; } = new();
    /// Whether GBIF's checklist was read; it is not for the history scope, since it has only latest assessments.
    public bool GbifRead { get; init; }
}

internal static class IucnDoiScopeReader {
    private static readonly Regex ReleaseFileName = new(@"^IUCN_(?<release>\d{4}-\d)\.sqlite$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static string ScopeName(DoiScope scope) => scope switch {
        DoiScope.LatestGlobal => "latest-global",
        DoiScope.LatestRegional => "latest-regional",
        DoiScope.AllLatest => "all-latest",
        DoiScope.History => "history",
        _ => scope.ToString(),
    };

    public static DoiScope? ParseScope(string? text) => text?.Trim().ToLowerInvariant() switch {
        null or "" or "latest-global" => DoiScope.LatestGlobal,
        "latest-regional" => DoiScope.LatestRegional,
        "all-latest" => DoiScope.AllLatest,
        "history" => DoiScope.History,
        _ => null,
    };

    /// The CSV export database of the release before the current one: an IUCN_&lt;release&gt;.sqlite
    /// beside it with the newest release older than the current one. Null when there is none.
    public static string? FindPreviousRelease(string currentDatabase, string currentRelease) {
        var directory = Path.GetDirectoryName(Path.GetFullPath(currentDatabase));
        if (directory is null || !Directory.Exists(directory) || ReleaseKey(currentRelease) is not { } current) {
            return null;
        }
        string? best = null;
        (int, int)? bestKey = null;
        foreach (var file in Directory.EnumerateFiles(directory, "IUCN_*.sqlite")) {
            var match = ReleaseFileName.Match(Path.GetFileName(file));
            if (!match.Success || ReleaseKey(match.Groups["release"].Value) is not { } key) {
                continue;
            }
            if (key.CompareTo(current) < 0 && (bestKey is null || key.CompareTo(bestKey.Value) > 0)) {
                best = file;
                bestKey = key;
            }
        }
        return best;
    }

    /// (year, number) of a release such as "2025-2"; null for anything else.
    public static (int Year, int Number)? ReleaseKey(string? release) {
        if (string.IsNullOrWhiteSpace(release)) {
            return null;
        }
        var parts = release.Trim().Split('-');
        if (parts.Length != 2
            || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var year)
            || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var number)) {
            return null;
        }
        return (year, number);
    }

    private sealed record CsvRow(long AssessmentId, long TaxonId, int? Year, string? Language, IReadOnlyList<string> Regions, string Kind);

    private sealed record Candidate(long AssessmentId, long TaxonId, int? Year, string Scope, string Kind, string Language, string? NewInRelease);

    public static DoiScopeResult Read(DoiScopeSources sources, DoiScope scope, Action<string> report, CancellationToken cancellationToken) {
        var watch = Stopwatch.StartNew();

        // 1. The CSV exports.
        string release;
        var csv = new Dictionary<long, CsvRow>();
        using (var connection = SiteIucnCsvReader.OpenReadOnly(sources.IucnDatabase)) {
            release = SiteIucnCsvReader.ReadRelease(connection);
            using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT a.assessmentId, a.taxonId, a.yearPublished, a.language, a.scopes, t.infraType, t.subpopulationName
                FROM assessments_html a
                LEFT JOIN taxonomy_html t ON t.taxonId = a.taxonId
                """;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                var row = new CsvRow(
                    reader.GetInt64(0),
                    reader.GetInt64(1),
                    Year(Text(reader, 2)),
                    IucnDoiCandidates.LanguageCode(Text(reader, 3)),
                    SiteBuildRules.CsvRegions(Text(reader, 4)),
                    SiteBuildRules.KindOf(Text(reader, 5), Text(reader, 6)));
                csv[row.AssessmentId] = row;
            }
        }
        report($"Read the {release} CSV export: {csv.Count:N0} assessments.");

        string? previousRelease = null;
        var previousLanguages = new Dictionary<long, string?>();
        if (sources.PreviousIucnDatabase is { } previousPath && File.Exists(previousPath)) {
            using var connection = SiteIucnCsvReader.OpenReadOnly(previousPath);
            previousRelease = SiteIucnCsvReader.ReadRelease(connection);
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT assessmentId, language FROM assessments_html";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                previousLanguages[reader.GetInt64(0)] = IucnDoiCandidates.LanguageCode(Text(reader, 1));
            }
            report($"Read the {previousRelease} CSV export: {previousLanguages.Count:N0} assessments.");
        }
        var pinsNewAssessments = previousRelease is not null
            && ReleaseKey(previousRelease) is { } previousKey && ReleaseKey(release) is { } currentKey
            && previousKey.CompareTo(currentKey) < 0;

        // The language of each taxon's current assessments, global first.
        var taxonLanguages = new Dictionary<long, string>();
        foreach (var row in csv.Values.OrderBy(r => IsGlobal(r.Regions) ? 0 : 1).ThenBy(r => r.AssessmentId)) {
            if (row.Language is { } language) {
                taxonLanguages.TryAdd(row.TaxonId, language);
            }
        }

        // 2. The API cache's taxon records: assessment headers, for predecessors and the history scope.
        var headersByTaxon = new Dictionary<long, IReadOnlyList<TaxaHeader>>();
        using var cache = SiteIucnCsvReader.OpenReadOnly(sources.ApiCache);
        using (var command = cache.CreateCommand()) {
            command.CommandText = "SELECT root_sis_id, json FROM taxa";
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                try {
                    using var document = JsonDocument.Parse(reader.GetFieldValue<byte[]>(1));
                    headersByTaxon[reader.GetInt64(0)] = IucnTaxaHeaders.Read(document.RootElement);
                } catch (JsonException) {
                    // An unreadable record gives no headers.
                }
            }
        }
        report($"Read {headersByTaxon.Count:N0} taxon records from the IUCN API cache.");

        // 3. The assessments in scope.
        var counts = new DoiScopeCounts();
        var inScope = new List<Candidate>();
        if (scope == DoiScope.History) {
            foreach (var (rootId, headers) in headersByTaxon) {
                foreach (var header in headers) {
                    if (header.TaxonId is { } headerTaxon && headerTaxon != rootId) continue;
                    if (csv.ContainsKey(header.AssessmentId)) continue;
                    if (Year(header.YearPublished) is not { } year) {
                        counts.HistoryUnpublished++;
                        continue;
                    }
                    var language = (previousLanguages.TryGetValue(header.AssessmentId, out var previous) ? previous : null)
                        ?? (taxonLanguages.TryGetValue(rootId, out var current) ? current : null)
                        ?? "en";
                    inScope.Add(new Candidate(header.AssessmentId, rootId, year, "history", "unknown", language, null));
                }
            }
            // The taxon kind, where the taxon is in the CSV export.
            var kinds = new Dictionary<long, string>();
            foreach (var row in csv.Values) kinds.TryAdd(row.TaxonId, row.Kind);
            for (var i = 0; i < inScope.Count; i++) {
                if (kinds.TryGetValue(inScope[i].TaxonId, out var kind)) {
                    inScope[i] = inScope[i] with { Kind = kind };
                }
            }
        } else {
            foreach (var row in csv.Values) {
                var global = IsGlobal(row.Regions);
                var include = scope switch {
                    DoiScope.LatestGlobal => global,
                    DoiScope.LatestRegional => !global && row.Regions.Count > 0,
                    _ => true,
                };
                if (!include) continue;
                var label = global ? "global" : row.Regions.Count > 0 ? "regional" : "no scope";
                var newIn = pinsNewAssessments && !previousLanguages.ContainsKey(row.AssessmentId) ? release : null;
                inScope.Add(new Candidate(row.AssessmentId, row.TaxonId, row.Year, label, row.Kind, row.Language ?? "en", newIn));
            }
        }
        inScope.Sort((a, b) => a.AssessmentId.CompareTo(b.AssessmentId));
        counts.InScope = inScope.Count;
        report($"{inScope.Count:N0} assessments in scope {ScopeName(scope)}.");

        // 4. GBIF's checklist (latest assessments only) and Wikidata.
        var skipped = new List<string>();
        var gbif = new Dictionary<long, (long? AssessmentId, string Doi)>();
        var gbifRead = false;
        if (scope != DoiScope.History) {
            if (sources.GbifChecklist is { } gbifPath && File.Exists(gbifPath)) {
                var checklist = GbifIucnChecklistReader.Read(gbifPath, cancellationToken);
                foreach (var (taxonId, taxon) in checklist.Taxa) {
                    if (taxon.Doi is { } doi) {
                        gbif[taxonId] = (taxon.AssessmentId, doi);
                    }
                }
                gbifRead = true;
                report($"Read GBIF's checklist {Path.GetFileName(gbifPath)}: {gbif.Count:N0} taxa with a DOI.");
            } else {
                skipped.Add("GBIF's checklist");
            }
        }
        var wikidata = new Dictionary<long, List<string>>();
        if (sources.WikidataCache is { } wikidataPath && File.Exists(wikidataPath)) {
            ReadWikidataDois(wikidataPath, wikidata);
            report($"Read the Wikidata cache: {wikidata.Count:N0} assessments with a DOI.");
        } else {
            skipped.Add("the Wikidata cache");
        }

        // 5. Settle what GBIF or Wikidata gives for the assessment's own ids, then read payloads.
        var needPayload = new Dictionary<long, Candidate>();
        foreach (var item in inScope) {
            string? gbifDoi = gbif.TryGetValue(item.TaxonId, out var g) && g.AssessmentId == item.AssessmentId ? g.Doi : null;
            if (NamesOwnIds(gbifDoi, item)) {
                counts.FromGbif++;
                continue;
            }
            if (wikidata.TryGetValue(item.AssessmentId, out var wikidataDois) && wikidataDois.Any(d => NamesOwnIds(d, item))) {
                counts.FromWikidata++;
                continue;
            }
            needPayload[item.AssessmentId] = item;
        }

        var targets = new List<DoiTarget>();
        var rowIds = new List<(long RowId, long AssessmentId)>();
        using (var command = cache.CreateCommand()) {
            command.CommandText = "SELECT id, assessment_id FROM assessments";
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                var assessmentId = reader.GetInt64(1);
                if (needPayload.ContainsKey(assessmentId)) {
                    rowIds.Add((reader.GetInt64(0), assessmentId));
                }
            }
        }
        rowIds.Sort((a, b) => a.RowId.CompareTo(b.RowId));
        using (var command = cache.CreateCommand()) {
            command.CommandText = "SELECT json FROM assessments WHERE id = @id";
            var id = command.Parameters.Add("@id", SqliteType.Integer);
            foreach (var (rowId, assessmentId) in rowIds) {
                cancellationToken.ThrowIfCancellationRequested();
                id.Value = rowId;
                if (command.ExecuteScalar() is not { } value) {
                    continue;
                }
                var item = needPayload[assessmentId];
                needPayload.Remove(assessmentId);
                var predecessors = headersByTaxon.TryGetValue(item.TaxonId, out var headers)
                    ? IucnTaxaHeaders.PredecessorIds(headers, assessmentId)
                    : Array.Empty<long>();
                var target = FromPayload(item, value, predecessors, gbif, wikidata, counts);
                if (target is not null) {
                    targets.Add(target);
                }
            }
        }
        foreach (var item in needPayload.Values) {
            counts.NoPayload++;
            targets.Add(TargetOf(item, null, Array.Empty<long>()));
        }
        targets.Sort((a, b) => a.AssessmentId.CompareTo(b.AssessmentId));
        counts.Targets = targets.Count;
        report($"Read {rowIds.Count:N0} cached assessment payloads. Finding the assessments took {watch.Elapsed.TotalSeconds:N0} s.");

        var result = new DoiScopeResult {
            Release = release,
            PreviousRelease = previousRelease,
            Targets = targets,
            Counts = counts,
            GbifRead = gbifRead,
        };
        result.Skipped.AddRange(skipped);
        return result;
    }

    // The payload's citation DOI, or GBIF's or Wikidata's DOI checked against the payload's errata
    // year and predecessors; when none fits, the assessment is a target.
    private static DoiTarget? FromPayload(Candidate item, object value, IReadOnlyList<long> predecessors,
        Dictionary<long, (long? AssessmentId, string Doi)> gbif, Dictionary<long, List<string>> wikidata, DoiScopeCounts counts) {
        IucnCitationParse parse;
        try {
            using var document = value switch {
                byte[] bytes => JsonDocument.Parse(bytes),
                string text => JsonDocument.Parse(text),
                _ => throw new JsonException("Unexpected column type."),
            };
            parse = IucnCitationPartsParser.Parse(document.RootElement, downloadedAtUtc: null, predecessors);
        } catch (JsonException) {
            counts.CitationUnreadable++;
            return TargetOf(item, null, predecessors) with { HasPayload = true };
        }
        if (parse.Parts is not { } parts) {
            counts.CitationUnreadable++;
            return TargetOf(item, null, predecessors) with { HasPayload = true };
        }
        // Counted in the same order as before the payload was read: GBIF, Wikidata, then the citation.
        string? gbifDoi = gbif.TryGetValue(item.TaxonId, out var g) && g.AssessmentId == item.AssessmentId ? g.Doi : null;
        wikidata.TryGetValue(item.AssessmentId, out var wikidataDois);
        var choice = IucnDoiSelector.Select(parts, citationDoi: null, gbifDoi, wikidataDois, predecessors);
        if (choice.Doi is not null) {
            if (choice.Source == BeastieBot3.Shared.Wikitext.DoiSource.Gbif) counts.FromGbif++; else counts.FromWikidata++;
            return null;
        }
        if (parts.Doi is not null) {
            counts.FromCitation++;
            return null;
        }
        return TargetOf(item, parts, predecessors) with { HasPayload = true };
    }

    private static DoiTarget TargetOf(Candidate item, BeastieBot3.Shared.Wikitext.IucnCitationParts? parts, IReadOnlyList<long> predecessors) => new() {
        AssessmentId = item.AssessmentId,
        TaxonId = item.TaxonId,
        YearPublished = parts?.Year ?? item.Year,
        ErrataYear = parts?.ErrataYear,
        AmendsYear = parts?.AmendsYear,
        PredecessorIds = predecessors,
        Language = item.Language,
        NewInRelease = item.NewInRelease,
        Scope = item.Scope,
        Kind = item.Kind,
    };

    private static bool NamesOwnIds(string? doi, Candidate item) =>
        IucnDoiSelector.TryParse(doi) is { } parsed && parsed.TaxonId == item.TaxonId && parsed.AssessmentId == item.AssessmentId;

    private static void ReadWikidataDois(string path, Dictionary<long, List<string>> into) {
        using var connection = SiteIucnCsvReader.OpenReadOnly(path);
        using (var exists = connection.CreateCommand()) {
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'wikidata_iucn_assessment_items'";
            if ((long)exists.ExecuteScalar()! == 0) {
                return;
            }
        }
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT assessment_id, doi, all_dois
            FROM wikidata_iucn_assessment_items
            WHERE assessment_id IS NOT NULL
            ORDER BY qid_numeric
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var found = new List<string>();
            if (!reader.IsDBNull(1) && SiteBuildRules.NullIfBlank(reader.GetString(1)) is { } mainDoi) {
                found.Add(mainDoi);
            }
            if (!reader.IsDBNull(2)) {
                found.AddRange(reader.GetString(2).Split(new[] { ' ', ',', ';', '|', '\n', '\t' },
                    StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
            }
            if (found.Count == 0) {
                continue;
            }
            var assessmentId = reader.GetInt64(0);
            if (!into.TryGetValue(assessmentId, out var list)) {
                into[assessmentId] = list = new List<string>();
            }
            list.AddRange(found);
        }
    }

    private static bool IsGlobal(IReadOnlyList<string> regions) =>
        regions.Any(r => string.Equals(r, SiteBuildRules.GlobalScope, StringComparison.OrdinalIgnoreCase));

    private static int? Year(string? text) =>
        int.TryParse(text?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var year) ? year : null;

    private static string? Text(SqliteDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
}
