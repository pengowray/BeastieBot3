using System.Text.Json;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn.Citations;
using BeastieBot3.Shared.Wikitext;
using Microsoft.Data.Sqlite;

// The assessment rows of the site database: which ones there are (Plan), and then one pass over the
// cached /api/v4/assessment payloads that adds what only the payload has (the citation parts, and
// the trend and criteria version of earlier assessments) and writes each row (WriteAll).
//
// Rules (see also SiteDbSchema):
//   - The CSV's rows are the latest assessments of release 2026-1, one per taxon and scope set.
//     Where the CSV and an API header describe the same assessment, the CSV's values are used.
//   - Every other header in the taxon's own API record is an earlier assessment, or a newer one
//     than the release. A header flagged latest is only latest here when the CSV has no row for the
//     taxon and that scope, so a region never has two latest rows.
//   - A taxon that is not in the release (only in the API cache) has no latest assessment: every
//     header of its record is stored as an earlier one, including any flagged latest.
//   - Headers for another taxon id, with no scope, with no year published (unpublished drafts) or
//     with no category are left out and counted.
//   - The citation parts are parsed from every cached payload. The DOI is IUCN's own when its
//     citation has one that fits; otherwise GBIF's, only for the assessment GBIF's checklist names
//     as the taxon's current global one; otherwise Wikidata's; otherwise the one `iucn resolve-dois`
//     found in Crossref's list of IUCN DOIs or at doi.org (IucnDoiSelector checks each).
//     Assessments with no cached payload (the CSV's subpopulations) keep citation_json NULL.
//   - An author name with a letter lost to an encoding error ("Kry?tufek, B.") is repaired from the
//     other assessor credits (AssessorNamePool). The pool is complete only after every payload has
//     been read, so the few rows with such a name are parsed again and written at the end.
//   - replaced_by_assessment_id is set on the assessment an errata or amended version replaced,
//     pointing at the newer one. An errata version (its title says "errata version published in")
//     replaced one of the assessments IucnTaxaHeaders.PredecessorIds gives that are also rows here;
//     when there is none, the assessment named by the DOI from `iucn resolve-dois` that the errata
//     version uses (IucnDoiSelector.ErrataPredecessorNamedBy), when that is a row here.
//     An amended version replaced an earlier assessment of the taxon, same scope, published in the
//     year its title names. Either way the replaced assessment has a lower id: IUCN numbers
//     assessments in the order they are made, and without this the two errata versions of the 2022
//     Pelophylax cerigensis assessment (published in 2022 and 2024, both since replaced) would each
//     be taken for the other's predecessor. With several candidates, one that another candidate
//     replaced is dropped (SettleChains). With none or still several, or when two newer versions name
//     the same assessment, nothing is set.

namespace BeastieBot3.SiteBuild;

/// The DOIs other sources offer, by taxon (GBIF: the assessment it names and its DOI) and by
/// assessment (Wikidata, and the DOIs `iucn resolve-dois` found in Crossref's list or at doi.org).
internal sealed class SiteDoiSources {
    public Dictionary<long, (long? AssessmentId, string? Doi)> Gbif { get; } = new();
    public Dictionary<long, List<string>> Wikidata { get; } = new();
    public Dictionary<long, string> Resolved { get; } = new();
}

internal sealed class SiteAssessmentPass {
    private readonly IReadOnlyDictionary<long, SiteTaxon> _taxa;
    private readonly IReadOnlyDictionary<long, ApiTaxonRecord> _records;
    private readonly SiteBuildStats _stats;
    private readonly Dictionary<long, SiteAssessment> _plan = new();

    // The planned rows of each taxon, kept after _plan is emptied, to find what a version replaced.
    private readonly Dictionary<long, List<(long AssessmentId, string Scope, int? YearPublished, bool IsLatest)>> _rowsByTaxon = new();
    // The assessment each replaced assessment was replaced by (null when two newer versions name it),
    // and whether that newer one is an errata version or an amended one.
    private readonly Dictionary<long, (long? By, bool ByErrata)> _replacedBy = new();
    // Versions with several candidates for the assessment they replaced, settled by SettleChains.
    private readonly List<(long Newer, List<long> Candidates, bool ByErrata)> _unsettled = new();

    private readonly AssessorNamePool _names = new();
    // Rows with a damaged author name, parsed again once _names is complete.
    private readonly List<(SiteAssessment Assessment, byte[] Json, DateTime? Downloaded, SiteDoiSources Dois)> _waitingForNames = new();

    public SiteAssessmentPass(IReadOnlyDictionary<long, SiteTaxon> taxa, IReadOnlyDictionary<long, ApiTaxonRecord> records,
        SiteBuildStats stats) {
        _taxa = taxa;
        _records = records;
        _stats = stats;
    }

    public int PlannedCount => _plan.Count;

    /// Decides the rows and each taxon's latest global assessment.
    public void Plan(IEnumerable<SiteAssessment> csvRows) {
        var csvScopes = new Dictionary<long, HashSet<string>>();
        foreach (var row in csvRows) {
            if (!_plan.TryAdd(row.AssessmentId, row)) {
                continue;
            }
            if (!csvScopes.TryGetValue(row.TaxonId, out var scopes)) {
                csvScopes[row.TaxonId] = scopes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            }
            scopes.Add(row.Scope);
        }

        foreach (var (taxonId, record) in _records) {
            csvScopes.TryGetValue(taxonId, out var scopesInCsv);
            var inRelease = _taxa[taxonId].InRelease;
            foreach (var header in record.Assessments) {
                if (header.TaxonId is { } headerTaxon && headerTaxon != taxonId) {
                    _stats.ApiHeadersOtherTaxon++;
                    continue;
                }
                if (_plan.TryGetValue(header.AssessmentId, out var existing)) {
                    if (existing.FromCsv && header.Category is not null
                        && !string.Equals(existing.Category, header.Category, StringComparison.Ordinal)) {
                        _stats.CsvCategoryDiffersFromApi++;
                    }
                    continue;
                }
                if (header.Scope is null) {
                    _stats.ApiHeadersNoScope++;
                    continue;
                }
                if (header.YearPublished is null) {
                    _stats.ApiHeadersUnpublished++;
                    continue;
                }
                if (header.Category is null) {
                    _stats.ApiHeadersNoCategory++;
                    continue;
                }
                var latest = header.Latest;
                if (latest && !inRelease) {
                    latest = false;
                    _stats.NotInReleaseLatestHeaders++;
                } else if (latest) {
                    if (scopesInCsv?.Contains(header.Scope) == true) {
                        latest = false;
                        _stats.ApiLatestCoveredByCsv++;
                    } else {
                        _stats.ApiLatestNotInCsv++;
                    }
                }
                _plan[header.AssessmentId] = new SiteAssessment {
                    AssessmentId = header.AssessmentId,
                    TaxonId = taxonId,
                    Scope = header.Scope,
                    IsLatest = latest,
                    Category = header.Category,
                    PossiblyExtinct = header.PossiblyExtinct,
                    PossiblyExtinctInTheWild = header.PossiblyExtinctInTheWild,
                    Criteria = header.Criteria,
                    YearPublished = header.YearPublished,
                    AssessmentDate = header.AssessmentDate,
                    FromCsv = false,
                };
            }
        }

        foreach (var row in _plan.Values) {
            if (!_rowsByTaxon.TryGetValue(row.TaxonId, out var rows)) {
                _rowsByTaxon[row.TaxonId] = rows = new();
            }
            rows.Add((row.AssessmentId, row.Scope, row.YearPublished, row.IsLatest));
        }

        // The latest global assessment: the CSV's global row, else a global API row still flagged latest.
        foreach (var assessment in _plan.Values
                     .Where(a => a.IsLatest && a.Scope == SiteBuildRules.GlobalScope)
                     .OrderBy(a => a.FromCsv ? 1 : 0)
                     .ThenBy(a => a.YearPublished ?? 0)
                     .ThenBy(a => a.AssessmentId)) {
            // Later rows win: CSV rows come last, then the newest.
            _taxa[assessment.TaxonId].LatestGlobalAssessmentId = assessment.AssessmentId;
        }
    }

    /// Reads the payloads in row order, adds what they give and writes every planned row.
    public void WriteAll(SqliteConnection cache, SiteDbWriter writer, SiteDoiSources dois, CancellationToken cancellationToken) {
        // Map the planned ids to row ids over the unique index, then read the rows in row order, so
        // the JSON is read front to back rather than at random.
        var rows = new List<(long RowId, long AssessmentId)>();
        using (var index = cache.CreateCommand()) {
            index.CommandText = "SELECT id, assessment_id FROM assessments";
            index.CommandTimeout = 0;
            using var reader = index.ExecuteReader();
            while (reader.Read()) {
                var assessmentId = reader.GetInt64(1);
                if (_plan.ContainsKey(assessmentId)) {
                    rows.Add((reader.GetInt64(0), assessmentId));
                }
            }
        }
        rows.Sort((a, b) => a.RowId.CompareTo(b.RowId));

        using var command = cache.CreateCommand();
        command.CommandText = "SELECT downloaded_at, json FROM assessments WHERE id = @id";
        var idParameter = command.Parameters.Add("@id", SqliteType.Integer);
        ProgressConsole.Run("Reading assessment payloads", rows.Count, progress => {
            foreach (var (rowId, assessmentId) in rows) {
                cancellationToken.ThrowIfCancellationRequested();
                progress.Increment();
                idParameter.Value = rowId;
                string downloadedAt;
                byte[] json;
                using (var reader = command.ExecuteReader()) {
                    if (!reader.Read()) {
                        continue;
                    }
                    downloadedAt = reader.GetString(0);
                    json = reader.GetFieldValue<byte[]>(1);
                }
                var assessment = _plan[assessmentId];
                _plan.Remove(assessmentId);
                if (AddFromPayload(assessment, json, downloadedAt, dois)) {
                    Write(writer, assessment);
                }
            }
        });

        // Rows with a damaged author name, now that every other name is known.
        foreach (var (assessment, json, downloaded, rowDois) in _waitingForNames) {
            using (var document = JsonDocument.Parse(json)) {
                AddCitation(assessment, document.RootElement, downloaded, rowDois, _names.Repair);
            }
            Write(writer, assessment);
        }
        _waitingForNames.Clear();

        // Planned rows with no cached payload: the CSV's subpopulations.
        foreach (var assessment in _plan.Values.OrderBy(a => a.AssessmentId)) {
            _stats.CitationsNotCached++;
            Write(writer, assessment);
        }
        _plan.Clear();

        SettleChains();
        foreach (var (replaced, (by, byErrata)) in _replacedBy) {
            if (by is not { } newer) {
                continue;
            }
            writer.SetReplacedBy(replaced, newer);
            if (byErrata) _stats.ReplacedByErrata++; else _stats.ReplacedByAmended++;
        }
    }

    // Adds what the payload gives. False when the row has a damaged author name and waits for the
    // name pool; it is then written at the end of WriteAll.
    private bool AddFromPayload(SiteAssessment assessment, byte[] json, string downloadedAt, SiteDoiSources dois) {
        JsonDocument document;
        try {
            document = JsonDocument.Parse(json);
        } catch (JsonException) {
            _stats.PayloadsUnreadable++;
            return true;
        }
        using (document) {
            var root = document.RootElement;
            _stats.PayloadsRead++;
            DateTime? downloaded = StoredUtc.Parse(downloadedAt);
            if (downloaded is { } at) {
                if (_stats.DownloadedFrom is null || at < _stats.DownloadedFrom) _stats.DownloadedFrom = at;
                if (_stats.DownloadedTo is null || at > _stats.DownloadedTo) _stats.DownloadedTo = at;
            }

            if (!assessment.FromCsv && root.ValueKind == JsonValueKind.Object) {
                assessment.PopulationTrend = PopulationTrend(root);
                assessment.CriteriaVersion = CriteriaVersion(root);
            }
            if (!AddCitation(assessment, root, downloaded, dois, repairAuthorName: null)) {
                _waitingForNames.Add((assessment, json, downloaded, dois));
                return false;
            }
            return true;
        }
    }

    // Parses the citation into the row. Without a repair function, a parse with a damaged author
    // name is not used and false is returned.
    private bool AddCitation(SiteAssessment assessment, JsonElement root, DateTime? downloaded, SiteDoiSources dois,
        Func<string, string?>? repairAuthorName) {
        var predecessors = _records.TryGetValue(assessment.TaxonId, out var record)
            ? IucnTaxaHeaders.PredecessorIds(record.Headers, assessment.AssessmentId)
            : Array.Empty<long>();
        var parse = IucnCitationPartsParser.Parse(root, downloaded, predecessors, repairAuthorName);
        if (repairAuthorName is null && parse.DamagedAuthorNames.Count > 0) {
            return false;
        }
        if (parse.Parts is not { } parts) {
            _stats.Count(_stats.CitationFailures, parse.Failure);
            return true;
        }
        _names.AddFrom(parse);
        foreach (var repair in parse.RepairedAuthorNames) {
            _stats.Count(_stats.AuthorNameRepairs, (repair.From, repair.To));
        }
        foreach (var name in parse.DamagedAuthorNames) {
            _stats.Count(_stats.AuthorNamesNotRepaired, name);
        }
        // The assessment named by the resolver's DOI, when this is an errata version and PredecessorIds
        // misses it (IucnDoiSelector.ErrataPredecessorNamedBy). Kept only when that DOI is used.
        long? doiNamedPredecessor = null;
        if (parts.Doi is null) {
            string? gbifDoi = null;
            if (dois.Gbif.TryGetValue(assessment.TaxonId, out var gbif) && gbif.AssessmentId == assessment.AssessmentId) {
                gbifDoi = gbif.Doi;
            }
            dois.Wikidata.TryGetValue(assessment.AssessmentId, out var wikidataDois);
            dois.Resolved.TryGetValue(assessment.AssessmentId, out var resolvedDoi);
            IReadOnlyList<long> resolvedPredecessors = predecessors;
            if (IucnDoiSelector.ErrataPredecessorNamedBy(parts, resolvedDoi) is { } named && !predecessors.Contains(named)) {
                resolvedPredecessors = predecessors.Append(named).ToList();
                doiNamedPredecessor = named;
            }
            var choice = IucnDoiSelector.Select(parts, citationDoi: null, gbifDoi, wikidataDois, predecessors, resolvedDoi, resolvedPredecessors);
            parts = parts with { Doi = choice.Doi, DoiSource = choice.Source };
            if (choice.Source != DoiSource.Resolved) {
                doiNamedPredecessor = null;
            }
        }
        _stats.Count(_stats.DoisBySource, parts.Doi is null ? DoiSource.None : parts.DoiSource);
        _stats.CitationsParsed++;
        assessment.CitationJson = parts.ToJson();
        LinkReplaced(assessment, parse, predecessors, doiNamedPredecessor);
        return true;
    }

    // Records which earlier assessment an errata or amended version replaced (see the file comment).
    private void LinkReplaced(SiteAssessment assessment, IucnCitationParse parse, IReadOnlyList<long> predecessors,
        long? doiNamedPredecessor) {
        _rowsByTaxon.TryGetValue(assessment.TaxonId, out var rows);
        rows ??= new();
        var newer = assessment.AssessmentId;
        List<long> candidates;
        if (parse.HasErrataAnnotation) {
            candidates = predecessors.Where(id => id < newer && rows.Any(r => r.AssessmentId == id)).Distinct().ToList();
            // The assessment the DOI names is used only when the same-year rule finds none. Added to a
            // list that already has one, it would make two candidates and neither would be linked:
            // taxon 9530's errata version 85829841 replaced 12998880 by the same-year rule, while its
            // DOI names the 2000 assessment 12998967.
            if (candidates.Count == 0 && doiNamedPredecessor is { } named && named < newer && rows.Any(r => r.AssessmentId == named)) {
                candidates.Add(named);
                _stats.ReplacedFoundFromDoi++;
            }
        } else if (parse.Parts?.AmendsYear is { } amendsYear) {
            candidates = rows
                .Where(r => r.AssessmentId < newer && !r.IsLatest && r.YearPublished == amendsYear
                    && string.Equals(r.Scope, assessment.Scope, StringComparison.Ordinal))
                .Select(r => r.AssessmentId)
                .ToList();
        } else {
            return;
        }
        if (candidates.Count == 0) {
            _stats.ReplacedNoCandidate++;
        } else if (candidates.Count > 1) {
            _unsettled.Add((newer, candidates, parse.HasErrataAnnotation));
        } else {
            Claim(candidates[0], newer, parse.HasErrataAnnotation);
        }
    }

    private void Claim(long replaced, long newer, bool byErrata) {
        if (_replacedBy.TryGetValue(replaced, out var existing)) {
            if (existing.By is { } other && other != newer) {
                _replacedBy[replaced] = (null, false);
                _stats.ReplacedClaimedTwice++;
            }
            return;
        }
        _replacedBy[replaced] = (newer, byErrata);
    }

    // A version with several candidates is usually the end of a chain. The giant panda's 2016
    // assessment 45033386 was replaced by the errata version 102080907, which the errata version
    // 121745669 replaced in turn; both earlier ones are candidates for the last. A candidate that
    // another candidate replaced is not the one this version replaced, so it is dropped, and a version
    // left with one candidate is linked to it. Each link can settle another, so this repeats.
    private void SettleChains() {
        bool settledAny;
        do {
            settledAny = false;
            for (var i = _unsettled.Count - 1; i >= 0; i--) {
                var (newer, candidates, byErrata) = _unsettled[i];
                var left = candidates
                    .Where(c => !(_replacedBy.TryGetValue(c, out var link) && link.By is { } by && candidates.Contains(by)))
                    .ToList();
                if (left.Count != 1) {
                    continue;
                }
                Claim(left[0], newer, byErrata);
                _unsettled.RemoveAt(i);
                settledAny = true;
            }
        } while (settledAny);
        _stats.ReplacedSeveralCandidates += _unsettled.Count;
        _unsettled.Clear();
    }

    private void Write(SiteDbWriter writer, SiteAssessment assessment) {
        writer.AddAssessment(assessment);
        if (!assessment.IsLatest) {
            _stats.AssessmentsHistory++;
        } else if (assessment.Scope == SiteBuildRules.GlobalScope) {
            _stats.AssessmentsGlobalLatest++;
        } else {
            _stats.AssessmentsRegionalLatest++;
        }
        assessment.CitationJson = null;
    }

    // population_trend: {"description": {"en": "Unknown"}, "code": "3"}
    private static string? PopulationTrend(JsonElement root) {
        if (!root.TryGetProperty("population_trend", out var trend) || trend.ValueKind != JsonValueKind.Object
            || !trend.TryGetProperty("description", out var description) || description.ValueKind != JsonValueKind.Object) {
            return null;
        }
        return SiteBuildRules.NullIfBlank(SiteApiTaxaReader.ReadString(description, "en"));
    }

    // red_list_category: {"version": "3.1", "description": {...}, "code": "VU"}
    private static string? CriteriaVersion(JsonElement root) {
        if (!root.TryGetProperty("red_list_category", out var category) || category.ValueKind != JsonValueKind.Object) {
            return null;
        }
        return SiteBuildRules.CriteriaVersion(SiteApiTaxaReader.ReadString(category, "version"));
    }
}
