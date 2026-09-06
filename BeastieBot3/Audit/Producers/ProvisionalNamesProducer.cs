using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using BeastieBot3.Audit.Model;
using BeastieBot3.Audit.Producers.ColCrosscheck;
using BeastieBot3.Col;
using BeastieBot3.Infrastructure;

// Provisional (undescribed) names that another catalogue now records as a described species.
//
// An assessment may be published for a species nobody has formally described yet, under a working
// name like "Notogomphus sp. nov. 'gorilla'". Some of those species are described later. This
// producer builds the binomial the quoted tag implies, looks for it in the Catalogue of Life, and
// separately asks Wikidata and the English Wikipedia what name they hold against the same SIS id.
//
// Deliberately conservative, because a bare name match is weak evidence: a taxon whose synonym list
// already names a described species is left alone (IUCN knows), a "cf." or "aff." tag is a
// comparison rather than an identification and is never turned into a candidate, and a tag that is
// a locality or a collector code yields nothing to look up. What survives is a lead for a person to
// check, not a determination. The interesting number moves every release, in both catalogues, which
// is why this is a standing report rather than a one-off list.

namespace BeastieBot3.Audit.Producers;

internal sealed class ProvisionalNamesProducer : IAuditReportProducer {
    private const string Id_ = "provisional-names";
    public string Id => Id_;

    // How a provisional name was disposed of, in the order the summary table lists them.
    private enum Disposition { Listed, KnownToIucn, Qualified, NoDescribedName, NothingToLookUp }

    public AuditReport? Produce(AuditContext ctx) {
        var iucn = ctx.IucnCsvOrNull();
        if (iucn is null || !AuditContext.ObjectExists(iucn, "view_assessments_html_taxonomy_html")) {
            return null;
        }

        var col = ctx.ColOrNull();
        var repo = col is not null && AuditContext.ObjectExists(col, "nameusage") ? new ColTaxonRepository(col) : null;
        var apiCache = ctx.IucnApiCacheOrNull();
        var hasSynonyms = apiCache is not null && AuditContext.ObjectExists(apiCache, "taxa");
        var others = OtherSourceIndex.Build(ctx.WikidataCacheOrNull(), ctx.WikipediaCacheOrNull());

        var tally = new Dictionary<Disposition, int>();
        var findings = new List<AuditFinding>();
        var provisionalCount = 0;

        foreach (var row in ReadProvisional(iucn, ctx)) {
            ctx.Ct.ThrowIfCancellationRequested();
            var name = AuditMapping.Decode(row.ScientificName) ?? "";
            var parsed = ProvisionalNames.Read(name, row.Genus, row.Species, row.Infraspecific);
            if (!parsed.IsProvisional) {
                continue;
            }
            provisionalCount++;

            // IUCN's own synonym list, which the API publishes and the CSV export does not. Read
            // per taxon rather than through IucnSynonymIndex, which scans the whole API cache to
            // build a release-wide map; only these few taxa are ever asked about. A synonym that is
            // itself provisional is not a described name, so it does not count.
            var known = hasSynonyms && DescribedSynonyms(apiCache!, row.TaxonId).Count > 0;
            if (known) {
                Count(tally, Disposition.KnownToIucn);
                continue;
            }
            if (parsed.Outcome == ProvisionalOutcome.Qualified) {
                Count(tally, Disposition.Qualified);
                continue;
            }

            var col1 = parsed.CandidateName is null || repo is null
                ? null
                : BestColMatch(repo, parsed.CandidateName, ctx);
            var other = others?.Lookup(row.TaxonId, name, ctx.Ct) ?? OtherSourceHit.None;
            var otherNames = other.OtherNames.Where(n => !ProvisionalNames.IsProvisional(n)).ToList();

            if (col1 is null && otherNames.Count == 0) {
                Count(tally, parsed.Outcome == ProvisionalOutcome.Candidate
                    ? Disposition.NoDescribedName
                    : Disposition.NothingToLookUp);
                continue;
            }

            Count(tally, Disposition.Listed);
            findings.Add(Build(row, name, parsed, col1, other, otherNames));
        }

        var ordered = findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(TaxonGroups.SortKey, StringComparer.Ordinal)
            .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return BuildReport(ctx, ordered, provisionalCount, tally, repo is not null, hasSynonyms, others);
    }

    // The described (non-provisional) names IUCN already files as synonyms of this taxon. One
    // indexed read per listed taxon, against the API cache's stored payload.
    private static IReadOnlyList<string> DescribedSynonyms(SqliteConnection apiCache, long taxonId) {
        using var command = apiCache.CreateCommand();
        command.CommandText = "SELECT json FROM taxa WHERE root_sis_id = @id LIMIT 1";
        command.Parameters.AddWithValue("@id", taxonId);
        command.CommandTimeout = 30;
        if (command.ExecuteScalar() is not string payload || string.IsNullOrWhiteSpace(payload)) {
            return Array.Empty<string>();
        }
        var names = new List<string>();
        try {
            using var document = JsonDocument.Parse(payload);
            if (!document.RootElement.TryGetProperty("taxon", out var taxon) ||
                !taxon.TryGetProperty("synonyms", out var synonyms) || synonyms.ValueKind != JsonValueKind.Array) {
                return names;
            }
            foreach (var synonym in synonyms.EnumerateArray()) {
                var genus = JsonString(synonym, "genus_name");
                var species = JsonString(synonym, "species_name");
                if (genus is null || species is null) {
                    continue;
                }
                var infra = JsonString(synonym, "infra_name");
                var name = infra is null ? $"{genus} {species}" : $"{genus} {species} {infra}";
                if (!ProvisionalNames.IsProvisional(name)) {
                    names.Add(name);
                }
            }
        } catch (JsonException) {
            // A payload this producer cannot read answers nothing; treat it as no synonyms rather
            // than dropping the taxon from the report.
        }
        return names;
    }

    private static string? JsonString(JsonElement element, string name) =>
        element.ValueKind == JsonValueKind.Object &&
        element.TryGetProperty(name, out var prop) && prop.ValueKind == JsonValueKind.String &&
        !string.IsNullOrWhiteSpace(prop.GetString())
            ? prop.GetString()!.Trim()
            : null;

    // -- CoL lookup --------------------------------------------------------------------------

    private sealed record ColMatch(ColTaxonRecord Record, ColTaxonRecord? AcceptedTarget, bool IsAccepted);

    // The best CoL usage of the candidate name: an accepted one if there is one, otherwise a
    // synonym, whose accepted taxon is resolved through parentID (this schema has no
    // acceptedNameUsageID column).
    private static ColMatch? BestColMatch(ColTaxonRepository repo, string candidate, AuditContext ctx) {
        var usages = repo.FindByScientificName(candidate, ctx.Ct);
        if (usages.Count == 0) {
            return null;
        }
        var accepted = usages.FirstOrDefault(u => Looks(u.Status, "accepted"));
        if (accepted is not null) {
            return new ColMatch(accepted, null, true);
        }
        var synonym = usages.FirstOrDefault(u => Looks(u.Status, "synonym")) ?? usages[0];
        var target = string.IsNullOrWhiteSpace(synonym.ParentId) ? null : repo.GetById(synonym.ParentId!, ctx.Ct);
        return new ColMatch(synonym, target, false);
    }

    private static bool Looks(string? status, string token) =>
        !string.IsNullOrWhiteSpace(status) && status.Contains(token, StringComparison.OrdinalIgnoreCase);

    // -- finding -----------------------------------------------------------------------------

    private static AuditFinding Build(ProvisionalRow row, string name, ProvisionalName parsed,
        ColMatch? col, OtherSourceHit other, IReadOnlyList<string> otherNames) {
        var (rank, isFull) = AuditMapping.Rank(row.InfraType, row.Subpopulation);
        var colName = AuditMapping.Decode(col?.Record.ScientificName);
        var colYear = ColYear(col?.Record);
        var assessmentYear = Year(row.YearPublished);
        var describedSince = colYear is not null && assessmentYear is not null && colYear > assessmentYear;

        // Strongest first: a name CoL accepts and dates after the assessment, then one CoL simply
        // accepts, then one CoL files under something else, then a name only the wikis carry.
        var severity = col is null ? 2 : col.IsAccepted ? (describedSince ? 5 : 4) : 3;
        var suggested = colName ?? otherNames[0];

        var finding = new AuditFinding {
            ReportId = Id_,
            Key = $"{row.TaxonId}:provisional",
            TaxonId = row.TaxonId,
            AssessmentId = row.AssessmentId,
            RedlistUrl = IucnUrls.Species(row.TaxonId, row.AssessmentId),
            ScientificName = name,
            Rank = rank,
            IsFullSpecies = isFull,
            InfraType = row.InfraType,
            SubpopulationName = row.Subpopulation,
            Kingdom = row.Kingdom,
            Phylum = row.Phylum,
            Class = row.Class,
            Order = row.Order,
            Family = row.Family,
            Genus = row.Genus,
            Species = row.Species,
            StatusCode = AuditMapping.CodeFromCategory(row.Category, row.PossiblyExtinct, row.PossiblyExtinctInTheWild),
            StatusCategory = row.Category,
            YearPublished = row.YearPublished,
            DataSource = "iucn-csv+col+wiki",
            Field = "scientificName",
            CurrentValue = name,
            SuggestedValue = suggested,
            IssueType = col is null ? "described-name-in-wikis"
                : col.IsAccepted ? (describedSince ? "described-since-assessment" : "described-name-in-col")
                : "candidate-name-is-a-col-synonym",
            SeverityTier = severity,
            Detail = Detail(col, colName, colYear, assessmentYear, describedSince, otherNames),
        };

        Set(finding, "candidateName", parsed.CandidateName);
        Set(finding, "colAuthority", AuditMapping.Decode(col?.Record.Authorship));
        Set(finding, "colYear", colYear?.ToString(CultureInfo.InvariantCulture));
        Set(finding, "colStatus", col is null ? null : col.IsAccepted ? "accepted" : "synonym");
        Set(finding, "colUrl", ColUrls.Taxon(col?.Record.Id));
        if (other.WikidataId is not null) {
            Set(finding, "wikidataId", other.WikidataId);
            Set(finding, "wikidataUrl", OtherSourceIndex.WikidataUrl(other.WikidataId));
        }
        if (other.WikipediaTitle is not null && !ProvisionalNames.IsProvisional(other.WikipediaTitle)) {
            Set(finding, "wikipediaTitle", other.WikipediaTitle);
            Set(finding, "wikipediaUrl", OtherSourceIndex.WikipediaUrl(other.WikipediaTitle));
        }

        if (col is not null && otherNames.Count > 0 &&
            otherNames.Any(n => string.Equals(n, colName, StringComparison.OrdinalIgnoreCase))) {
            finding.Notes.Add("Wikidata or Wikipedia records the same name against this taxon's SIS id.");
        }
        // Only when both sides actually name a family. IUCN writes "NOT ASSIGNED" where it records
        // none, which is not a family and must not be printed as one.
        if (col is not null && Named(row.Family) is { } iucnFamily && Named(col.Record.Family) is { } colFamily &&
            !string.Equals(iucnFamily, colFamily, StringComparison.OrdinalIgnoreCase)) {
            finding.Notes.Add($"CoL places that name in {colFamily} while IUCN places this taxon in {iucnFamily}, so the two may not be the same taxon.");
        }
        return finding;
    }

    // A family name, or null where the source records none. IUCN uses the literal "NOT ASSIGNED".
    private static string? Named(string? value) {
        var trimmed = value?.Trim();
        return string.IsNullOrEmpty(trimmed) || trimmed.Equals("NOT ASSIGNED", StringComparison.OrdinalIgnoreCase)
            ? null : trimmed;
    }

    private static string Detail(ColMatch? col, string? colName, int? colYear, int? assessmentYear,
        bool describedSince, IReadOnlyList<string> otherNames) {
        if (col is null) {
            return $"No Catalogue of Life record matches the name built from the tag, but {string.Join(" and ", otherNames)} is recorded against this taxon's SIS id elsewhere.";
        }
        if (!col.IsAccepted) {
            var target = AuditMapping.Decode(col.AcceptedTarget?.ScientificName);
            return target is null
                ? $"The Catalogue of Life holds {colName} as a synonym, so the tag may point at a name that is no longer in use."
                : $"The Catalogue of Life holds {colName} as a synonym of {target}, so the tag may point at that taxon rather than an undescribed one.";
        }
        if (describedSince) {
            return $"The Catalogue of Life accepts {colName}, published in {colYear}, after this assessment of {assessmentYear}.";
        }
        return $"The Catalogue of Life accepts {colName} as a described species.";
    }

    private static int? ColYear(ColTaxonRecord? record) {
        if (record is null) {
            return null;
        }
        return Year(record.NamePublishedInYear) ?? Year(YearPattern.Match(record.Authorship ?? "").Value);
    }

    private static int? Year(string? text) =>
        !string.IsNullOrWhiteSpace(text) && int.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var y)
            ? y : null;

    private static readonly Regex YearPattern = new(@"1[5-9]\d\d|20\d\d", RegexOptions.Compiled);

    private static void Set(AuditFinding f, string key, string? value) {
        if (!string.IsNullOrWhiteSpace(value)) {
            f.Extra[key] = value!;
        }
    }

    private static void Count(Dictionary<Disposition, int> tally, Disposition d) =>
        tally[d] = tally.TryGetValue(d, out var n) ? n + 1 : 1;

    // -- reading the release ------------------------------------------------------------------

    private sealed record ProvisionalRow(long TaxonId, long? AssessmentId, string? ScientificName, string? InfraType,
        string? Subpopulation, string? Category, string? PossiblyExtinct, string? PossiblyExtinctInTheWild,
        string? YearPublished, string? Kingdom, string? Phylum, string? Class, string? Order, string? Family,
        string? Genus, string? Species) {
        public bool Infraspecific => !string.IsNullOrWhiteSpace(InfraType);
    }

    // The LIKE is a coarse net over an unindexed column; ProvisionalNames.Read is the real gate, so
    // "Conophytum flavum subsp. novicium" is read past rather than counted as provisional.
    private static IEnumerable<ProvisionalRow> ReadProvisional(SqliteConnection connection, AuditContext ctx) {
        const string sql = @"
SELECT i.taxonId, i.assessmentId, i.scientificName, i.infraType, i.subpopulationName,
       i.redlistCategory, i.possiblyExtinct, i.possiblyExtinctInTheWild, i.yearPublished,
       i.kingdomName, i.phylumName, i.className, i.orderName, i.familyName, i.genusName, i.speciesName
FROM view_assessments_html_taxonomy_html i
WHERE i.scientificName LIKE '%nov.%'
ORDER BY i.scientificName, i.taxonId";

        using var command = connection.CreateCommand();
        command.CommandText = ctx.Limit is > 0 ? sql + "\nLIMIT " + ctx.Limit.Value : sql;
        command.CommandTimeout = 0;

        var seen = new HashSet<long>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var taxonId = reader.GetInt64(0);
            if (!seen.Add(taxonId)) {
                continue;
            }
            yield return new ProvisionalRow(
                taxonId,
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                Str(reader, 2), Str(reader, 3), Str(reader, 4), Str(reader, 5), Str(reader, 6), Str(reader, 7),
                Str(reader, 8), Str(reader, 9), Str(reader, 10), Str(reader, 11), Str(reader, 12), Str(reader, 13),
                Str(reader, 14), Str(reader, 15));
        }
    }

    private static string? Str(SqliteDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) || string.IsNullOrWhiteSpace(reader.GetString(ordinal)) ? null : reader.GetString(ordinal).Trim();

    // -- report ------------------------------------------------------------------------------

    private static AuditReport BuildReport(AuditContext ctx, IReadOnlyList<AuditFinding> findings,
        int provisionalCount, Dictionary<Disposition, int> tally, bool hasCol, bool hasSynonyms, OtherSourceIndex? others) {
        var sources = new List<string> { $"IUCN Red List {ctx.Release} (CSV export)" };
        if (hasCol) {
            sources.Add($"Catalogue of Life {ctx.ColReleaseLabel() ?? "(release unknown)"}");
        }
        if (hasSynonyms) {
            sources.Add("IUCN Red List API synonyms");
        }
        if (others?.HasWikidata == true) {
            sources.Add("Wikidata");
        }
        if (others?.HasWikipedia == true) {
            sources.Add("English Wikipedia");
        }

        var rows = new List<IReadOnlyList<string>>();
        void Row(Disposition d, string label) {
            var n = d == Disposition.Listed ? findings.Count : tally.TryGetValue(d, out var v) ? v : 0;
            rows.Add(new[] { label, n.ToString("N0", CultureInfo.InvariantCulture) });
        }
        Row(Disposition.Listed, "Listed below");
        Row(Disposition.KnownToIucn, "IUCN already records a described synonym");
        Row(Disposition.Qualified, "Tag qualified with cf. or aff.");
        Row(Disposition.NoDescribedName, "No described name found");
        Row(Disposition.NothingToLookUp, "No name to look up in the tag");

        var caveats = new List<string>();
        if (!hasCol) {
            caveats.Add("The Catalogue of Life database was unavailable, so only Wikidata and Wikipedia were checked.");
        }
        if (!hasSynonyms) {
            caveats.Add("The Red List API cache was unavailable, so taxa whose synonym list already names a described species could not be filtered out.");
        }
        if (others is null) {
            caveats.Add("Neither the Wikidata nor the Wikipedia cache was available, so only the Catalogue of Life was checked.");
        }

        return new AuditReport {
            Id = Id_,
            SectionId = "records",
            Title = "Provisional names that another catalogue now records as described",
            Action = ActionClass.ByHand,
            TriageRank = findings.Count > 0 ? 5 : 0,
            TriageReason = "A handful of rows, each a name that may have been described since.",
            DataSourceLabel = string.Join(" + ", sources),
            Blurb = "Assessments published under a provisional name where the Catalogue of Life, Wikidata or Wikipedia now holds a described name for the same taxon.",
            Summary = SummaryText(provisionalCount, caveats),
            Columns = Columns(),
            Findings = findings,
            ShowGroupCounts = true,
            SummaryTables = new List<AuditSummaryTable> {
                new() {
                    Title = "Every provisional name in the release",
                    Note = $"{provisionalCount:N0} assessed taxa carry a provisional name. What happened to each.",
                    Headers = new[] { "Outcome", "Taxa" },
                    Rows = rows,
                    NumericColumns = new[] { 1 },
                },
            },
        };
    }

    private static string SummaryText(int provisionalCount, IReadOnlyList<string> caveats) {
        var text =
            $"An assessment can be published for a species that has not been formally described, under a provisional name such as \"Notogomphus sp. nov. 'gorilla'\". {provisionalCount:N0} assessed taxa in this release carry one. Some of those species have since been described, and the table below lists the ones another catalogue appears to have a described name for.\n\n" +
            "A candidate binomial is built from the quoted tag, so \"Notogomphus sp. nov. 'gorilla'\" gives Notogomphus gorilla, and that name is looked up in the Catalogue of Life. Wikidata and the English Wikipedia are asked separately what name they hold against the same SIS id. Only a tag that reads as a species epithet produces a candidate: a locality, a collector code or a description gives nothing to look up.\n\n" +
            "Three exclusions keep the list conservative. A taxon whose Red List synonym list already names a described species is left out, because the link is already recorded. A tag qualified with \"cf.\" or \"aff.\" is never turned into a candidate, because it says the taxon resembles that species rather than being it. And a name match alone is a lead, not a determination: the same binomial can belong to a different taxon, so each row needs a person to confirm against the description.\n\n" +
            "### Why it matters\n\n" +
            "A provisional name is not an error. It is how an undescribed species gets assessed, and it is often the right record. But once the species is described, the assessment is filed under a name no other database uses, so the taxon is hard to link, hard to search for, and easy to assess twice under two names.\n\n" +
            "### Suggestion\n\n" +
            "Check each row against the publication the Catalogue of Life cites, and where it is the same taxon, record the described name. Adding it as a synonym would be enough to link the two catalogues.";
        if (caveats.Count > 0) {
            text += "\n\n" + string.Join(" ", caveats);
        }
        return text;
    }

    private static IReadOnlyList<AuditColumn> Columns() => new List<AuditColumn> {
        AuditColumns.ScientificName("IUCN name"),
        AuditColumns.Rank(),
        AuditColumns.Status(),
        AuditColumns.Year("IUCN year"),
        AuditColumns.SuggestedValue("Described name", AuditColumnType.Text),
        AuditColumns.Custom("colAuthority", "CoL authority", AuditColumnType.Text,
            "Authorship of the Catalogue of Life name, showing who described the species and when."),
        AuditColumns.Custom("colYear", "CoL year", AuditColumnType.Number,
            "Year of the Catalogue of Life name: its name-published year, or the year in its authority."),
        AuditColumns.Custom("colStatus", "CoL status", AuditColumnType.Text,
            "Whether the Catalogue of Life accepts that name or files it as a synonym of another."),
        AuditColumns.ColLink(),
        new AuditColumn {
            Key = "wikidataId", Header = "Wikidata", Type = AuditColumnType.Url,
            Value = f => f.Get("wikidataId"), Href = f => f.Get("wikidataUrl"),
            Help = "Wikidata item carrying this taxon's SIS id.",
        },
        new AuditColumn {
            Key = "wikipediaTitle", Header = "Wikipedia", Type = AuditColumnType.Url,
            Value = f => f.Get("wikipediaTitle"), Href = f => f.Get("wikipediaUrl"),
            Help = "English Wikipedia article matched to this taxon.",
        },
        AuditColumns.Group(),
        AuditColumns.Class(csvOnly: true),
        AuditColumns.Family(csvOnly: true),
        AuditColumns.TaxonId("Taxon id"),
        AuditColumns.RedlistLink(),
        AuditColumns.Detail(),
    };
}
