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
            // itself provisional is not a described name, so it does not count, and the row says so.
            var (described, stillProvisional) = hasSynonyms
                ? Synonyms(apiCache!, row.TaxonId)
                : (Array.Empty<string>(), Array.Empty<string>());
            if (described.Count > 0) {
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
            findings.Add(Build(row, name, parsed, col1, other, otherNames, stillProvisional, repo is not null));
        }

        var ordered = findings
            .OrderByDescending(f => f.SeverityTier)
            .ThenBy(TaxonGroups.SortKey, StringComparer.Ordinal)
            .ThenBy(f => f.ScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return BuildReport(ctx, ordered, provisionalCount, tally, repo is not null, hasSynonyms, others);
    }

    // The names IUCN already files as synonyms of this taxon, split into described names (which
    // answer the question and take the taxon off the list) and names that are themselves provisional
    // (which do not). One indexed read per provisional taxon, against the API cache's stored payload.
    private static (IReadOnlyList<string> Described, IReadOnlyList<string> Provisional) Synonyms(
        SqliteConnection apiCache, long taxonId) {
        using var command = apiCache.CreateCommand();
        command.CommandText = "SELECT json FROM taxa WHERE root_sis_id = @id LIMIT 1";
        command.Parameters.AddWithValue("@id", taxonId);
        command.CommandTimeout = 30;
        if (command.ExecuteScalar() is not string payload || string.IsNullOrWhiteSpace(payload)) {
            return (Array.Empty<string>(), Array.Empty<string>());
        }
        var described = new List<string>();
        var provisional = new List<string>();
        try {
            using var document = JsonDocument.Parse(payload);
            if (!document.RootElement.TryGetProperty("taxon", out var taxon) ||
                !taxon.TryGetProperty("synonyms", out var synonyms) || synonyms.ValueKind != JsonValueKind.Array) {
                return (described, provisional);
            }
            foreach (var synonym in synonyms.EnumerateArray()) {
                var genus = JsonString(synonym, "genus_name");
                var species = JsonString(synonym, "species_name");
                if (genus is null || species is null) {
                    continue;
                }
                var infra = JsonString(synonym, "infra_name");
                var name = infra is null ? $"{genus} {species}" : $"{genus} {species} {infra}";
                (ProvisionalNames.IsProvisional(name) ? provisional : described).Add(name);
            }
        } catch (JsonException) {
            // A payload this producer cannot read answers nothing; treat it as no synonyms rather
            // than dropping the taxon from the report.
        }
        return (described, provisional);
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

    private static AuditFinding Build(ProvisionalRow row, string name, ProvisionalName parsed, ColMatch? col,
        OtherSourceHit other, IReadOnlyList<string> otherNames, IReadOnlyList<string> provisionalSynonyms, bool colChecked) {
        var (rank, isFull) = AuditMapping.Rank(row.InfraType, row.Subpopulation);
        var colName = AuditMapping.Decode(col?.Record.ScientificName);
        var colAuthority = AuditMapping.Decode(col?.Record.Authorship);
        var colYear = ColYear(col?.Record);
        var iucnYear = Year(row.YearPublished);
        var describedSince = colYear is not null && iucnYear is not null && colYear > iucnYear;
        var describedName = colName ?? otherNames[0];

        // Strongest first: a name CoL accepts and dates after the assessment, then one CoL simply
        // accepts, then one CoL files under something else, then a name only the wikis carry.
        var severity = col is null ? 2 : col.IsAccepted ? (describedSince ? 5 : 4) : 3;

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
            SuggestedValue = describedName,
            IssueType = col is null ? "described-name-in-wikis"
                : col.IsAccepted ? (describedSince ? "described-since-assessment" : "described-name-in-col")
                : "candidate-name-is-a-col-synonym",
            SeverityTier = severity,
            Detail = Detail(col, describedName, colAuthority, colYear, iucnYear, other, otherNames, colChecked),
        };

        Set(finding, "candidateName", parsed.CandidateName);
        Set(finding, "colAuthority", colAuthority);
        Set(finding, "colYear", colYear?.ToString(CultureInfo.InvariantCulture));
        Set(finding, "colStatus", col is null ? null : col.IsAccepted ? "accepted" : "synonym");
        Set(finding, "colUrl", ColUrls.Taxon(col?.Record.Id));
        if (other.WikidataId is not null) {
            Set(finding, "wikidataId", other.WikidataId);
            Set(finding, "wikidataUrl", OtherSourceIndex.WikidataUrl(other.WikidataId));
        }
        var wikipediaTitle = other.WikipediaTitle is not null && !ProvisionalNames.IsProvisional(other.WikipediaTitle)
            ? other.WikipediaTitle : null;
        if (wikipediaTitle is not null) {
            Set(finding, "wikipediaTitle", wikipediaTitle);
            Set(finding, "wikipediaUrl", OtherSourceIndex.WikipediaUrl(wikipediaTitle));
        }

        AddNotes(finding, col, describedName, wikipediaTitle, otherNames, provisionalSynonyms, row);
        return finding;
    }

    private static void AddNotes(AuditFinding finding, ColMatch? col, string describedName, string? wikipediaTitle,
        IReadOnlyList<string> otherNames, IReadOnlyList<string> provisionalSynonyms, ProvisionalRow row) {
        // Only worth saying when CoL is the source of the described name; when the wikis are the
        // only source, the Detail has already named them.
        if (col is not null) {
            var onWikidata = otherNames.Any(n => string.Equals(n, describedName, StringComparison.OrdinalIgnoreCase))
                             && !string.Equals(wikipediaTitle, describedName, StringComparison.OrdinalIgnoreCase);
            var onWikipedia = string.Equals(wikipediaTitle, describedName, StringComparison.OrdinalIgnoreCase);
            var both = otherNames.Any(n => string.Equals(n, describedName, StringComparison.OrdinalIgnoreCase)) && onWikipedia;
            if (both) {
                finding.Notes.Add($"Wikidata and English Wikipedia also give {describedName} as this taxon's name.");
            } else if (onWikipedia) {
                finding.Notes.Add($"English Wikipedia also has an article on this taxon under {describedName}.");
            } else if (onWikidata) {
                finding.Notes.Add($"Wikidata also gives {describedName} as this taxon's name.");
            }
        }

        // Why this taxon survived the already-known exclusion: IUCN does record a synonym, but it is
        // another working name, so no described name is on record yet.
        if (provisionalSynonyms.Count > 0) {
            finding.Notes.Add($"IUCN lists {provisionalSynonyms[0]} as a synonym of this taxon; that name is also provisional, so it does not count as a described name.");
        }

        // Only when both sides actually name a family. IUCN writes "NOT ASSIGNED" where it records
        // none, which is not a family and must not be printed as one.
        if (col is not null && Named(row.Family) is { } iucnFamily && Named(col.Record.Family) is { } colFamily &&
            !string.Equals(iucnFamily, colFamily, StringComparison.OrdinalIgnoreCase)) {
            finding.Notes.Add($"CoL places that name in {colFamily} while IUCN places this taxon in {iucnFamily}, so the two may not be the same taxon.");
        }
    }

    // A family name, or null where the source records none. IUCN uses the literal "NOT ASSIGNED".
    private static string? Named(string? value) {
        var trimmed = value?.Trim();
        return string.IsNullOrEmpty(trimmed) || trimmed.Equals("NOT ASSIGNED", StringComparison.OrdinalIgnoreCase)
            ? null : trimmed;
    }

    // One sentence for what the Catalogue of Life makes of the name, plus one for how its year sits
    // against the assessment. The two are separate facts: "CoL records no year" and "the name already
    // existed when the taxon was assessed" say different things and a reader acts on them differently.
    private static string Detail(ColMatch? col, string describedName, string? colAuthority, int? colYear,
        int? iucnYear, OtherSourceHit other, IReadOnlyList<string> otherNames, bool colChecked) {
        if (col is null) {
            var source = SourcePhrase(other, otherNames);
            var colClause = colChecked
                ? "The Catalogue of Life lookup found no described name for it."
                : "The Catalogue of Life was not checked for this build.";
            return $"This taxon is named {describedName} on {source}. {colClause}";
        }

        // The authority follows the name unbracketed: "Heptapleurum nanocephalum (de Kok)" would read
        // to a botanist as a basionym author awaiting a combining author, a different claim.
        var named = string.IsNullOrWhiteSpace(colAuthority) ? describedName : $"{describedName} {colAuthority}";
        if (col.IsAccepted) {
            var head = $"{named} is an accepted name in the Catalogue of Life";
            return head + YearClause(colYear, iucnYear) ;
        }

        var target = AuditMapping.Decode(col.AcceptedTarget?.ScientificName);
        var synonymHead = target is null
            ? $"{named} is in the Catalogue of Life as a synonym, with no accepted name linked to it"
            : $"{named} is in the Catalogue of Life as a synonym of {target}";
        return synonymHead + YearClause(colYear, iucnYear);
    }

    private static string YearClause(int? colYear, int? iucnYear) {
        if (colYear is null) {
            return iucnYear is null
                ? "."
                : $". CoL records no publication year for it, so whether it predates the {iucnYear} assessment is unknown.";
        }
        if (iucnYear is null) {
            return $", published in {colYear}.";
        }
        if (colYear > iucnYear) {
            var gap = colYear.Value - iucnYear.Value;
            var span = gap == 1 ? "1 year" : $"{gap} years";
            return $", published in {colYear}, {span} after the {iucnYear} assessment.";
        }
        return $", published in {colYear}. The name already existed when the taxon was assessed in {iucnYear}.";
    }

    private static string SourcePhrase(OtherSourceHit other, IReadOnlyList<string> otherNames) {
        var wikidata = other.WikidataId is not null && otherNames.Count > 0;
        var wikipedia = other.WikipediaTitle is not null && !ProvisionalNames.IsProvisional(other.WikipediaTitle);
        return (wikidata, wikipedia) switch {
            (true, true) => "Wikidata and English Wikipedia",
            (false, true) => "English Wikipedia",
            _ => "Wikidata",
        };
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

        int Tally(Disposition d) => tally.TryGetValue(d, out var v) ? v : 0;
        var rows = new List<IReadOnlyList<string>> {
            Row("Listed: a described name was found", findings.Count),
            Row("Dropped: IUCN's synonym list already has a described name", Tally(Disposition.KnownToIucn)),
            Row("Dropped: tag qualified with cf. or aff.", Tally(Disposition.Qualified)),
            Row("Not listed: no described name found", Tally(Disposition.NoDescribedName)),
            Row("Not listed: no name to look up in the tag", Tally(Disposition.NothingToLookUp)),
        };

        return new AuditReport {
            Id = Id_,
            SectionId = "records",
            Title = "Provisional (sp. nov.) names with a described name in another source",
            Action = ActionClass.ByHand,
            TriageRank = findings.Count > 0 ? 5 : 0,
            TriageReason = "Few rows, each a specific described name to confirm against its publication.",
            DataSourceLabel = string.Join(" + ", sources),
            Blurb = "Taxa assessed under a provisional name (Genus sp. nov. 'epithet') for which the Catalogue of Life, Wikidata, or English Wikipedia now records a described name that IUCN's own synonym list does not.",
            Summary = SummaryText(ctx.Release, provisionalCount, Tally(Disposition.KnownToIucn), Tally(Disposition.Qualified),
                Tally(Disposition.NoDescribedName) + Tally(Disposition.NothingToLookUp), hasCol, hasSynonyms, others),
            Columns = Columns(others),
            Findings = findings,
            ShowGroupCounts = true,
            SummaryTables = new List<AuditSummaryTable> {
                new() {
                    Title = "By outcome of the check",
                    Note = $"Every assessed name containing sp. nov. or ssp. nov. in {ctx.Release}, each taxon counted once.",
                    Headers = new[] { "Outcome", "Taxa" },
                    Rows = rows,
                    NumericColumns = new[] { 1 },
                },
            },
        };
    }

    private static IReadOnlyList<string> Row(string label, int count) =>
        new[] { label, count.ToString("N0", CultureInfo.InvariantCulture) };

    private static string SummaryText(string release, int total, int known, int qualified, int none,
        bool hasCol, bool hasSynonyms, OtherSourceIndex? others) {
        var opening =
            "The table below lists taxa assessed under a provisional name (Genus sp. nov. 'epithet', for example Notogomphus sp. nov. 'gorilla') for which the Catalogue of Life, Wikidata, or English Wikipedia now records a described name that IUCN's own synonym list does not. " +
            "Assessing a species before it is formally described is deliberate and valid, and the provisional name is expected in that case. Each row is a case where a described name may since have been published for the same species.";
        if (!hasSynonyms) {
            opening += " IUCN synonym data from the Red List API was not available for this build, so the check for described names IUCN already lists as synonyms did not run. Some rows may be relationships IUCN already records.";
        }
        if (others is null) {
            opening += " The Wikidata and Wikipedia check did not run for this build, so only the Catalogue of Life was consulted.";
        }
        if (!hasCol) {
            opening += " The Catalogue of Life database was not available for this build, so only Wikidata and Wikipedia were consulted.";
        }

        return opening + "\n\n" +
            $"Every assessed taxon whose name contains sp. nov. or ssp. nov. is collected ({total:N0} in {release}). " +
            "Where the quoted tag is one plain lower-case word, joining it to the genus gives a candidate binomial: Notogomphus sp. nov. 'gorilla' gives Notogomphus gorilla. " +
            "A tag that is a locality, a collector code, or a description ('Bavispe Trout', 'HC - blind', 'B = Bester 11112') gives no candidate. " +
            "Each candidate is matched against CoL as an exact name, so a described name published under a different genus, or with a different ending, is reachable only through the other two sources. " +
            "Separately, Wikidata and English Wikipedia are checked by IUCN taxon id for any non-provisional name they record for the taxon.\n\n" +
            "Two exclusions keep the list conservative. " +
            $"Where IUCN's own synonym list for the taxon (published through the Red List API; the CSV export carries no synonyms) already holds a described name, the described name is already on record and the row is dropped ({known:N0} taxa this release). " +
            "A synonym that is itself provisional does not count, so those taxa stay listed. " +
            $"Where the tag carries cf. or aff., as in Barbus sp. nov. 'cf. gurneyi', the assessor compared the species with gurneyi and left its identity open, so the row is dropped ({qualified:N0} taxa). " +
            $"The remaining {none:N0} taxa have no described name in any source checked. The table under this description gives the breakdown.\n\n" +
            "A match is a lead. The same binomial can belong to a different species from the one assessed, and only a reader of the published description can confirm that the two names refer to the same taxon. " +
            "The catalogues checked change between releases, so this page is rebuilt with each one, and a taxon absent now may appear later.\n\n" +
            "### Why it matters\n\n" +
            "Once a species is described, its provisional name and its binomial are two different strings, and no field on either side points to the other. " +
            "A search for the described name does not find the assessment, a reader of the assessment has no route to the description, and any database that matches the Red List by name files the taxon as unmatched. " +
            "Where both years are given, the IUCN year and CoL year columns show the gap between the assessment and the description.\n\n" +
            "### Suggestion\n\n" +
            "Check each row against the publication in the CoL authority column, where given. " +
            "Where the described species is the one that was assessed, recording the described name on the assessment, as a synonym now or as the accepted name at the next reassessment, would let the two records join. " +
            "Where CoL treats the described name as a synonym, the accepted name given in the Detail column is the one to compare. Where it is a different species, no change is needed.";
    }

    // Wikidata and Wikipedia columns appear only when their caches were read. Leaving them out beats
    // two blank columns, which read as "checked, found nothing".
    private static IReadOnlyList<AuditColumn> Columns(OtherSourceIndex? others) {
        var columns = new List<AuditColumn> {
            AuditColumns.ScientificName("IUCN name"),
            AuditColumns.Rank(),
            AuditColumns.Status("IUCN status"),
            new AuditColumn {
                Key = "yearPublished", Header = "IUCN year", Type = AuditColumnType.Number,
                Value = f => f.YearPublished,
                Help = "Year the current assessment was published. Compare with CoL year.",
            },
            AuditColumns.SuggestedValue("Described name", AuditColumnType.Text),
            AuditColumns.Custom("colAuthority", "CoL authority", AuditColumnType.Text,
                "Authorship (with year) of the described name in the Catalogue of Life. Blank when the name was found only on Wikidata or Wikipedia."),
            AuditColumns.Custom("colYear", "CoL year", AuditColumnType.Number,
                "Year of the Catalogue of Life name: its name-published year, or the year in its authority."),
            AuditColumns.Custom("colStatus", "CoL status", AuditColumnType.Text,
                "Whether the Catalogue of Life treats the described name as an accepted name or as a synonym of another name. Blank when the name is not in CoL."),
            AuditColumns.ColLink(),
        };

        if (others?.HasWikidata == true) {
            columns.Add(new AuditColumn {
                Key = "wikidataId", Header = "Wikidata", Type = AuditColumnType.Url,
                Value = f => f.Get("wikidataId"), Href = f => f.Get("wikidataUrl"),
                Help = "The Wikidata item linked to this taxon's IUCN id. Blank: no item found.",
            });
        }
        if (others?.HasWikipedia == true) {
            columns.Add(new AuditColumn {
                Key = "wikipediaTitle", Header = "Wikipedia", Type = AuditColumnType.Url,
                Value = f => f.Get("wikipediaTitle"), Href = f => f.Get("wikipediaUrl"),
                Help = "The English Wikipedia article matched to this taxon. Blank: no article found.",
            });
        }

        columns.AddRange(new[] {
            AuditColumns.Group(),
            AuditColumns.Class(csvOnly: true),
            AuditColumns.Family(csvOnly: true),
            AuditColumns.TaxonId("Taxon id"),
            AuditColumns.RedlistLink(),
            AuditColumns.Detail(),
        });
        return columns;
    }
}
