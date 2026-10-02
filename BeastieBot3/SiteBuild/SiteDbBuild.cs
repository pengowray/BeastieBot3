using System.Diagnostics;
using System.Globalization;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;
using Spectre.Console;

// Builds the public site's database from the local caches (`site build-db`). The phases, in order:
//
//   1. IUCN CSV export: every taxon (the backbone) and its latest assessments.
//   2. IUCN API taxon records: assessment history, IUCN common names and synonyms.
//   3. Plan the assessment rows.
//   4. DOI sources: GBIF's checklist and Wikidata.
//   5. IUCN API assessment payloads: citation parts; the assessment rows are written here.
//   6. Common names store: English names, the best English name, CoL synonyms.
//   7. Links: English Wikipedia, Wikidata, Catalogue of Life, SPRAT.
//   8. Parents, then the taxon and name rows, then meta.
//   9. Name keys, indexes, full-text index, ANALYZE, VACUUM.
//
// The database is written to "<output>.building" and moved over the output only when every phase
// has finished, so a failed or cancelled build leaves the old database as it was.

namespace BeastieBot3.SiteBuild;

internal sealed class SiteDbBuild {
    private readonly SiteBuildInputs _inputs;
    private readonly IAnsiConsole _console;
    private readonly SiteBuildStats _stats = new();

    public SiteDbBuild(SiteBuildInputs inputs, IAnsiConsole console) {
        _inputs = inputs;
        _console = console;
    }

    public static string TempPathFor(string output) => output + ".building";

    public SiteBuildStats Run(CancellationToken cancellationToken) {
        var total = Stopwatch.StartNew();
        var tempPath = TempPathFor(_inputs.Output);
        try {
            BuildInto(tempPath, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            SqliteConnection.ClearAllPools();
            File.Move(tempPath, _inputs.Output, overwrite: true);
            _stats.FileBytes = new FileInfo(_inputs.Output).Length;
        } finally {
            SqliteConnection.ClearAllPools();
            if (File.Exists(tempPath)) {
                SiteDbWriter.DeleteDatabaseFiles(tempPath);
            }
        }
        _stats.Phases.Add(("Total", total.Elapsed));
        return _stats;
    }

    private void BuildInto(string tempPath, CancellationToken ct) {
        using var writer = SiteDbWriter.Create(tempPath);

        // 1. The CSV export.
        List<SiteTaxon> taxonList = null!;
        List<SiteAssessment> csvAssessments = null!;
        Phase("Reading the IUCN CSV export", () => {
            using var csv = SiteIucnCsvReader.OpenReadOnly(_inputs.IucnDatabase);
            _stats.IucnRelease = SiteIucnCsvReader.ReadRelease(csv);
            taxonList = SiteIucnCsvReader.ReadTaxa(csv, _inputs.Limit, ct);
            var byId = taxonList.ToDictionary(t => t.TaxonId);
            csvAssessments = SiteIucnCsvReader.ReadAssessments(csv, byId, _stats, ct);
            return $"{taxonList.Count:N0} taxa, {csvAssessments.Count:N0} latest assessments, release {_stats.IucnRelease}";
        });
        var taxa = taxonList.ToDictionary(t => t.TaxonId);
        foreach (var taxon in taxonList) {
            _stats.Count(_stats.TaxaByKind, taxon.Kind);
        }

        using var cache = SiteIucnCsvReader.OpenReadOnly(_inputs.ApiCache);

        // 2. API taxon records.
        var apiTaxa = new SiteApiTaxaReader(taxa, _stats);
        Phase("Reading IUCN API taxon records", () => {
            apiTaxa.Read(cache, readAll: _inputs.Limit is null, ct);
            return $"{apiTaxa.Records.Count:N0} of the taxa have a record";
        });

        // 3. Plan.
        var assessments = new SiteAssessmentPass(taxa, apiTaxa.Records, _stats);
        Phase("Planning assessment rows", () => {
            assessments.Plan(csvAssessments);
            csvAssessments = null!;
            return $"{assessments.PlannedCount:N0} assessments";
        });

        // 4. DOI sources.
        var dois = new SiteDoiSources();
        Optional("GBIF checklist", _inputs.GbifChecklist, path => {
            var (version, published) = SiteLinkReaders.ReadGbif(path, taxa, dois, ct);
            _stats.GbifVersion = version;
            _stats.GbifPublished = published;
            return $"{dois.Gbif.Count:N0} DOIs, version {version}, published {published}";
        });
        Optional("Wikidata cache", _inputs.WikidataCache, path => {
            SiteLinkReaders.ReadWikidata(path, taxa, _stats, dois, ct);
            return $"{_stats.QidsFromP627 + _stats.QidsFromNameMatch:N0} items, {dois.Wikidata.Count:N0} assessment DOIs";
        });

        // 5. Payloads; the assessment rows are written here.
        Phase("Reading IUCN API assessment payloads", () => {
            assessments.WriteAll(cache, writer, dois, ct);
            return $"{_stats.CitationsParsed:N0} citations parsed";
        });

        // 6. Common names store.
        var colCrossReferences = new Dictionary<long, string>();
        Optional("Common names store", _inputs.CommonNames, path => {
            SiteCommonNamesReader.Read(path, taxa, _stats, colCrossReferences, ct);
            return $"{_stats.CommonNameEn:N0} best English names";
        });

        // 7. Links.
        Optional("Wikipedia cache", _inputs.WikipediaCache, path => {
            SiteLinkReaders.ReadWikipedia(path, taxa, _stats, ct);
            return $"{_stats.EnwikiTitles:N0} articles";
        });
        Optional("Catalogue of Life placement", _inputs.ColPlacement, path => {
            _stats.ColRelease = SiteLinkReaders.ReadColPlacement(path, taxa, _stats, ct);
            return $"{_stats.ColIdsFromPlacement:N0} species";
        });
        SiteLinkReaders.ApplyColCrossReferences(taxa, colCrossReferences, _stats);
        _stats.ColRelease ??= SiteBuildRules.ColReleaseFromPath(_inputs.ColDatabase);
        Optional("SPRAT database", _inputs.SpratDatabase, path => {
            _stats.SpratReport = SiteLinkReaders.ReadSprat(path, taxa, _stats, ct);
            return $"{_stats.SpratMatched:N0} taxa matched, {_stats.EpbcStatuses:N0} listed";
        });

        // 8. Parents, taxa, names, meta.
        Phase("Writing taxa and names", () => {
            SetParents(taxonList, taxa, apiTaxa.SubpopulationParents);
            foreach (var taxon in taxonList) {
                ct.ThrowIfCancellationRequested();
                writer.AddTaxon(taxon);
                WriteNames(writer, taxon);
            }
            return $"{taxonList.Count:N0} taxa, {writer.NameCount:N0} names";
        });
        WriteMeta(writer, taxonList.Count);

        // 9. Indexes and compaction.
        writer.Finish((name, body) => Phase(name, () => {
            ct.ThrowIfCancellationRequested();
            body();
            return null;
        }));
    }

    // ------------------------------------------------------------ parents

    // The species of an infraspecific taxon, by "Genus species" among the CSV's species (same kingdom
    // first); the taxon a subpopulation belongs to, by its name without the subpopulation part. When
    // the name finds nothing, the API record's species (species_taxa) or the record that lists the
    // subpopulation. Only a taxon that is in the site database is stored as the parent.
    private void SetParents(List<SiteTaxon> taxonList, IReadOnlyDictionary<long, SiteTaxon> taxa,
        IReadOnlyDictionary<long, long> subpopulationParents) {
        var byName = new Dictionary<string, List<SiteTaxon>>(StringComparer.Ordinal);
        foreach (var taxon in taxonList) {
            if (taxon.Kind == SiteTaxonKind.Subpopulation) {
                continue;
            }
            if (!byName.TryGetValue(taxon.ScientificName, out var list)) {
                byName[taxon.ScientificName] = list = new List<SiteTaxon>();
            }
            list.Add(taxon);
        }

        foreach (var taxon in taxonList) {
            string? parentName = taxon.Kind switch {
                SiteTaxonKind.Subpopulation when taxon.SubpopulationName is not null =>
                    SiteBuildRules.SubpopulationParentName(taxon.ScientificName, taxon.SubpopulationName),
                SiteTaxonKind.Subspecies or SiteTaxonKind.Variety when taxon.Genus is not null && taxon.SpeciesEpithet is not null =>
                    $"{taxon.Genus} {taxon.SpeciesEpithet}",
                _ => null,
            };
            if (taxon.Kind == SiteTaxonKind.Species) {
                continue;
            }
            if (parentName is not null && byName.TryGetValue(parentName, out var candidates)) {
                var parent = candidates.FirstOrDefault(c => c.TaxonId != taxon.TaxonId && c.Kingdom == taxon.Kingdom)
                    ?? candidates.FirstOrDefault(c => c.TaxonId != taxon.TaxonId);
                if (parent is not null) {
                    taxon.ParentTaxonId = parent.TaxonId;
                    _stats.ParentsByName++;
                    continue;
                }
            }
            long? fallback = taxon.Kind == SiteTaxonKind.Subpopulation
                ? (subpopulationParents.TryGetValue(taxon.TaxonId, out var root) ? root : null)
                : taxon.ApiSpeciesId;
            if (fallback is { } id && id != taxon.TaxonId && taxa.ContainsKey(id)) {
                taxon.ParentTaxonId = id;
                _stats.ParentsFromApi++;
            } else {
                _stats.ParentsMissing++;
            }
        }
    }

    // ------------------------------------------------------------ names

    // Order: the scientific name, IUCN's common names (main first), the other English names, IUCN's
    // synonyms, then CoL's. The order sets name_id, which the site uses to order names within a list.
    private void WriteNames(SiteDbWriter writer, SiteTaxon taxon) {
        var names = new SiteNameSet();
        names.Add(taxon.ScientificName, SiteNameType.Scientific, null, SiteNameSource.Iucn, isPreferred: true);
        foreach (var name in taxon.IucnCommonNames) {
            names.Add(name.Name, SiteNameType.Common, name.Language, SiteNameSource.Iucn, name.IsMain);
        }
        foreach (var (name, source, preferred) in taxon.EnglishNames
                     .OrderBy(n => SourceOrder(n.Source))) {
            names.Add(name, SiteNameType.Common, "en", source, preferred);
        }
        foreach (var synonym in taxon.IucnSynonyms) {
            names.Add(synonym, SiteNameType.Synonym, null, SiteNameSource.Iucn);
        }
        foreach (var synonym in taxon.ColSynonyms) {
            names.Add(synonym, SiteNameType.Synonym, null, SiteNameSource.Col);
        }
        foreach (var name in names.Names) {
            writer.AddName(taxon.TaxonId, name);
            _stats.Count(_stats.NamesByType, name.NameType);
            if (name.NameType == SiteNameType.Common && name.Language == "en") {
                _stats.CommonNamesEnglish++;
            }
        }
        // The lists are not needed again.
        taxon.IucnCommonNames = new List<IucnCommonName>();
        taxon.IucnSynonyms = new List<string>();
        taxon.EnglishNames.Clear();
        taxon.ColSynonyms.Clear();
    }

    private static int SourceOrder(string source) => source switch {
        SiteNameSource.Iucn => 0,
        SiteNameSource.Wikipedia => 1,
        SiteNameSource.Wikidata => 2,
        SiteNameSource.Col => 3,
        _ => 4,
    };

    // ------------------------------------------------------------ meta

    private void WriteMeta(SiteDbWriter writer, int taxonCount) {
        var assessmentCount = writer.Scalar("SELECT COUNT(*) FROM assessment");
        writer.SetMeta(SiteDbSchema.MetaKeys.SchemaVersion, SiteDbSchema.Version.ToString(CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.BuiltAtUtc, DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.IucnRelease, _stats.IucnRelease);
        writer.SetMeta(SiteDbSchema.MetaKeys.IucnApiDownloadedFrom, _stats.DownloadedFrom?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.IucnApiDownloadedTo, _stats.DownloadedTo?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.GbifChecklistVersion, _stats.GbifVersion);
        writer.SetMeta(SiteDbSchema.MetaKeys.GbifChecklistPublished, _stats.GbifPublished);
        writer.SetMeta(SiteDbSchema.MetaKeys.ColRelease, _stats.ColRelease);
        writer.SetMeta(SiteDbSchema.MetaKeys.SpratReport, _stats.SpratReport);
        writer.SetMeta(SiteDbSchema.MetaKeys.TaxonCount, taxonCount.ToString(CultureInfo.InvariantCulture));
        writer.SetMeta(SiteDbSchema.MetaKeys.AssessmentCount, assessmentCount);
    }

    // ------------------------------------------------------------ phases

    private void Phase(string name, Func<string?> body) {
        var watch = Stopwatch.StartNew();
        _console.MarkupLineInterpolated($"[grey]{name}...[/]");
        var detail = body();
        watch.Stop();
        _stats.Phases.Add((name, watch.Elapsed));
        var seconds = watch.Elapsed.TotalSeconds.ToString("N1", CultureInfo.InvariantCulture);
        if (detail is null) {
            _console.MarkupLineInterpolated($"  {name}: done in {seconds} s");
        } else {
            _console.MarkupLineInterpolated($"  {name}: {detail} ({seconds} s)");
        }
    }

    private void Optional(string sourceName, string? path, Func<string, string?> body) {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path)) {
            _stats.MissingSources.Add(sourceName);
            var where = string.IsNullOrWhiteSpace(path) ? "no path is configured" : $"{path} does not exist";
            _console.MarkupLineInterpolated($"[yellow]Skipped the {sourceName}: {where}. The site database gets no data from it.[/]");
            return;
        }
        Phase($"Reading the {sourceName}", () => body(path));
    }
}
