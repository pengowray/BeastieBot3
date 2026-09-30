using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Web.Endpoints;

// Dry run for updating IUCN conservation statuses on Wikidata. Reads only local caches: the IUCN
// API cache (latest global assessment per taxon), the Wikidata cache (items, the links between
// taxa and items, existing assessment items). Sends nothing to Wikidata.
//
// For each taxon-item pair: classify the link (TaxonLinkClassifier), plan the P141/P627 changes in
// each rank variant (IucnStatusEditPlanner), keep the pair in the plan store, and keep a few
// example wbeditentity payloads per tier, category and variant for people to read.
//
// Items are streamed, not loaded, so memory holds the assessments and the links but only one item
// at a time.

namespace BeastieBot3.WikidataEdits;

[CommandInfo("wikidata iucn-status-plan", CommandKind.Mutates,
    "Dry run: plan updates to the IUCN conservation status (P141) of Wikidata taxon items from the latest global assessments, each status cited to the Red List release and to its own assessment. Stores the plan and writes a report; sends nothing to Wikidata.",
    Reason = "Replaces the stored plan and overwrites the report files for the release. Reads the local caches only; makes no Wikidata edits.",
    Rerun = RerunEffect.Rebuilds,
    Examples = new[] {
        "wikidata iucn-status-plan",
        "wikidata iucn-status-plan --limit 2000",
        "wikidata iucn-status-plan --samples 10"
    })]
internal sealed class WikidataIucnStatusPlanCommand : AsyncCommand<WikidataIucnStatusPlanCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--limit <N>")]
        [Description("Stop after this many Wikidata items linked to IUCN taxa (0 = all). A run that stops at the limit replaces the stored plan and the report with partial versions. For a complete plan and report, run again without --limit.")]
        public int Limit { get; init; }

        [CommandOption("--samples <N>")]
        [Description("Sample edits to keep per confidence tier, change type and rank variant (default 5).")]
        public int Samples { get; init; } = 5;

        [CommandOption("-o|--output <DIR>")]
        [Description("Folder for the report files (defaults to the configured reports folder).")]
        public string? OutputDirectory { get; init; }
    }

    private sealed record Sample(string Tier, string Category, string Variant, long TaxonId, string ScientificName,
        IReadOnlyList<LinkFlag> Flags, WbEdit Edit);

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        return Task.FromResult(Run(settings, cancellationToken));
    }

    private static int Run(Settings settings, CancellationToken ct) {
        var paths = settings.CreatePaths();
        var apiCache = paths.GetIucnApiCachePath();
        var wikidataCache = paths.GetWikidataCachePath();
        var planPath = paths.GetWikidataIucnPlanPath();
        if (!File.Exists(apiCache) || !File.Exists(wikidataCache) || planPath is null) {
            AnsiConsole.MarkupLine("[red]Missing database:[/] this needs the IUCN API cache and the Wikidata cache (IUCN_api_cache_sqlite and wikidata_cache_sqlite in paths.ini).");
            return -1;
        }

        var config = LoadConfig(paths, out var configPath);
        AnsiConsole.MarkupLineInterpolated($"[grey]Settings:[/] {configPath ?? "built-in defaults (rules/wikidata/iucn-status.yml not found)"}");
        if (config.EditionItemOrNull is null) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]No Wikidata item set for the {config.Release} release.[/] Planned edits cite a placeholder until edition_item is set in the settings file.");
        }

        var started = DateTime.UtcNow;
        var tally = new WikidataIucnPlanTally();

        // ---- IUCN: latest global assessment per taxon
        AnsiConsole.MarkupLine("[grey]Reading IUCN assessments...[/]");
        var readCounts = new IucnGlobalAssessmentReadCounts();
        Dictionary<long, IucnGlobalAssessment> assessments;
        using (var reader = IucnGlobalAssessmentReader.Open(apiCache!)) {
            assessments = reader.ReadAll(readCounts, ct).ToDictionary(a => a.TaxonId);
        }
        tally.TaxaWithGlobalAssessment = assessments.Count;
        tally.TaxaWithoutGlobalAssessment = readCounts.NoGlobalAssessment;
        tally.TaxaNotEligible = readCounts.SkippedVarieties + readCounts.SkippedSubpopulations;
        var currentTaxonIds = assessments.Keys.ToHashSet();

        // ---- Links: strongest source per (taxon, item), taxa in this release only
        AnsiConsole.MarkupLine("[grey]Reading links between taxa and Wikidata items...[/]");
        TaxonItemLinkSet linkSet;
        using (var linkReader = TaxonItemLinkReader.Open(wikidataCache!)) {
            linkSet = linkReader.ReadAll();
        }
        var links = new Dictionary<(long, string), TaxonItemLink>();
        foreach (var link in linkSet.Links) {
            if (!currentTaxonIds.Contains(link.TaxonId)) {
                tally.LinksToTaxaOutsideRelease++;
                continue;
            }
            var key = (link.TaxonId, link.Qid);
            if (!links.TryGetValue(key, out var existing) || link.Source < existing.Source) {
                links[key] = link;
            }
        }
        var linksByItem = links.Values.GroupBy(l => l.Qid, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);
        var classification = new LinkClassificationContext {
            CurrentTaxonIds = currentTaxonIds,
            ItemsPerTaxonIdClaim = links.Values.Where(l => l.Source == LinkSource.P627Claim)
                .GroupBy(l => l.TaxonId).ToDictionary(g => g.Key, g => g.Count()),
            TaxaPerItem = linksByItem.ToDictionary(kv => kv.Key, kv => kv.Value.Count, StringComparer.Ordinal),
        };
        var itemsPerTaxon = links.Values.GroupBy(l => l.TaxonId).ToDictionary(g => g.Key, g => g.Count());
        tally.TaxaWithSeveralItems = itemsPerTaxon.Count(kv => kv.Value > 1);

        // ---- Existing assessment items
        var existingItems = ExistingAssessmentItemReader.Read(wikidataCache!).ByAssessmentId;

        using var store = WikidataIucnPlanStore.Open(planPath);
        var decisions = store.LoadReviewDecisions();
        var runId = store.BeginRun(config.Release, config.EditionItemOrNull, started);
        store.ClearPairs();

        // ---- Items
        AnsiConsole.MarkupLine("[grey]Planning edits item by item...[/]");
        var variants = config.Variants.DefaultIfEmpty(EditVariant.Preferred).Distinct().ToList();
        var samplesWanted = Math.Max(0, settings.Samples);
        var samples = new List<Sample>();
        var sampleCounts = new Dictionary<string, int>();
        var assessmentSamples = new List<JsonObject>();
        var batch = new List<PlanPairRow>(1000);
        var itemsWithLinks = 0L;
        var stoppedAtLimit = false;

        using (var items = WdTaxonItemReader.Open(wikidataCache!)) {
            foreach (var item in items.ReadAll(ct)) {
                tally.ItemsRead++;
                if (!linksByItem.TryGetValue(item.Qid, out var itemLinks)) continue;
                itemsWithLinks++;
                tally.SeeItemDownload(item.DownloadedAtUtc, started);

                foreach (var link in itemLinks) {
                    var assessment = assessments[link.TaxonId];
                    if (decisions.TryGetValue((link.TaxonId, item.Qid), out var decision)) {
                        if (decision == "rejected") { tally.ReviewRejected++; continue; }
                        tally.ReviewConfirmed++;
                    }

                    var cls = TaxonLinkClassifier.Classify(assessment, link, item, classification);
                    var existingItem = existingItems[assessment.AssessmentId].FirstOrDefault();
                    var plan = IucnStatusEditPlanner.Plan(assessment, item, existingItem, config, currentTaxonIds);
                    var row = new PlanPairRow(link.TaxonId, item.Qid, assessment.ScientificName, assessment.AssessmentId,
                        assessment.CategoryCode, link.Source, cls.Tier, cls.Flags, plan.Category, plan.TargetValueQid,
                        plan.CurrentValueQids, plan.AssessmentRef, plan.CreatesAssessmentItem, plan.VariantsDiffer,
                        item.LastRevId, plan.Actions);
                    tally.Add(row);
                    batch.Add(row);
                    if (batch.Count >= 1000) { store.InsertPairs(batch); batch.Clear(); }

                    if (plan.Category is not (PlanCategory.NoChange or PlanCategory.UnmappedCategory)) {
                        foreach (var variant in plan.VariantsDiffer ? variants : variants.Take(1)) {
                            var bucket = $"{cls.Tier}|{plan.Category}|{variant}";
                            var have = sampleCounts.GetValueOrDefault(bucket);
                            if (have >= samplesWanted) continue;
                            sampleCounts[bucket] = have + 1;
                            samples.Add(new Sample(cls.Tier.ToString(), plan.Category.ToString(),
                                plan.VariantsDiffer ? variant.ToString().ToLowerInvariant() : "both",
                                link.TaxonId, assessment.ScientificName, cls.Flags,
                                WbEditPayloadBuilder.Build(item, assessment, plan, variant, config)));
                        }
                        if (plan.CreatesAssessmentItem && assessmentSamples.Count < samplesWanted * 4) {
                            assessmentSamples.Add(AssessmentItemPayloadBuilder.Build(assessment, item.Qid, config));
                        }
                    }
                }

                if (settings.Limit > 0 && itemsWithLinks >= settings.Limit) {
                    stoppedAtLimit = true;
                    break;
                }
            }
        }
        store.InsertPairs(batch);

        var linkedItemIds = linksByItem.Keys.ToHashSet(StringComparer.Ordinal);
        tally.TaxaWithNoItem = assessments.Count - itemsPerTaxon.Count;
        // Only a run that read every item can say which linked items it never saw. --limit on its own
        // does not mean the run stopped early: a limit above the number of linked items is never reached.
        tally.StoppedAtLimit = stoppedAtLimit ? settings.Limit : null;
        tally.ItemsLinkedButNotDownloaded = stoppedAtLimit ? 0 : Math.Max(0, linkedItemIds.Count - itemsWithLinks);

        // ---- Report files
        // Top level of the reports folder: the workflow page's "latest file" links only look there.
        var dir = ReportPathResolver.ResolveDirectory(paths, settings.OutputDirectory, null);
        var stem = Path.Combine(dir, $"wikidata-iucn-status-{config.Release}");
        var csvPath = stem + ".csv";
        var samplesPath = stem + "-sample-edits.jsonl";
        var assessmentSamplesPath = stem + "-sample-assessment-items.jsonl";

        WriteCsv(store, csvPath);
        WriteSamples(samples, samplesPath);
        File.WriteAllLines(assessmentSamplesPath, assessmentSamples.Select(s => s.ToJsonString()));
        var mdPath = stem + ".md";
        File.WriteAllText(mdPath, WikidataIucnPlanReport.Write(tally, config, started, csvPath, samplesPath, assessmentSamplesPath));

        store.FinishRun(runId, DateTime.UtcNow, tally.OldestItemDownloadUtc, tally);

        PrintSummary(tally);
        AnsiConsole.MarkupLineInterpolated($"[green]Report:[/] {mdPath} (CSV of every pair and sample files beside it)");
        AnsiConsole.MarkupLineInterpolated($"[grey]Plan stored in {planPath}. Nothing was sent to Wikidata.[/]");
        return 0;
    }

    private static WikidataIucnEditConfig LoadConfig(PathsService paths, out string? loadedFrom) {
        loadedFrom = null;
        try {
            var rules = RulesPaths.Resolve(paths);
            foreach (var dir in new[] { rules.SourceRulesDir, rules.BuildOutputRulesDir }) {
                var path = WikidataIucnEditConfig.PathFor(dir);
                if (File.Exists(path)) {
                    loadedFrom = path;
                    return WikidataIucnEditConfig.Load(dir);
                }
            }
        } catch {
            // fall through to defaults
        }
        return new WikidataIucnEditConfig();
    }

    private static void WriteCsv(WikidataIucnPlanStore store, string path) {
        using var writer = new StreamWriter(path);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);
        foreach (var header in new[] { "taxon_id", "scientific_name", "qid", "tier", "flags", "category", "iucn_code",
                     "target_value", "current_values", "link_source", "assessment_id", "assessment_ref", "creates_assessment_item",
                     "variants_differ", "base_rev_id", "actions" }) {
            csv.WriteField(header);
        }
        csv.NextRecord();
        foreach (var row in store.ReadPairsForExport()) {
            foreach (var field in row) csv.WriteField(field);
            csv.NextRecord();
        }
    }

    private static void WriteSamples(IEnumerable<Sample> samples, string path) {
        using var writer = new StreamWriter(path);
        foreach (var s in samples.OrderBy(s => s.Tier).ThenBy(s => s.Category).ThenBy(s => s.Variant)) {
            var line = new JsonObject {
                ["tier"] = s.Tier,
                ["category"] = s.Category,
                ["variant"] = s.Variant,
                ["taxon_id"] = s.TaxonId,
                ["scientific_name"] = s.ScientificName,
                ["flags"] = new JsonArray(s.Flags.Select(f => (JsonNode)JsonValue.Create(f.ToString())!).ToArray()),
                ["wbeditentity"] = new JsonObject {
                    ["id"] = s.Edit.Qid,
                    ["baserevid"] = s.Edit.BaseRevId,
                    ["summary"] = s.Edit.Summary,
                    ["data"] = s.Edit.Data.DeepClone(),
                },
            };
            writer.WriteLine(line.ToJsonString());
        }
    }

    internal static string SummaryTitle(WikidataIucnPlanTally t) => t.StoppedAtLimit is { } limit
        ? $"Planned changes, pairs (partial: stopped at --limit {limit})"
        : "Planned changes, pairs (A and B would be edited; C and D need a person first)";

    private static void PrintSummary(WikidataIucnPlanTally t) {
        var table = new Table().Border(TableBorder.Simple).Title(Markup.Escape(SummaryTitle(t)));
        table.AddColumn("Tier");
        var categories = new[] { PlanCategory.StatusChanged, PlanCategory.StatusAdded, PlanCategory.ReferencesOnly, PlanCategory.NoChange, PlanCategory.UnmappedCategory };
        var headers = new[] { "Status changed", "Status added", "References added", "Nothing to change", "No P141 value" };
        foreach (var h in headers) table.AddColumn(new TableColumn(h).RightAligned());
        foreach (var tier in Enum.GetValues<ConfidenceTier>()) {
            table.AddRow(new[] { tier.ToString() }.Concat(categories.Select(c => t.Count(tier, c).ToString("n0"))).ToArray());
        }
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Taxa:[/] {t.TaxaWithGlobalAssessment:n0} with a global assessment · {t.TaxaWithNoItem:n0} with no Wikidata item · {t.TaxaWithSeveralItems:n0} linked to more than one item");
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]Also planned:[/] {t.TaxonIdsToAdd:n0} IUCN taxon ids (P627) to add · {t.TaxonIdsToDeprecate:n0} renumbered ids to mark deprecated · {t.AssessmentItemsToCreate:n0} assessment items to create, {t.AssessmentItemsReused:n0} existing reused");
    }
}
