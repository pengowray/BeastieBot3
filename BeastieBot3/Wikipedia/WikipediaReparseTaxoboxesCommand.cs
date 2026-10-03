using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Infrastructure;
using BeastieBot3.Taxonomy;

// Parses the taxobox of every downloaded Wikipedia page again, from the wikitext already in the
// cache, and saves the fields that changed. wiki_taxobox_data is written when a page is downloaded
// (WikipediaPageFetcher), so a fix to TaxoboxParser otherwise only reaches pages downloaded after
// it. The October 2026 fix (nested templates keep both braces; parameters split the way MediaWiki
// splits them) changed the name parameter on about 1,000 of the 98,500 cached taxoboxes.
//
// Pages are read in row id order a batch at a time, and each batch's changes are saved in one
// transaction, so an interrupted run loses at most one batch and a second run finds only what is
// left. --dry-run opens the cache read-only.

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia reparse-taxoboxes", CommandKind.Mutates,
    "Read the taxobox of every downloaded Wikipedia page again from the wikitext in the cache, and save the fields that changed. Taxobox fields are saved when a page is downloaded, so for pages downloaded before a change to the taxobox parser, the cache has the fields that the earlier parser read. Nothing is downloaded. Afterwards, run common-names aggregate --source wikipedia --replace to read the common names again from the new fields. With --dry-run, the command counts the pages that would change and saves nothing.",
    Rerun = RerunEffect.Rebuilds,
    RerunNote = "Each run parses the taxobox of every downloaded page and saves only the fields that differ from the saved ones, so a second run with the same parser saves nothing. With --dry-run, nothing is saved.",
    ReportOnlyWith = new[] { "--dry-run" },
    Examples = new[] {
        "wikipedia reparse-taxoboxes --dry-run",
        "wikipedia reparse-taxoboxes --dry-run --report reparse.tsv",
        "wikipedia reparse-taxoboxes",
    })]
internal sealed class WikipediaReparseTaxoboxesCommand : Command<WikipediaReparseTaxoboxesCommand.Settings> {
    private const int BatchSize = 500;

    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <FILE>")]
        [Description("Path to the Wikipedia cache SQLite database. Defaults to Datastore:enwiki_cache_sqlite.")]
        public string? CachePath { get; init; }

        [CommandOption("--dry-run")]
        [Description("Count the pages whose taxobox fields would change, and save nothing.")]
        public bool DryRun { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Read only the first N downloaded pages, for a test run.")]
        public int? Limit { get; init; }

        [CommandOption("--report <FILE>")]
        [Description("Write every page whose taxobox fields change to this file, one tab-separated line per page: title, what changed, and the common name, name field and scientific name before and after.")]
        public string? ReportPath { get; init; }
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();

        string cachePath;
        try {
            cachePath = paths.ResolveWikipediaCachePath(settings.CachePath);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]{Markup.Escape(ex.Message)}[/]");
            return -1;
        }
        if (!File.Exists(cachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Wikipedia cache not found:[/] {Markup.Escape(cachePath)}");
            return -1;
        }

        using var store = settings.DryRun ? WikipediaCacheStore.OpenReadOnly(cachePath) : WikipediaCacheStore.Open(cachePath);
        if (store is null) {
            AnsiConsole.MarkupLineInterpolated($"[red]Could not open the Wikipedia cache:[/] {Markup.Escape(cachePath)}");
            return -1;
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]Wikipedia cache:[/] {Markup.Escape(cachePath)}");

        StreamWriter? report = null;
        if (!string.IsNullOrWhiteSpace(settings.ReportPath)) {
            var reportPath = Path.GetFullPath(settings.ReportPath);
            Directory.CreateDirectory(Path.GetDirectoryName(reportPath)!);
            report = new StreamWriter(reportPath);
            report.WriteLine("title\tresult\tcolumns\tparameters\tcommon_name_before\tcommon_name_after\tname_field_before\tname_field_after\tscientific_name_before\tscientific_name_after");
        }

        var tally = new TaxoboxReparseTally();
        long saved = 0;
        try {
            var downloaded = store.CountDownloadedPages();
            var total = settings.Limit is > 0 ? Math.Min(downloaded, settings.Limit.Value) : downloaded;
            ProgressConsole.Run("Parsing taxoboxes", total, progress => {
                long after = 0;
                long seen = 0;
                while (seen < total) {
                    cancellationToken.ThrowIfCancellationRequested();
                    var pages = store.ReadDownloadedPages(after, (int)Math.Min(BatchSize, total - seen));
                    if (pages.Count == 0) {
                        break;
                    }

                    var upserts = new List<WikiTaxoboxData>();
                    var deletes = new List<long>();
                    foreach (var page in pages) {
                        after = page.PageRowId;
                        seen++;
                        progress.Increment(1);
                        if (page.Wikitext is null) {
                            continue;
                        }

                        var parsed = TaxoboxParser.TryParse(page.PageRowId, page.Wikitext);
                        var result = TaxoboxReparse.Compare(page.Taxobox, parsed);
                        tally.Add(page.PageTitle, result, page.Taxobox, parsed);
                        switch (result.Outcome) {
                            case TaxoboxReparseOutcome.Changed or TaxoboxReparseOutcome.Added:
                                upserts.Add(parsed!);
                                break;
                            case TaxoboxReparseOutcome.Removed:
                                deletes.Add(page.PageRowId);
                                break;
                            default:
                                continue;
                        }
                        report?.WriteLine(ReportLine(page.PageTitle, result, page.Taxobox, parsed));
                    }

                    if (!settings.DryRun) {
                        store.SaveTaxoboxChanges(upserts, deletes);
                        saved += upserts.Count + deletes.Count;
                    }
                }
            });
        }
        finally {
            report?.Dispose();
        }

        WriteSummary(tally);
        if (report is not null) {
            AnsiConsole.MarkupLineInterpolated($"[grey]Every change written to[/] {Markup.Escape(Path.GetFullPath(settings.ReportPath!))}");
        }

        if (tally.ToSave == 0) {
            AnsiConsole.MarkupLineInterpolated($"[green]Nothing to save:[/] the saved taxobox fields of all {tally.PagesRead:N0} pages are what the parser reads now.");
        } else if (settings.DryRun) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Dry run: nothing saved.[/] Run again without --dry-run to save the taxobox fields of {tally.ToSave:N0} pages.");
        } else {
            AnsiConsole.MarkupLineInterpolated($"[green]Saved the taxobox fields of {saved:N0} pages.[/]");
            AnsiConsole.MarkupLine("[grey]Run common-names aggregate --source wikipedia --replace to read the common names again from the new fields.[/]");
        }
        return 0;
    }

    private static void WriteSummary(TaxoboxReparseTally tally) {
        var table = new Table().AddColumn("Downloaded pages with wikitext").AddColumn(new TableColumn("Pages").RightAligned());
        table.AddRow("All", $"{tally.PagesRead:N0}");
        table.AddRow("Taxobox fields unchanged", $"{tally.Unchanged:N0}");
        table.AddRow("Taxobox fields changed", $"{tally.Changed:N0}");
        table.AddRow("  name field changed", $"{tally.NameChanged:N0}");
        table.AddRow("  scientific name changed", $"{tally.ScientificNameChanged:N0}");
        table.AddRow("  rank or classification changed", $"{tally.OtherColumnsChanged:N0}");
        table.AddRow("Taxobox found, none saved before", $"{tally.Added:N0}");
        table.AddRow("No taxobox found, one saved before", $"{tally.Removed:N0}");
        table.AddRow("Common name read from the name field changed", $"{tally.CommonNameChanged:N0}");
        AnsiConsole.Write(table);

        if (tally.CommonNameExamples.Count == 0) {
            return;
        }
        AnsiConsole.MarkupLineInterpolated($"First {tally.CommonNameExamples.Count} common name changes:");
        foreach (var (title, before, after) in tally.CommonNameExamples) {
            AnsiConsole.MarkupLineInterpolated($"  {Markup.Escape(title)}: [grey]{Markup.Escape(before ?? "(none)")}[/] → {Markup.Escape(after ?? "(none)")}");
        }
    }

    private static string ReportLine(string title, TaxoboxReparseResult result, WikiTaxoboxData? stored, WikiTaxoboxData? parsed) =>
        string.Join('\t', new[] {
            title,
            result.Outcome.ToString(),
            string.Join(',', result.Columns),
            string.Join(',', result.Parameters),
            TaxoboxReparseTally.CommonName(stored, title) ?? string.Empty,
            TaxoboxReparseTally.CommonName(parsed, title) ?? string.Empty,
            TaxoboxReparse.Parameter(stored?.DataJson, "name") ?? string.Empty,
            TaxoboxReparse.Parameter(parsed?.DataJson, "name") ?? string.Empty,
            stored?.ScientificName ?? string.Empty,
            parsed?.ScientificName ?? string.Empty,
        }.Select(Tsv));

    private static string Tsv(string value) => value.Replace('\t', ' ').Replace('\n', ' ').Replace('\r', ' ');
}
