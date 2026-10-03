using System.ComponentModel;
using System.Globalization;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn.Gbif;
using BeastieBot3.SiteBuild;
using Spectre.Console;
using Spectre.Console.Cli;

// `iucn resolve-dois`: finds the DOIs of assessments that IUCN's citation text, GBIF's checklist
// and Wikidata give none for, and saves them in the DOI cache (IucnDoiCacheStore).
//
//   1. IucnDoiScopeReader lists the assessments in the scope with no DOI from those sources.
//   2. Crossref's list of every DOI under IUCN's prefix (CrossrefIucnWorks, about 260 requests) is
//      downloaded when the cache has none from the last 7 days. An assessment whose DOI is in it is
//      settled with no further request.
//   3. For the rest, likely DOIs (IucnDoiCandidates) are checked one by one at doi.org's handle API
//      until one exists (IucnDoiResolution).
//
// Each assessment's result is saved as soon as it is known, so a stopped run loses nothing and the
// next run starts where it stopped. Assessments already in doi_check are skipped unless --recheck
// or --recheck-missing-after is given.

namespace BeastieBot3.Iucn.Doi;

[CommandInfo("iucn resolve-dois", CommandKind.Mutates,
    "Find the DOIs of IUCN Red List assessments that have no DOI in IUCN's citation, GBIF's checklist or Wikidata, and save them in Datastore:IUCN_doi_cache_sqlite. Looks each assessment up in Crossref's list of IUCN DOIs, then checks likely DOIs at doi.org.",
    Rerun = RerunEffect.IdempotentAdd,
    RerunNote = "Skips assessments already checked. --recheck checks every assessment in the scope again, and --recheck-missing-after <DAYS> checks again the assessments with no DOI found at least that many days ago.",
    ReportOnlyWith = new[] { "--status" },
    Examples = new[] {
        "iucn resolve-dois --status",
        "iucn resolve-dois --limit 50",
        "iucn resolve-dois",
        "iucn resolve-dois --scope latest-regional",
        "iucn resolve-dois --scope history --no-doi-org",
    })]
internal sealed class IucnResolveDoisCommand : AsyncCommand<IucnResolveDoisCommand.Settings> {
    public const string UserAgent = "BeastieBot3/1.0 (+https://en.wikipedia.org/wiki/User:Beastie_Bot)";
    private static readonly TimeSpan CrossrefMaxAge = TimeSpan.FromDays(7);
    private static readonly TimeSpan CrossrefInterval = TimeSpan.FromMilliseconds(250);
    private const int MaxUnexpectedInARow = 10;

    public sealed class Settings : CommonSettings {
        [CommandOption("--scope <SCOPE>")]
        [Description("Which assessments to check: latest-global (default; includes subspecies, varieties and subpopulations), latest-regional, all-latest, or history (assessments that are no longer the latest).")]
        public string? Scope { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Check at most this many assessments in this run.")]
        public int? Limit { get; init; }

        [CommandOption("--recheck")]
        [Description("Check every assessment in the scope again, including those already checked.")]
        public bool Recheck { get; init; }

        [CommandOption("--recheck-missing-after <DAYS>")]
        [Description("Check again the assessments with no DOI found by a check at least this many days ago.")]
        public int? RecheckMissingAfterDays { get; init; }

        [CommandOption("--delay <MS>")]
        [Description("Time between the starts of two doi.org requests, in milliseconds. Default: 300.")]
        [DefaultValue(300)]
        public int DelayMs { get; init; } = 300;

        [CommandOption("--no-crossref")]
        [Description("Skip Crossref's list of IUCN DOIs and check every assessment at doi.org.")]
        public bool NoCrossref { get; init; }

        [CommandOption("--refresh-crossref")]
        [Description("Download Crossref's list of IUCN DOIs again, even when the cache has a copy from the last 7 days.")]
        public bool RefreshCrossref { get; init; }

        [CommandOption("--no-doi-org")]
        [Description("Use only Crossref's list of IUCN DOIs. Assessments not in the list stay unchecked, so a later run can check them at doi.org.")]
        public bool NoDoiOrg { get; init; }

        [CommandOption("--status")]
        [Description("Show how many assessments in the scope have a DOI, and from which source, then stop. Sends no requests and saves nothing.")]
        public bool Status { get; init; }

        [CommandOption("--database <PATH>")]
        [Description("DOI cache to write. Default: Datastore:IUCN_doi_cache_sqlite in paths.ini.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--previous-iucn-db <PATH>")]
        [Description("CSV export database of the previous Red List release. Default: the IUCN_<release>.sqlite beside the current one with the newest older release.")]
        public string? PreviousIucnDatabase { get; init; }

        public override ValidationResult Validate() {
            if (IucnDoiScopeReader.ParseScope(Scope) is null) {
                return ValidationResult.Error("--scope must be latest-global, latest-regional, all-latest or history.");
            }
            if (Limit is <= 0) {
                return ValidationResult.Error("--limit must be 1 or more.");
            }
            if (RecheckMissingAfterDays is < 0) {
                return ValidationResult.Error("--recheck-missing-after must be 0 or more days.");
            }
            if (DelayMs < 0) {
                return ValidationResult.Error("--delay must be 0 or more milliseconds.");
            }
            if (NoCrossref && NoDoiOrg) {
                return ValidationResult.Error("--no-crossref and --no-doi-org together leave nothing to check with.");
            }
            return ValidationResult.Success();
        }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var paths = settings.CreatePaths();
        var scope = IucnDoiScopeReader.ParseScope(settings.Scope)!.Value;
        var scopeName = IucnDoiScopeReader.ScopeName(scope);

        var cachePath = settings.DatabasePath ?? paths.GetIucnDoiCachePath();
        var iucnDatabase = paths.GetIucnDatabasePath();
        var apiCache = paths.GetIucnApiCachePath();
        if (string.IsNullOrWhiteSpace(cachePath) || string.IsNullOrWhiteSpace(iucnDatabase) || string.IsNullOrWhiteSpace(apiCache)) {
            AnsiConsole.MarkupLine("[red]Missing paths:[/] set Datastore:IUCN_sqlite_from_cvs, Datastore:IUCN_api_cache_sqlite and Datastore:IUCN_doi_cache_sqlite (or datastore_dir) in paths.ini.");
            return -1;
        }
        foreach (var required in new[] { iucnDatabase, apiCache }) {
            if (!File.Exists(required)) {
                AnsiConsole.MarkupLineInterpolated($"[red]File not found:[/] {required}");
                return -1;
            }
        }

        string release;
        using (var csv = SiteIucnCsvReader.OpenReadOnly(iucnDatabase)) {
            release = SiteIucnCsvReader.ReadRelease(csv);
        }
        var previous = settings.PreviousIucnDatabase ?? IucnDoiScopeReader.FindPreviousRelease(iucnDatabase, release);
        if (settings.PreviousIucnDatabase is { } given && !File.Exists(given)) {
            AnsiConsole.MarkupLineInterpolated($"[red]File not found:[/] {given}");
            return -1;
        }
        var sources = new DoiScopeSources(
            iucnDatabase,
            previous,
            apiCache,
            GbifIucnChecklistReader.FindNewest(paths.GetGbifIucnDir()),
            paths.GetWikidataCachePath());

        AnsiConsole.MarkupLineInterpolated($"[grey]Finding assessments in scope {scopeName} with no DOI from IUCN's citation, GBIF or Wikidata...[/]");
        var found = await Task.Run(() => IucnDoiScopeReader.Read(sources, scope,
            line => AnsiConsole.MarkupLineInterpolated($"[grey]  {line}[/]"), cancellationToken), cancellationToken).ConfigureAwait(false);
        foreach (var source in found.Skipped) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Skipped {source}: no file is configured or the file does not exist. Its DOIs are not counted.[/]");
        }
        if (previous is null) {
            AnsiConsole.MarkupLine("[yellow]No CSV export of the previous release was found, so no assessment is known to be new in this release.[/]");
        }

        using var store = IucnDoiCacheStore.Open(cachePath);
        var checks = store.ReadChecks();
        var now = DateTime.UtcNow;
        var plan = DoiRunPlan.Make(found.Targets, checks, settings.Recheck,
            settings.RecheckMissingAfterDays is { } days ? TimeSpan.FromDays(days) : null, now);

        var listing = store.LastCompletedListing();
        if (settings.Status) {
            WriteStatus(scopeName, found, plan, listing, store.CountCrossrefWorks());
            return 0;
        }

        var toCheck = settings.Limit is { } limit ? plan.ToCheck.Take(limit).ToList() : plan.ToCheck;
        var summary = new DoiRunSummary();
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(UserAgent);
        http.DefaultRequestHeaders.Accept.ParseAdd("application/json");
        void Warn(string message) => AnsiConsole.MarkupLineInterpolated($"[yellow]{message}[/]");

        try {
            if (!settings.NoCrossref && toCheck.Count > 0) {
                if (settings.RefreshCrossref || listing?.CompletedAtUtc is not { } completed || now - completed > CrossrefMaxAge) {
                    await DownloadCrossrefAsync(store, http, Warn, cancellationToken).ConfigureAwait(false);
                    listing = store.LastCompletedListing();
                } else {
                    AnsiConsole.MarkupLineInterpolated($"[grey]Using Crossref's list of IUCN DOIs downloaded {completed:yyyy-MM-dd HH:mm} UTC ({store.CountCrossrefWorks():N0} assessment DOIs).[/]");
                }
            }

            var doiOrg = new PoliteHttpGetter(http, TimeSpan.FromMilliseconds(settings.DelayMs)) { OnRetry = Warn };
            var lookup = new DoiHandleClient(doiOrg);
            await CheckAsync(store, toCheck, settings, lookup, summary, cancellationToken).ConfigureAwait(false);
            summary.DoiOrgRequests = doiOrg.Requests;
            summary.RateLimited = doiOrg.RateLimited;
        } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
            AnsiConsole.MarkupLine("[yellow]Stopped. Every result found so far is saved; run the command again to continue.[/]");
            WriteSummary(scopeName, summary, plan);
            return 1;
        } catch (PoliteHttpException ex) {
            AnsiConsole.MarkupLineInterpolated($"[red]Stopped: {ex.Message}[/] ({ex.Url})");
            AnsiConsole.MarkupLine("Every result found so far is saved; run the command again to continue.");
            WriteSummary(scopeName, summary, plan);
            return 2;
        }
        WriteSummary(scopeName, summary, plan);
        return summary.StoppedByErrors ? 2 : 0;
    }

    private static async Task DownloadCrossrefAsync(IucnDoiCacheStore store, HttpClient http, Action<string> warn, CancellationToken cancellationToken) {
        var getter = new PoliteHttpGetter(http, CrossrefInterval) { OnRetry = warn, RateLimitWait = TimeSpan.FromSeconds(10) };
        long works = 0;
        var unread = new List<string>();
        await ProgressConsole.RunAsync("Downloading Crossref's list of IUCN DOIs", 0, async progress => {
            await CrossrefIucnWorks.DownloadAsync(getter, store, page => {
                if (page.TotalResults is { } total) {
                    progress.Total = total;
                }
                works += page.Works.Count;
                unread.AddRange(page.UnreadRlts);
                progress.Increment(page.ItemCount);
            }, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
        AnsiConsole.MarkupLineInterpolated($"Downloaded Crossref's list of IUCN DOIs: {works:N0} assessment DOIs in {getter.Requests:N0} requests.");
        if (unread.Count > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{unread.Count:N0} DOIs with \".RLTS.\" in them do not have the usual form and were left out, for example {string.Join(", ", unread.Take(3))}.[/]");
        }
    }

    private static async Task CheckAsync(IucnDoiCacheStore store, IReadOnlyList<DoiTarget> toCheck, Settings settings,
        IDoiHandleLookup lookup, DoiRunSummary summary, CancellationToken cancellationToken) {
        if (toCheck.Count == 0) {
            return;
        }
        var unexpectedInARow = 0;
        await ProgressConsole.RunAsync("Checking assessments", toCheck.Count, async progress => {
            foreach (var target in toCheck) {
                cancellationToken.ThrowIfCancellationRequested();
                var year = summary.Year(target.YearPublished);
                year.Checked++;
                summary.Checked++;

                if (!settings.NoCrossref) {
                    var choice = IucnDoiResolution.ChooseFromCrossref(target, store.CrossrefWorksFor(target.AssessmentId));
                    if (choice.Doi is { } crossrefDoi) {
                        store.SaveCheck(new DoiCheckRow(target.AssessmentId, target.TaxonId, crossrefDoi, DateTime.UtcNow, 0),
                            DoiFoundBy.Crossref, target.Scope, target.YearPublished, choice.Note, Array.Empty<DoiLookupLogRow>());
                        year.FoundCrossref++;
                        summary.FoundCrossref++;
                        Progress(progress, summary);
                        continue;
                    }
                    if (settings.NoDoiOrg) {
                        year.NotInCrossref++;
                        summary.NotInCrossref++;
                        Progress(progress, summary);
                        continue;
                    }
                    summary.CrossrefNotes += choice.Note is null ? 0 : 1;
                    var probe = await ProbeAndSaveAsync(store, target, lookup, choice.Note, year, summary, cancellationToken).ConfigureAwait(false);
                    unexpectedInARow = probe ? 0 : unexpectedInARow + 1;
                } else {
                    var probe = await ProbeAndSaveAsync(store, target, lookup, null, year, summary, cancellationToken).ConfigureAwait(false);
                    unexpectedInARow = probe ? 0 : unexpectedInARow + 1;
                }
                Progress(progress, summary);
                if (unexpectedInARow >= MaxUnexpectedInARow) {
                    summary.StoppedByErrors = true;
                    AnsiConsole.MarkupLineInterpolated($"[red]Stopped: doi.org gave unexpected answers for {MaxUnexpectedInARow} assessments in a row.[/] The lookups are in the doi_lookup_log table of the DOI cache.");
                    return;
                }
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    // Checks candidates at doi.org and saves the result. False when doi.org gave an unexpected
    // answer, in which case nothing is saved for the assessment except its lookups.
    private static async Task<bool> ProbeAndSaveAsync(IucnDoiCacheStore store, DoiTarget target, IDoiHandleLookup lookup, string? crossrefNote,
        DoiYearCounts year, DoiRunSummary summary, CancellationToken cancellationToken) {
        var candidates = IucnDoiCandidates.For(target.CandidateRequest());
        if (candidates.Count == 0) {
            store.SaveCheck(new DoiCheckRow(target.AssessmentId, target.TaxonId, null, DateTime.UtcNow, 0),
                null, target.Scope, target.YearPublished, "No year published, so no DOI to check.", Array.Empty<DoiLookupLogRow>());
            year.NotFound++;
            summary.NotFound++;
            return true;
        }
        var result = await IucnDoiResolution.ProbeAsync(target, candidates, lookup, () => DateTime.UtcNow, cancellationToken).ConfigureAwait(false);
        year.Lookups += result.Tried;
        if (!result.Complete) {
            store.LogLookups(result.Lookups);
            year.Errors++;
            summary.Errors++;
            return false;
        }
        var note = string.Join(" ", new[] { crossrefNote, result.Note }.Where(n => n is not null));
        store.SaveCheck(new DoiCheckRow(target.AssessmentId, target.TaxonId, result.Doi, DateTime.UtcNow, result.Tried),
            result.Doi is null ? null : DoiFoundBy.DoiOrg, target.Scope, target.YearPublished, note.Length == 0 ? null : note, result.Lookups);
        if (result.Doi is null) {
            year.NotFound++;
            summary.NotFound++;
        } else {
            year.FoundDoiOrg++;
            summary.FoundDoiOrg++;
        }
        return true;
    }

    private static void Progress(IProgressHandle progress, DoiRunSummary summary) {
        progress.Increment();
        progress.Description = $"Checking assessments: {summary.FoundCrossref + summary.FoundDoiOrg:N0} DOIs found, {summary.NotFound:N0} not found";
    }

    // ------------------------------------------------------------ output

    private static void WriteStatus(string scopeName, DoiScopeResult found, DoiRunPlan plan, CrossrefListing? listing, long crossrefWorks) {
        var counts = found.Counts;
        var table = new Table().Title($"DOIs for scope {scopeName}, release {found.Release}").AddColumn("Assessments").AddColumn(new TableColumn("Count").RightAligned());
        table.AddRow("In scope", $"{counts.InScope:N0}");
        table.AddRow("DOI from GBIF's checklist", $"{counts.FromGbif:N0}");
        table.AddRow("DOI from Wikidata", $"{counts.FromWikidata:N0}");
        table.AddRow("DOI in IUCN's citation", $"{counts.FromCitation:N0}");
        table.AddRow("No DOI from these sources", $"{counts.Targets:N0}");
        table.AddRow("  Checked, DOI found", $"{plan.CheckedFound:N0}");
        table.AddRow("  Checked, no DOI found", $"{plan.CheckedNotFound:N0}");
        table.AddRow("  To check", $"{plan.ToCheck.Count:N0}");
        if (counts.NoPayload > 0) {
            table.AddRow("  of which with no cached API payload", $"{counts.NoPayload:N0}");
        }
        table.Caption("Each assessment is counted once, under the first source with a DOI for it: GBIF's checklist, then Wikidata, then IUCN's citation.");
        AnsiConsole.Write(table);
        if (listing?.CompletedAtUtc is { } completed) {
            AnsiConsole.MarkupLineInterpolated($"Crossref's list of IUCN DOIs: downloaded {completed:yyyy-MM-dd HH:mm} UTC, {crossrefWorks:N0} assessment DOIs.");
        } else {
            AnsiConsole.MarkupLine("Crossref's list of IUCN DOIs: not downloaded yet. The next run downloads it (about 260 requests).");
        }
    }

    private static void WriteSummary(string scopeName, DoiRunSummary summary, DoiRunPlan plan) {
        var table = new Table().Title($"DOI checks this run, scope {scopeName}")
            .AddColumn("Year published")
            .AddColumn(new TableColumn("Checked").RightAligned())
            .AddColumn(new TableColumn("Found in Crossref's list").RightAligned())
            .AddColumn(new TableColumn("Found at doi.org").RightAligned())
            .AddColumn(new TableColumn("Not found").RightAligned())
            .AddColumn(new TableColumn("doi.org lookups").RightAligned());
        if (summary.NotInCrossref > 0) {
            table.AddColumn(new TableColumn("Not in Crossref's list, not checked").RightAligned());
        }
        if (summary.Errors > 0) {
            table.AddColumn(new TableColumn("Unexpected answers").RightAligned());
        }
        void Row(string label, DoiYearCounts c) {
            var cells = new List<string> {
                label, $"{c.Checked:N0}", $"{c.FoundCrossref:N0}", $"{c.FoundDoiOrg:N0}", $"{c.NotFound:N0}", $"{c.Lookups:N0}",
            };
            if (summary.NotInCrossref > 0) cells.Add($"{c.NotInCrossref:N0}");
            if (summary.Errors > 0) cells.Add($"{c.Errors:N0}");
            table.AddRow(cells.ToArray());
        }
        foreach (var (year, counts) in summary.ByYear.OrderBy(p => p.Key == DoiRunSummary.UnknownYear ? int.MaxValue : p.Key)) {
            Row(year == DoiRunSummary.UnknownYear ? "unknown" : year.ToString(CultureInfo.InvariantCulture), counts);
        }
        Row("Total", summary.Totals());
        AnsiConsole.Write(table);

        var left = plan.ToCheck.Count - summary.Checked + summary.Errors + summary.NotInCrossref;
        AnsiConsole.MarkupLineInterpolated($"Requests to doi.org: {summary.DoiOrgRequests:N0} ({(summary.Checked == 0 ? 0 : (double)summary.DoiOrgRequests / summary.Checked):0.##} per assessment checked). HTTP 429 answers: {summary.RateLimited:N0}.");
        AnsiConsole.MarkupLineInterpolated($"Left to check in scope {scopeName}: {left:N0} of {plan.ToCheck.Count:N0}.");
    }
}

/// Which targets a run checks, and how many were settled by earlier runs.
internal sealed record DoiRunPlan(IReadOnlyList<DoiTarget> ToCheck, int CheckedFound, int CheckedNotFound) {
    /// Pure: a target is checked when it has no doi_check row, or with recheck, or when its row
    /// found no DOI and is at least recheckMissingAfter old.
    public static DoiRunPlan Make(IReadOnlyList<DoiTarget> targets, IReadOnlyDictionary<long, DoiCheckRow> checks, bool recheck,
        TimeSpan? recheckMissingAfter, DateTime nowUtc) {
        var toCheck = new List<DoiTarget>();
        int found = 0, notFound = 0;
        foreach (var target in targets) {
            if (!checks.TryGetValue(target.AssessmentId, out var row)) {
                toCheck.Add(target);
                continue;
            }
            if (row.Doi is null) notFound++; else found++;
            if (recheck || (row.Doi is null && recheckMissingAfter is { } age && nowUtc - row.CheckedAtUtc >= age)) {
                toCheck.Add(target);
            }
        }
        return new DoiRunPlan(toCheck, found, notFound);
    }
}

internal sealed class DoiYearCounts {
    public int Checked { get; set; }
    public int FoundCrossref { get; set; }
    public int FoundDoiOrg { get; set; }
    public int NotFound { get; set; }
    public int NotInCrossref { get; set; }
    public int Errors { get; set; }
    public int Lookups { get; set; }
}

internal sealed class DoiRunSummary {
    /// The ByYear key for an assessment whose year published is not known.
    public const int UnknownYear = 0;

    public Dictionary<int, DoiYearCounts> ByYear { get; } = new();
    public int Checked { get; set; }
    public int FoundCrossref { get; set; }
    public int FoundDoiOrg { get; set; }
    public int NotFound { get; set; }
    public int NotInCrossref { get; set; }
    public int Errors { get; set; }
    public int CrossrefNotes { get; set; }
    public int DoiOrgRequests { get; set; }
    public int RateLimited { get; set; }
    public bool StoppedByErrors { get; set; }

    public DoiYearCounts Year(int? year) {
        var key = year ?? UnknownYear;
        if (!ByYear.TryGetValue(key, out var counts)) {
            ByYear[key] = counts = new DoiYearCounts();
        }
        return counts;
    }

    public DoiYearCounts Totals() => new() {
        Checked = ByYear.Values.Sum(c => c.Checked),
        FoundCrossref = ByYear.Values.Sum(c => c.FoundCrossref),
        FoundDoiOrg = ByYear.Values.Sum(c => c.FoundDoiOrg),
        NotFound = ByYear.Values.Sum(c => c.NotFound),
        NotInCrossref = ByYear.Values.Sum(c => c.NotInCrossref),
        Errors = ByYear.Values.Sum(c => c.Errors),
        Lookups = ByYear.Values.Sum(c => c.Lookups),
    };
}
