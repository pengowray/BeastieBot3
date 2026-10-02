using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;
using Spectre.Console.Cli;
using BeastieBot3.Configuration;
using BeastieBot3.Sprat;
using BeastieBot3.WikipediaLists;

// Posts the generated list files (wikipedia generate-lists, sprat generate-lists) to English
// Wikipedia as user-space drafts, one page per list under --base, plus an index page at --base.
//
// Which files are posted comes from the list definitions (rules/wikipedia-lists.yml and
// SpratListGroups), not from a folder listing, so the archive folder and files left behind by a
// renamed list are never posted. A page whose text already matches is skipped by comparing sha1s,
// so a run after an interruption, or after regenerating only some lists, saves only what changed.
//
// Credentials come from WIKIPEDIA_BOT_USERNAME / WIKIPEDIA_BOT_PASSWORD (environment or .env), a
// bot password from Special:BotPasswords. Without --apply nothing logs in and nothing is saved.

namespace BeastieBot3.Wikipedia;

[CommandInfo("wikipedia post-drafts", CommandKind.Mutates,
    "Post the generated IUCN Red List and Australian lists to English Wikipedia as draft pages in user space, with an index page that links them all. Each draft goes to <base>/<article title>, for example \"User:Beastie Bot/Draft 2026/List of critically endangered mammals\". Run wikipedia generate-lists and sprat generate-lists first. An old draft that no list produces any more is moved to the list's new title when only capital letters changed, and blanked otherwise. Without --apply, the command only reads Wikipedia and shows which drafts it would create, update, move or blank.",
    Rerun = RerunEffect.Publishes,
    RerunNote = "Without --apply, the command saves nothing. With --apply, it saves only the drafts whose text differs from the page on Wikipedia, so a second run after an interrupted one posts only the drafts that are still missing or out of date. An old draft that was moved or blanked is not touched again.",
    ChangesOnlyWith = new[] { "--apply" },
    Examples = new[] {
        "wikipedia post-drafts",
        "wikipedia post-drafts --apply --title \"critically endangered mammals\"",
        "wikipedia post-drafts --apply",
    })]
internal sealed class WikipediaPostDraftsCommand : AsyncCommand<WikipediaPostDraftsCommand.Settings> {
    private const string DefaultBase = "User:Beastie Bot/Draft 2026";

    public sealed class Settings : CommonSettings {
        [CommandOption("--apply")]
        [Description("Log in and save the pages. Without it the command only reports what it would save.")]
        public bool Apply { get; init; }

        [CommandOption("--base <TITLE>")]
        [Description("Title of the index page. Each draft is posted at <TITLE>/<article title>. Default: \"User:Beastie Bot/Draft 2026\".")]
        public string? BaseTitle { get; init; }

        [CommandOption("--title <TEXT>")]
        [Description("Post only the drafts whose article title contains this text (not case-sensitive). The index page still lists every draft that is on Wikipedia.")]
        public string? TitleFilter { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Save at most N draft pages in this run (the index page is not counted).")]
        public int? Limit { get; init; }

        [CommandOption("--write-dir <DIR>")]
        [Description("Also write the text of every draft page and the index page to this folder, one .wikitext file per page, for checking before posting.")]
        public string? WriteDirectory { get; init; }

        [CommandOption("--delay <SECONDS>")]
        [Description("Seconds to wait between saves. Default: 10.")]
        public int? DelaySeconds { get; init; }
    }

    private sealed record Draft(DraftGroup Group, string ArticleTitle, string DraftTitle, string Text, int Bytes, string Sha1, DateTime Generated, string SourceFile) {
        public bool TooLarge => Bytes > DraftPageBuilder.MaxPageBytes;
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var baseTitle = string.IsNullOrWhiteSpace(settings.BaseTitle) ? DefaultBase : settings.BaseTitle.Trim().Replace('_', ' ');
        var delay = TimeSpan.FromSeconds(Math.Max(0, settings.DelaySeconds ?? 10));

        var drafts = LoadDrafts(paths, baseTitle);
        if (drafts.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No generated list files found.[/] Run [white]wikipedia generate-lists[/] and [white]sprat generate-lists[/] first.");
            return 1;
        }

        var configuration = WikipediaConfiguration.FromEnvironment();
        using var client = new WikipediaEditClient(configuration);

        var allTitles = drafts.Select(d => d.DraftTitle).Append(baseTitle).ToList();
        AnsiConsole.MarkupLineInterpolated($"[grey]Checking {allTitles.Count} pages on {configuration.ActionEndpoint.Host}...[/]");
        var onWiki = await client.GetLatestRevisionsAsync(allTitles, cancellationToken).ConfigureAwait(false);

        var selected = drafts
            .Where(d => settings.TitleFilter is null || d.ArticleTitle.Contains(settings.TitleFilter, StringComparison.OrdinalIgnoreCase))
            .ToList();
        var tooLarge = selected.Where(d => d.TooLarge).ToList();
        var toSave = selected
            .Where(d => !d.TooLarge && !(onWiki.TryGetValue(d.DraftTitle, out var page) && page.Sha1 == d.Sha1))
            .ToList();
        var unchanged = selected.Count - tooLarge.Count - toSave.Count;
        if (settings.Limit is int limit && toSave.Count > limit) {
            toSave = toSave.Take(Math.Max(0, limit)).ToList();
        }

        // Old drafts: pages below the base title that no current list produces (a renamed list).
        IReadOnlyList<OldDraftStep> oldSteps = Array.Empty<OldDraftStep>();
        if (settings.TitleFilter is null) {
            oldSteps = await PlanOldDraftsAsync(client, drafts, onWiki, baseTitle, cancellationToken).ConfigureAwait(false);
        }

        PrintPlan(selected.Count, toSave, onWiki, unchanged, tooLarge, settings, oldSteps);

        if (!string.IsNullOrWhiteSpace(settings.WriteDirectory)) {
            WritePages(settings.WriteDirectory, selected, BuildIndexText(drafts, baseTitle, d => toSave.Contains(d) || (onWiki.TryGetValue(d.DraftTitle, out var page) && page.Exists)));
        }

        if (!settings.Apply) {
            AnsiConsole.MarkupLine("[grey]Nothing was saved. Add[/] [white]--apply[/] [grey]to log in and save these pages.[/]");
            return 0;
        }

        EnvFileLoader.LoadIfPresent();
        var username = Environment.GetEnvironmentVariable("WIKIPEDIA_BOT_USERNAME")?.Trim();
        var password = Environment.GetEnvironmentVariable("WIKIPEDIA_BOT_PASSWORD")?.Trim();
        if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password)) {
            AnsiConsole.MarkupLine("[red]Missing Wikipedia login.[/] Set WIKIPEDIA_BOT_USERNAME and WIKIPEDIA_BOT_PASSWORD in the environment or in .env.");
            AnsiConsole.MarkupLine("[grey]Create a bot password at https://en.wikipedia.org/wiki/Special:BotPasswords with the grants \"Edit existing pages\" and \"Create, edit, and move pages\". The username has the form Account@BotName.[/]");
            return 1;
        }

        var loginError = await client.LoginAsync(username, password, cancellationToken).ConfigureAwait(false);
        if (loginError is not null) {
            AnsiConsole.MarkupLineInterpolated($"[red]Login failed:[/] {loginError}");
            return 1;
        }
        AnsiConsole.MarkupLineInterpolated($"Logged in as [white]{client.LoggedInAs}[/].");

        var counts = new Dictionary<EditStatus, int>();
        var failures = new List<(string Title, string Message)>();
        var savedTitles = new HashSet<string>(StringComparer.Ordinal);

        // Moves go first: a move needs its new title free, and saving the new draft would take it.
        // A failed move falls back to blanking the old draft; the note says why.
        var toBlank = oldSteps.Where(s => s.Action == OldDraftAction.Blank).Select(s => (Title: s.Title, Note: (string?)null)).ToList();
        var moves = oldSteps.Where(s => s.Action == OldDraftAction.Move).ToList();
        for (var i = 0; i < moves.Count; i++) {
            cancellationToken.ThrowIfCancellationRequested();
            var move = moves[i];
            var moved = await client.MoveAsync(move.Title, move.NewTitle!, DraftPageBuilder.MoveReason, cancellationToken).ConfigureAwait(false);
            if (moved.Status == EditStatus.Failed) {
                toBlank.Add((move.Title, $"move to {move.NewTitle} failed: {moved.Message}"));
            } else {
                AnsiConsole.MarkupLineInterpolated($"[grey][[{i + 1}/{moves.Count}]][/] [green]moved[/] {move.Title} → {move.NewTitle}");
            }
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }

        for (var i = 0; i < toSave.Count; i++) {
            cancellationToken.ThrowIfCancellationRequested();
            var draft = toSave[i];
            var summary = DraftPageBuilder.DraftEditSummary(draft.ArticleTitle, baseTitle);
            var outcome = await client.EditAsync(draft.DraftTitle, draft.Text, summary, cancellationToken).ConfigureAwait(false);
            counts[outcome.Status] = counts.GetValueOrDefault(outcome.Status) + 1;
            PrintOutcome(i + 1, toSave.Count, draft, outcome);
            if (outcome.Status == EditStatus.Failed) {
                failures.Add((draft.DraftTitle, outcome.Message ?? "failed"));
            }
            else {
                savedTitles.Add(draft.DraftTitle);
            }
            if (outcome.Status != EditStatus.Unchanged && i < toSave.Count - 1) {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        var retiredText = DraftPageBuilder.RetiredDraftText(baseTitle);
        for (var i = 0; i < toBlank.Count; i++) {
            cancellationToken.ThrowIfCancellationRequested();
            var (title, note) = toBlank[i];
            var blanked = await client.EditAsync(title, retiredText, DraftPageBuilder.RetiredEditSummary(baseTitle), cancellationToken).ConfigureAwait(false);
            if (blanked.Status == EditStatus.Failed) {
                failures.Add((title, blanked.Message ?? "failed"));
                AnsiConsole.MarkupLineInterpolated($"[grey][[{i + 1}/{toBlank.Count}]][/] [red]failed[/] {title} ({blanked.Message}{(note is null ? "" : $"; {note}")})");
            } else if (note is null) {
                AnsiConsole.MarkupLineInterpolated($"[grey][[{i + 1}/{toBlank.Count}]][/] [green]blanked[/] {title}");
            } else {
                AnsiConsole.MarkupLineInterpolated($"[grey][[{i + 1}/{toBlank.Count}]][/] [green]blanked[/] {title} [yellow]({note})[/]");
            }
            if (i < toBlank.Count - 1) {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        // The index lists every draft that is on Wikipedia now: posted by this run or an earlier one.
        var index = BuildIndexText(drafts, baseTitle,
            d => savedTitles.Contains(d.DraftTitle) || (onWiki.TryGetValue(d.DraftTitle, out var page) && page.Exists));
        var indexText = index.Text;

        if (onWiki.TryGetValue(baseTitle, out var indexPage) && indexPage.Sha1 == DraftPageBuilder.Sha1Hex(indexText)) {
            AnsiConsole.MarkupLineInterpolated($"Index page unchanged: {baseTitle}");
        }
        else {
            var indexOutcome = await client.EditAsync(baseTitle, indexText, index.Summary, cancellationToken).ConfigureAwait(false);
            AnsiConsole.MarkupLineInterpolated($"Index page {DescribeStatus(indexOutcome.Status)}: {baseTitle}{(indexOutcome.Message is null ? "" : $" ({indexOutcome.Message})")}");
            if (indexOutcome.Status == EditStatus.Failed) {
                failures.Add((baseTitle, indexOutcome.Message ?? "failed"));
            }
        }

        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLineInterpolated($"Created {counts.GetValueOrDefault(EditStatus.Created)}, updated {counts.GetValueOrDefault(EditStatus.Updated)}, already up to date {unchanged + counts.GetValueOrDefault(EditStatus.Unchanged)}, failed {failures.Count}, too large to post {tooLarge.Count}.");
        foreach (var (title, message) in failures) {
            AnsiConsole.MarkupLineInterpolated($"[red]Failed:[/] {title}: {message}");
        }
        AnsiConsole.MarkupLineInterpolated($"Index: {configuration.ActionEndpoint.GetLeftPart(UriPartial.Authority)}/wiki/{Uri.EscapeDataString(baseTitle.Replace(' ', '_')).Replace("%2F", "/").Replace("%3A", ":")}");
        return failures.Count == 0 ? 0 : 1;
    }

    private static (string Text, string Summary) BuildIndexText(List<Draft> drafts, string baseTitle, Func<Draft, bool> isOnWiki) {
        var posted = drafts.Where(isOnWiki)
            .Select(d => new DraftIndexEntry(d.Group, d.ArticleTitle, d.DraftTitle, d.Bytes))
            .ToList();
        var oversize = drafts.Where(d => d.TooLarge)
            .Select(d => new DraftIndexEntry(d.Group, d.ArticleTitle, d.DraftTitle, d.Bytes))
            .ToList();
        var newest = drafts.Max(d => d.Generated);
        var iucnVersion = drafts.Where(d => d.Group == DraftGroup.Iucn)
            .Select(d => DraftPageBuilder.FindIucnVersion(File.ReadAllText(d.SourceFile)))
            .FirstOrDefault(v => v is not null);
        var text = DraftPageBuilder.BuildIndex(baseTitle, posted, oversize, newest, iucnVersion);
        var summary = DraftPageBuilder.IndexEditSummary(newest,
            posted.Count(e => e.Group == DraftGroup.Iucn), posted.Count(e => e.Group == DraftGroup.Australia), oversize.Count);
        return (text, summary);
    }

    // File names are the page titles with "/" and ":" replaced, so each page is one flat file.
    private static void WritePages(string dir, List<Draft> drafts, (string Text, string Summary) index) {
        var fullDir = Path.GetFullPath(dir);
        Directory.CreateDirectory(fullDir);
        static string FileName(string title) => title.Replace('/', '~').Replace(':', '~').Replace(' ', '_') + ".wikitext";
        foreach (var draft in drafts) {
            File.WriteAllText(Path.Combine(fullDir, FileName(draft.DraftTitle)), draft.Text);
        }
        File.WriteAllText(Path.Combine(fullDir, "_index.wikitext"), index.Text);
        AnsiConsole.MarkupLineInterpolated($"[grey]Wrote {drafts.Count} draft pages and the index page (as it would be after this run) to {fullDir}[/]");
    }

    private static List<Draft> LoadDrafts(PathsService paths, string baseTitle) {
        var drafts = new List<Draft>();
        var missing = new List<string>();

        var iucnDir = WikipediaListCommand.ResolveOutputDir(paths, null);
        var config = new WikipediaListDefinitionLoader().Load(WikipediaListCommand.ResolveConfigPath(paths, null));
        var iucnFiles = config.Lists.Select(d => d.OutputFile).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        foreach (var file in iucnFiles) {
            AddDraft(DraftGroup.Iucn, Path.Combine(iucnDir, file), Path.GetFileNameWithoutExtension(file).Replace('_', ' '));
        }

        var australiaDir = SpratGenerateListsCommand.ResolveOutputDir(paths, null);
        foreach (var group in SpratListGroups.All) {
            AddDraft(DraftGroup.Australia, Path.Combine(australiaDir, group.OutputFile), group.Title);
        }

        if (missing.Count > 0) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]{missing.Count} list files not generated yet, skipped:[/] {string.Join(", ", missing.Take(10))}{(missing.Count > 10 ? ", ..." : "")}");
        }
        ReportUnlisted(iucnDir, iucnFiles);
        ReportUnlisted(australiaDir, SpratListGroups.All.Select(g => g.OutputFile).ToList());
        return drafts;

        void AddDraft(DraftGroup group, string path, string articleTitle) {
            if (!File.Exists(path)) {
                missing.Add(Path.GetFileName(path));
                return;
            }
            var generated = File.GetLastWriteTime(path);
            var draftTitle = DraftPageBuilder.DraftTitle(baseTitle, articleTitle);
            var text = DraftPageBuilder.BuildDraft(File.ReadAllText(path), articleTitle, baseTitle, generated);
            drafts.Add(new Draft(group, articleTitle, draftTitle, text, DraftPageBuilder.ByteCount(text), DraftPageBuilder.Sha1Hex(text), generated, path));
        }
    }

    // A .wikitext file that no list definition names (a renamed or removed list) is not posted.
    private static void ReportUnlisted(string dir, IReadOnlyCollection<string> expected) {
        if (!Directory.Exists(dir)) {
            return;
        }
        var known = new HashSet<string>(expected, StringComparer.OrdinalIgnoreCase);
        var unlisted = Directory.GetFiles(dir, "*.wikitext", SearchOption.TopDirectoryOnly)
            .Select(Path.GetFileName)
            .Where(f => f is not null && !known.Contains(f))
            .ToList();
        if (unlisted.Count > 0) {
            AnsiConsole.MarkupLineInterpolated($"[grey]{unlisted.Count} files in {dir} are not in any list definition and are not posted: {string.Join(", ", unlisted.Take(5))}{(unlisted.Count > 5 ? ", ..." : "")}[/]");
        }
    }

    private static async Task<IReadOnlyList<OldDraftStep>> PlanOldDraftsAsync(
        WikipediaEditClient client,
        List<Draft> drafts,
        IReadOnlyDictionary<string, PageRevisionInfo> onWiki,
        string baseTitle,
        CancellationToken cancellationToken) {
        var subpages = await client.GetSubpagesAsync(baseTitle, cancellationToken).ConfigureAwait(false);
        var draftTitles = drafts.Select(d => d.DraftTitle).ToList();
        var current = new HashSet<string>(draftTitles, StringComparer.Ordinal);
        var candidates = subpages.Where(p => !p.IsRedirect && !current.Contains(p.Title)).Select(p => p.Title).ToList();
        var texts = candidates.Count == 0
            ? new Dictionary<string, string?>()
            : await client.GetTextsAsync(candidates, cancellationToken).ConfigureAwait(false);
        return OldDraftPlanner.Plan(subpages, texts, draftTitles,
            title => onWiki.TryGetValue(title, out var page) && page.Exists,
            DraftPageBuilder.RetiredDraftText(baseTitle));
    }

    private static void PrintPlan(int selectedCount, List<Draft> toSave, IReadOnlyDictionary<string, PageRevisionInfo> onWiki, int unchanged, List<Draft> tooLarge, Settings settings, IReadOnlyList<OldDraftStep> oldSteps) {
        var create = toSave.Count(d => !(onWiki.TryGetValue(d.DraftTitle, out var p) && p.Exists));
        var update = toSave.Count - create;

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Drafts");
        table.AddColumn(new TableColumn("Pages").RightAligned());
        table.AddRow("To create", create.ToString("N0"));
        table.AddRow("To update", update.ToString("N0"));
        table.AddRow("Already up to date", unchanged.ToString("N0"));
        table.AddRow("Too large to post (over 2 MB)", tooLarge.Count.ToString("N0"));
        table.AddRow("[grey]Total[/]", selectedCount.ToString("N0"));
        AnsiConsole.Write(table);

        // Pages under the base title that no current list produces.
        if (oldSteps.Count > 0) {
            var old = new Table().Border(TableBorder.Rounded);
            old.AddColumn("Old drafts");
            old.AddColumn(new TableColumn("Pages").RightAligned());
            old.AddRow("To move to a new title", oldSteps.Count(s => s.Action == OldDraftAction.Move).ToString("N0"));
            old.AddRow("To blank", oldSteps.Count(s => s.Action == OldDraftAction.Blank).ToString("N0"));
            old.AddRow("Left alone (no draft banner)", oldSteps.Count(s => s.Action == OldDraftAction.LeaveAlone).ToString("N0"));
            AnsiConsole.Write(old);
        }
        foreach (var step in oldSteps) {
            var line = step.Action switch {
                OldDraftAction.Move => $"[green]To move:[/] {Markup.Escape(step.Title)} → {Markup.Escape(step.NewTitle!)}",
                OldDraftAction.Blank => $"[yellow]To blank:[/] {Markup.Escape(step.Title)}",
                _ => $"[grey]Left alone:[/] {Markup.Escape(step.Title)} (no draft banner)",
            };
            AnsiConsole.MarkupLine(line);
        }
        if (settings.TitleFilter is not null) {
            AnsiConsole.MarkupLine("[grey]Skipping old drafts: --title is set. Run without --title to move or blank old drafts.[/]");
        }

        foreach (var draft in tooLarge) {
            AnsiConsole.MarkupLineInterpolated($"[yellow]Too large:[/] {draft.ArticleTitle} ({DraftPageBuilder.FormatMegabytes(draft.Bytes)})");
        }
        if (settings.Limit is not null) {
            AnsiConsole.MarkupLineInterpolated($"[grey]--limit {settings.Limit}: this run saves at most {settings.Limit} drafts.[/]");
        }
    }

    private static void PrintOutcome(int index, int total, Draft draft, EditOutcome outcome) {
        var size = $"{DraftPageBuilder.FormatKilobytes(draft.Bytes)} kB";
        var status = DescribeStatus(outcome.Status);
        var colour = outcome.Status == EditStatus.Failed ? "red" : outcome.Status == EditStatus.Unchanged ? "grey" : "green";
        var note = outcome.Message is null ? string.Empty : $" ({outcome.Message})";
        AnsiConsole.MarkupLine($"[grey][[{index}/{total}]][/] [{colour}]{status}[/] {Markup.Escape(draft.DraftTitle)} [grey]{Markup.Escape(size + note)}[/]");
    }

    private static string DescribeStatus(EditStatus status) => status switch {
        EditStatus.Created => "created",
        EditStatus.Updated => "updated",
        EditStatus.Unchanged => "unchanged",
        _ => "failed",
    };
}
