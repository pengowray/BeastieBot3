using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn.Citations;
using BeastieBot3.Shared.Wikitext;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

// `site check-citations`: reads the citation of every latest assessment in the IUCN API cache into
// IucnCitationParts, the way `site build-db` will, and checks the result against the {{cite iucn}}
// templates in cached English Wikipedia articles. Read-only on both caches; writes a Markdown report.
//
// Three passes:
//   1. `taxa`: the latest assessments (any scope), taken from each taxon record's assessment list,
//      with the assessments an errata version may have replaced (IucnTaxaHeaders).
//   2. `wiki_pages` in the Wikipedia cache: every {{cite iucn}} whose article-number names one of
//      those assessments. About 2 seconds once the file is in the disk cache.
//   3. `assessments`: each latest assessment's record, read in row order, parsed and, where an
//      article cites it, compared.
//   4. Only when some author name has a letter lost to an encoding error: the other assessments'
//      records, for their assessor names, so the damaged names are repaired from the same names
//      `site build-db` uses (AssessorNamePool). The latest assessments with such a name are parsed
//      again after that.

namespace BeastieBot3.SiteBuild;

[CommandInfo("site check-citations", CommandKind.ReadOnly,
    "Parse IUCN's citation of every latest assessment in the IUCN API cache into authors, title annotations and DOI, the parts the public site uses to build {{cite iucn}}. Compares the result with the {{cite iucn}} templates in cached English Wikipedia articles that cite the same assessment, and writes a Markdown report: parse failures, kinds of author name, how author lists were split, DOI checks, and each kind of difference from Wikipedia.",
    Examples = new[] {
        "site check-citations",
        "site check-citations --limit 5000",
        "site check-citations --no-wiki",
    })]
internal sealed class SiteCheckCitationsCommand : Command<SiteCheckCitationsCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--cache <PATH>")]
        [Description("IUCN API cache database. Default: IUCN_api_cache_sqlite in paths.ini.")]
        public string? CacheDatabase { get; init; }

        [CommandOption("--wiki-cache <PATH>")]
        [Description("Wikipedia cache database. Default: enwiki_cache_sqlite in paths.ini.")]
        public string? WikiCacheDatabase { get; init; }

        [CommandOption("--no-wiki")]
        [Description("Parse the IUCN citations without comparing them with Wikipedia.")]
        public bool NoWiki { get; init; }

        [CommandOption("--limit <N>")]
        [Description("Parse only the first N latest assessments, in the order of the cached taxon records.")]
        public int? Limit { get; init; }

        [CommandOption("--examples <N>")]
        [Description("Number of examples the report lists for each kind of result. Default: 10.")]
        public int Examples { get; init; } = 10;

        [CommandOption("-o|--output <PATH>")]
        [Description("File for the report. Default: site-citation-check-<date and time>.md in reports_dir from paths.ini.")]
        public string? OutputPath { get; init; }
    }

    private sealed record LatestAssessment(long AssessmentId, AssessmentScopeKind Scope, IReadOnlyList<long> Predecessors);

    private sealed record WikiCite(string ArticleTitle, WikiCiteIucn Cite);

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var cachePath = paths.ResolveIucnApiCachePath(settings.CacheDatabase);
        if (!File.Exists(cachePath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]IUCN API cache not found:[/] {cachePath}");
            return -1;
        }
        string? wikiPath = null;
        if (!settings.NoWiki) {
            wikiPath = paths.ResolveWikipediaCachePath(settings.WikiCacheDatabase);
            if (!File.Exists(wikiPath)) {
                AnsiConsole.MarkupLineInterpolated($"[yellow]Wikipedia cache not found, so no comparison:[/] {wikiPath}");
                wikiPath = null;
            }
        }
        if (settings.Limit is <= 0) {
            AnsiConsole.MarkupLine("[red]--limit must be at least 1.[/]");
            return -1;
        }
        AnsiConsole.MarkupLineInterpolated($"[grey]IUCN API cache:[/] {cachePath}");
        if (wikiPath is not null) AnsiConsole.MarkupLineInterpolated($"[grey]Wikipedia cache:[/] {wikiPath}");

        var started = Stopwatch.StartNew();
        var tally = new CitationCheckTally(Math.Max(1, settings.Examples));
        using var cache = OpenReadOnly(cachePath);

        var latest = ReadLatest(cache, tally, settings.Limit, cancellationToken);
        AnsiConsole.MarkupLineInterpolated($"[grey]Latest assessments to read:[/] {latest.Count:N0}");

        var wikiCites = new Dictionary<long, List<WikiCite>>();
        if (wikiPath is not null) {
            using var wiki = OpenReadOnly(wikiPath);
            wikiCites = ScanWiki(wiki, latest, tally, cancellationToken);
            tally.WikiCompared = true;
            AnsiConsole.MarkupLineInterpolated($"[grey]{{{{cite iucn}}}} templates citing those assessments:[/] {wikiCites.Values.Sum(l => l.Count):N0}");
        }

        ReadAssessments(cache, latest, wikiCites, tally, cancellationToken);

        var outputPath = ReportPathResolver.ResolveFilePath(
            paths, settings.OutputPath, explicitDirectory: null,
            fallbackBaseDirectory: Path.GetDirectoryName(cachePath),
            defaultFileName: $"site-citation-check-{DateTimeOffset.Now:yyyyMMdd-HHmmss}.md");
        var inputs = new CitationCheckInputs(cachePath, wikiPath, settings.Limit, DateTimeOffset.Now, started.Elapsed);
        File.WriteAllText(outputPath, CitationCheckReport.Build(tally, inputs), Encoding.UTF8);

        WriteSummary(tally);
        AnsiConsole.MarkupLineInterpolated($"[green]Report written to:[/] {outputPath}");
        return 0;
    }

    private static SqliteConnection OpenReadOnly(string path) {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
        }.ConnectionString);
        connection.Open();
        return connection;
    }

    // ------------------------------------------------------------ pass 1: latest assessments

    private static List<LatestAssessment> ReadLatest(SqliteConnection cache, CitationCheckTally tally, int? limit, CancellationToken cancellationToken) {
        var latest = new List<LatestAssessment>();
        var seen = new HashSet<long>();
        using var command = cache.CreateCommand();
        command.CommandText = "SELECT json FROM taxa ORDER BY id";
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        ProgressConsole.Run("Reading taxon records", 0, progress => {
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                tally.TaxaRows++;
                progress.Increment();
                IReadOnlyList<IucnAssessmentHeader> headers;
                try {
                    using var document = JsonDocument.Parse(reader.GetString(0));
                    headers = IucnTaxaHeaders.Read(document.RootElement);
                } catch (JsonException) {
                    tally.TaxaRowsUnreadable++;
                    continue;
                }
                foreach (var header in headers) {
                    if (!header.Latest || !seen.Add(header.AssessmentId)) continue;
                    var scope = IucnTaxaHeaders.IsGlobal(header) ? AssessmentScopeKind.Global
                        : header.ScopeCodes.Count == 0 ? AssessmentScopeKind.NoScope
                        : AssessmentScopeKind.Regional;
                    if (limit is { } max && latest.Count >= max) {
                        tally.LatestSkippedByLimit++;
                        continue;
                    }
                    tally.AddLatest(scope);
                    latest.Add(new LatestAssessment(header.AssessmentId, scope, IucnTaxaHeaders.PredecessorIds(headers, header.AssessmentId)));
                }
            }
        });
        return latest;
    }

    // ------------------------------------------------------------ pass 2: en-wiki templates

    private static Dictionary<long, List<WikiCite>> ScanWiki(SqliteConnection wiki, List<LatestAssessment> latest,
        CitationCheckTally tally, CancellationToken cancellationToken) {
        var wanted = latest.Select(l => l.AssessmentId).ToHashSet();
        var cites = new Dictionary<long, List<WikiCite>>();
        using var command = wiki.CreateCommand();
        command.CommandText = """
            SELECT page_title, wikitext FROM wiki_pages
            WHERE download_status = 'cached' AND is_redirect = 0 AND wikitext IS NOT NULL
            """;
        command.CommandTimeout = 0;
        using var reader = command.ExecuteReader();
        ProgressConsole.Run("Scanning Wikipedia articles", 0, progress => {
            while (reader.Read()) {
                cancellationToken.ThrowIfCancellationRequested();
                tally.WikiPagesScanned++;
                progress.Increment();
                var title = reader.GetString(0);
                var any = false;
                foreach (var cite in CiteIucnTemplateScanner.Find(reader.GetString(1))) {
                    any = true;
                    tally.WikiTemplates++;
                    if (CiteIucnTemplateScanner.ArticleIds(cite) is not { } ids) {
                        tally.WikiTemplatesWithoutIds++;
                        continue;
                    }
                    if (!wanted.Contains(ids.AssessmentId)) {
                        tally.WikiTemplatesOtherAssessment++;
                        continue;
                    }
                    if (!cites.TryGetValue(ids.AssessmentId, out var list)) cites[ids.AssessmentId] = list = new List<WikiCite>();
                    list.Add(new WikiCite(title, cite));
                }
                if (any) tally.WikiPagesWithTemplate++;
            }
        });
        return cites;
    }

    // ------------------------------------------------------------ pass 3: assessment records

    private static void ReadAssessments(SqliteConnection cache, List<LatestAssessment> latest,
        Dictionary<long, List<WikiCite>> wikiCites, CitationCheckTally tally, CancellationToken cancellationToken) {
        // Map each wanted assessment id to its row id over the unique index, then read the rows in
        // row order, so the JSON is read front to back rather than at random.
        var byId = latest.ToDictionary(l => l.AssessmentId);
        var rows = new List<(long RowId, LatestAssessment Assessment)>();
        using (var index = cache.CreateCommand()) {
            index.CommandText = "SELECT id, assessment_id FROM assessments";
            index.CommandTimeout = 0;
            using var reader = index.ExecuteReader();
            while (reader.Read()) {
                if (byId.TryGetValue(reader.GetInt64(1), out var assessment)) rows.Add((reader.GetInt64(0), assessment));
            }
        }
        tally.PayloadMissing = latest.Count - rows.Count;
        rows.Sort((a, b) => a.RowId.CompareTo(b.RowId));

        var names = new AssessorNamePool();
        var waiting = new List<(LatestAssessment Assessment, string DownloadedAt, string Json)>();
        using var command = cache.CreateCommand();
        command.CommandText = "SELECT downloaded_at, json FROM assessments WHERE id = @id";
        var idParameter = command.Parameters.Add("@id", SqliteType.Integer);
        ProgressConsole.Run("Reading assessment citations", rows.Count, progress => {
            foreach (var (rowId, assessment) in rows) {
                cancellationToken.ThrowIfCancellationRequested();
                progress.Increment();
                idParameter.Value = rowId;
                string downloadedAt;
                string json;
                using (var reader = command.ExecuteReader()) {
                    if (!reader.Read()) continue;
                    downloadedAt = reader.GetString(0);
                    json = reader.GetString(1);
                }
                JsonDocument document;
                try {
                    document = JsonDocument.Parse(json);
                } catch (JsonException) {
                    tally.PayloadUnreadable++;
                    continue;
                }
                using (document) {
                    var root = document.RootElement;
                    if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("latest", out var flag) && flag.ValueKind == JsonValueKind.False) {
                        tally.PayloadLatestFlagFalse++;
                    }
                    var parse = IucnCitationPartsParser.Parse(root, StoredUtc.Parse(downloadedAt), assessment.Predecessors);
                    if (parse.DamagedAuthorNames.Count > 0) {
                        waiting.Add((assessment, downloadedAt, json));
                        continue;
                    }
                    names.AddFrom(parse);
                    AddParse(tally, parse, assessment, wikiCites);
                }
            }
        });
        if (waiting.Count == 0) {
            return;
        }

        // The repair must use the same names as `site build-db`, which reads earlier assessments too.
        var latestRows = rows.Select(r => r.RowId).ToHashSet();
        using (var others = cache.CreateCommand()) {
            others.CommandText = "SELECT id, json FROM assessments ORDER BY id";
            others.CommandTimeout = 0;
            using var reader = others.ExecuteReader();
            ProgressConsole.Run("Reading the other assessments' author names", 0, progress => {
                while (reader.Read()) {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (latestRows.Contains(reader.GetInt64(0))) continue;
                    progress.Increment();
                    try {
                        using var document = JsonDocument.Parse(reader.GetString(1));
                        names.AddFrom(IucnCitationPartsParser.Parse(document.RootElement, null));
                        tally.NamePoolPayloads++;
                    } catch (JsonException) {
                        // An unreadable earlier record only gives no names.
                    }
                }
            });
        }
        foreach (var (assessment, downloadedAt, json) in waiting) {
            using var document = JsonDocument.Parse(json);
            AddParse(tally, IucnCitationPartsParser.Parse(document.RootElement, StoredUtc.Parse(downloadedAt), assessment.Predecessors, names.Repair),
                assessment, wikiCites);
        }
    }

    private static void AddParse(CitationCheckTally tally, IucnCitationParse parse, LatestAssessment assessment,
        Dictionary<long, List<WikiCite>> wikiCites) {
        tally.AddParse(parse, assessment.Scope);
        if (parse.Parts is { } parts && wikiCites.TryGetValue(assessment.AssessmentId, out var cites)) {
            foreach (var cite in cites) {
                tally.AddComparison(parts, cite.ArticleTitle, WikiCitationComparer.Compare(parts, cite.Cite, assessment.Predecessors));
            }
        }
    }

    private static void WriteSummary(CitationCheckTally tally) {
        var table = new Table().AddColumn("Measure").AddColumn(new TableColumn("Count").RightAligned());
        table.AddRow("Parsed into citation parts", $"{tally.Parsed:N0}");
        table.AddRow("Not parsed", $"{tally.Failures.Values.Sum():N0}");
        table.AddRow("Every author identified as a person or organisation", $"{tally.AllAuthorsStructured:N0}");
        table.AddRow("At least one author name left as published", $"{tally.SomeAuthorsVerbatim:N0}");
        table.AddRow("Author names with a lost letter, repaired", $"{tally.AuthorNameRepairs.Values.Sum():N0}");
        table.AddRow("Author names with a lost letter, not repaired", $"{tally.AuthorNamesNotRepaired.Values.Sum():N0}");
        if (tally.WikiCompared) {
            table.AddRow("En-wiki {{cite iucn}} templates compared", $"{tally.WikiTemplatesMatched:N0}");
            table.AddRow("… identical authors", $"{tally.AuthorAgreements.GetValueOrDefault(AuthorAgreement.Same):N0}");
            table.AddRow("… same DOI", $"{tally.DoiAgreements.GetValueOrDefault(DoiAgreement.Same):N0}");
        }
        AnsiConsole.Write(table);
    }
}
