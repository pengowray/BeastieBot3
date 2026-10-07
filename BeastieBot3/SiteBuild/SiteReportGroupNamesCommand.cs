using System.ComponentModel;
using System.Text;
using System.Text.RegularExpressions;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

// `site report-group-names`: the groups of the site database (kingdom to genus, with the CoL groups
// between) that have no English name, largest first, with the English names that English Wikipedia
// (titles of redirects to the group's article) and the Catalogue of Life give for them, and a
// rules-list.txt line to copy. A name added to rules-list.txt names the group on the site and in the
// Wikipedia lists' headings at once. Writes Markdown and CSV to reports_dir.

namespace BeastieBot3.SiteBuild;

[CommandInfo("site report-group-names", CommandKind.ReadOnly,
    "Report the site's groups that have no English name, largest first, with candidate names from English Wikipedia and the Catalogue of Life and a rules-list.txt line for each.",
    Examples = new[] { "site report-group-names", "site report-group-names --min-species 50" })]
internal sealed partial class SiteReportGroupNamesCommand : Command<SiteReportGroupNamesCommand.Settings> {
    public sealed class Settings : CommonSettings {
        [CommandOption("--database <FILE>")]
        [Description("Site database. Default: Datastore:site_sqlite in paths.ini.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--min-species <N>")]
        [Description("Leave out groups with fewer species than this. Default: 20.")]
        public int MinSpecies { get; init; } = 20;

        [CommandOption("--include-genera")]
        [Description("Also list genera (left out by default: most have no English name of their own).")]
        public bool IncludeGenera { get; init; }

        [CommandOption("-o|--output <FILE>")]
        [Description("Markdown file to write. Default: site-group-names-<time>.md in reports_dir; the CSV goes beside it.")]
        public string? OutputPath { get; init; }
    }

    internal sealed record Candidate(string Name, bool Wikipedia, bool Col, int Score);

    internal sealed record GroupRow(string Rank, string Name, string? Kingdom, int Species, IReadOnlyList<Candidate> Candidates) {
        /// A rules-list.txt line for the best candidate: "Ursidae plural bears".
        public string? RulesLine => Candidates.FirstOrDefault() is { } best ? $"{Name} plural {LowerFirst(best.Name)}" : null;
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        _ = context;
        var paths = settings.CreatePaths();
        var dbPath = settings.DatabasePath ?? paths.GetSiteDatabasePath();
        if (dbPath is null || !File.Exists(dbPath)) {
            AnsiConsole.MarkupLineInterpolated($"[red]Site database not found:[/] {dbPath}");
            return -1;
        }
        var rows = Read(dbPath, settings.MinSpecies, settings.IncludeGenera);
        var output = ReportPathResolver.ResolveFilePath(paths, settings.OutputPath, explicitDirectory: null,
            fallbackBaseDirectory: Path.GetDirectoryName(Path.GetFullPath(dbPath)),
            defaultFileName: $"site-group-names-{DateTimeOffset.Now:yyyyMMdd-HHmmss}.md");
        File.WriteAllText(output, Markdown(rows, dbPath, settings.MinSpecies), Encoding.UTF8);
        File.WriteAllText(Path.ChangeExtension(output, ".csv"), Csv(rows), Encoding.UTF8);
        AnsiConsole.MarkupLineInterpolated($"Groups with no English name and at least {settings.MinSpecies:N0} species: {rows.Count:N0}, of which {rows.Count(r => r.Candidates.Count > 0):N0} have a candidate.");
        AnsiConsole.MarkupLineInterpolated($"[green]Wrote[/] {output}");
        return 0;
    }

    private static List<GroupRow> Read(string dbPath, int minSpecies, bool includeGenera) {
        using var connection = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = dbPath, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT h.node_id, h.rank, h.name, h.kingdom, h.species_count, n.name, n.source
            FROM higher_taxon h
            LEFT JOIN higher_taxon_name n ON n.node_id = h.node_id AND n.source IN ('wikipedia', 'col')
            WHERE h.common_name_en IS NULL AND h.species_count >= @min AND (@genera = 1 OR h.rank <> 'genus')
            ORDER BY h.species_count DESC, h.node_id
            """;
        command.Parameters.AddWithValue("@min", minSpecies);
        command.Parameters.AddWithValue("@genera", includeGenera ? 1 : 0);
        using var reader = command.ExecuteReader();
        var groups = new List<(long Id, string Rank, string Name, string? Kingdom, int Species, List<(string Name, string Source)> Names)>();
        while (reader.Read()) {
            var id = reader.GetInt64(0);
            if (groups.Count == 0 || groups[^1].Id != id) {
                groups.Add((id, reader.GetString(1), reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3), reader.GetInt32(4), []));
            }
            if (!reader.IsDBNull(5)) {
                groups[^1].Names.Add((reader.GetString(5), reader.GetString(6)));
            }
        }
        // A redirect from a species' name ("Hechtia tillandsioides") is not a name of the group.
        using var scientific = connection.CreateCommand();
        scientific.CommandText = """
            SELECT EXISTS (SELECT 1 FROM name_key k JOIN name n ON n.name_id = k.name_id
                           WHERE k.key = @key AND n.name_type IN ('scientific', 'synonym'))
            """;
        var key = scientific.Parameters.Add("@key", SqliteType.Text);
        bool IsScientific(string name) {
            key.Value = SiteNameKey.Fold(name);
            return Convert.ToInt64(scientific.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture) == 1;
        }
        return [.. groups.Select(g => new GroupRow(g.Rank, g.Name, g.Kingdom, g.Species,
            Candidates(g.Name, g.Names.Where(n => !IsScientific(n.Name)))))];
    }

    /// The English names among a group's names, best first: in both sources, then plural, then shorter.
    internal static IReadOnlyList<Candidate> Candidates(string groupName, IEnumerable<(string Name, string Source)> names) {
        var groupKey = SiteNameKey.Fold(groupName);
        return [.. names
            .Where(n => IsEnglishName(n.Name, groupKey))
            .GroupBy(n => SiteNameKey.Fold(n.Name))
            .Select(g => {
                var wikipedia = g.Any(n => n.Source == "wikipedia");
                var col = g.Any(n => n.Source == "col");
                var name = g.First().Name;
                var score = (wikipedia && col ? 4 : 0) + (IsPlural(name) ? 2 : 0) + (wikipedia ? 1 : 0);
                return new Candidate(name, wikipedia, col, score);
            })
            .OrderByDescending(c => c.Score).ThenBy(c => c.Name.Length).ThenBy(c => c.Name, StringComparer.Ordinal)];
    }

    /// A name that can be an English name of the group: not the group's own name or a form of it, no
    /// Latin ending, no adjective.
    internal static bool IsEnglishName(string name, string groupKey) {
        var key = SiteNameKey.Fold(name);
        if (key.Length < 3 || key.StartsWith(groupKey, StringComparison.Ordinal) || key.StartsWith("order ", StringComparison.Ordinal)
            || key.StartsWith("family ", StringComparison.Ordinal) || key.StartsWith("list of ", StringComparison.Ordinal) || key.Contains('(')) {
            return false;
        }

        // One capitalised word with a Latin rank ending ("Leguminosae", "Rutales") is a scientific name.
        if (!name.Contains(' ') && LatinEnding().IsMatch(key)) {
            return false;
        }
        return !AdjectiveEnding().IsMatch(key);
    }

    internal static bool IsPlural(string name) {
        var last = name.Split(' ', StringSplitOptions.RemoveEmptyEntries).LastOrDefault()?.ToLowerInvariant() ?? "";
        return last is "family" || (last.EndsWith('s') && !last.EndsWith("ss", StringComparison.Ordinal) && !last.EndsWith("us", StringComparison.Ordinal));
    }

    private static string LowerFirst(string s) => s.Length > 1 && char.IsUpper(s[0]) && !char.IsUpper(s[1]) ? char.ToLowerInvariant(s[0]) + s[1..] : s;

    [GeneratedRegex(@"(ae|ales|iformes|ida|idae|inae|oidea|ini|anae|iflorae|oidei|ata|ia|morpha|phyta|opsida|ina)$")]
    private static partial Regex LatinEnding();

    [GeneratedRegex(@"(ous|ic|ian|eous)$")]
    private static partial Regex AdjectiveEnding();

    private static string Markdown(IReadOnlyList<GroupRow> rows, string dbPath, int minSpecies) {
        var sb = new StringBuilder();
        sb.AppendLine("# Groups with no English name");
        sb.AppendLine();
        sb.AppendLine($"Site database: `{dbPath}`. Groups with at least {minSpecies:N0} species, largest first: {rows.Count:N0}, of which {rows.Count(r => r.Candidates.Count > 0):N0} have a candidate name.");
        sb.AppendLine();
        sb.AppendLine("Candidates are the titles of English Wikipedia redirects to the group's article (W) and the Catalogue of Life's English names (C), best first: in both sources, then plural, then shorter. Check a candidate before adding its line to `rules/rules-list.txt`: some redirects are old names or names of a part of the group. A line `Name = singular ! plural` gives both forms.");
        sb.AppendLine();
        sb.AppendLine("| Rank | Group | Kingdom | Species | Candidates | rules-list.txt line |");
        sb.AppendLine("| --- | --- | --- | ---: | --- | --- |");
        foreach (var r in rows) {
            var candidates = string.Join(", ", r.Candidates.Take(8).Select(c => $"{c.Name} ({(c.Wikipedia ? "W" : "")}{(c.Col ? "C" : "")})"));
            sb.AppendLine($"| {r.Rank} | {r.Name} | {r.Kingdom} | {r.Species:N0} | {candidates.Replace("|", "\\|")} | {(r.RulesLine is { } line ? $"`{line}`" : "")} |");
        }
        return sb.ToString();
    }

    private static string Csv(IReadOnlyList<GroupRow> rows) {
        static string Q(string? v) => v is null ? "" : "\"" + v.Replace("\"", "\"\"") + "\"";
        var sb = new StringBuilder("rank,group,kingdom,species,best_candidate,other_candidates,rules_line\n");
        foreach (var r in rows) {
            sb.Append(Q(r.Rank)).Append(',').Append(Q(r.Name)).Append(',').Append(Q(r.Kingdom)).Append(',').Append(r.Species).Append(',')
                .Append(Q(r.Candidates.FirstOrDefault()?.Name)).Append(',').Append(Q(string.Join("; ", r.Candidates.Skip(1).Select(c => c.Name)))).Append(',')
                .Append(Q(r.RulesLine)).Append('\n');
        }
        return sb.ToString();
    }
}
