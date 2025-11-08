using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

// Characterize data in a SQLite database to get a sense of the shape of the data, whether 
// normalization is needed, how often they have null values, etc.
//
// Originally for "nameusage" table in Catalogue of Life, but can be used for other tables too. 
// (Class and command ought to be renamed to reflect this)
//
// Checking for non-normalized or unusual text characters, e.g.
// - checks for leading/trailing whitespace
// - checks for non-NFC normalized text
// - checks for control characters (tabs, newlines, etc.)
// - checks for use of NBSP (non-breaking spaces), and narrow NBSP

// Focuses somewhat on taxon related things
// - e.g. checks for daggers in text (which represent extinct taxa but ought to be absent in certain fields)
// - e.g. checks for capitalization patterns (important for taxa)
// 
// TODO: 
// - [ ] rename and move to more general name/command name
// - [x] add option to run on all (non-system/non-FTS) tables in the database
// - [ ] list fields containing smart quotes
// - [ ] fix caps checking: characterize caps category of first four words. Categories: none (no first/second/third/fourth word), allcaps, title (initial case), lower, title-mixed (initial letter upper and then mixed), lower-mixed (initial letter lowercase), unicameral (e.g. Arabic, CJK, etc)
// - [ ] include a count of "only A-Za-z" in the value, and (A-Za-z and ordinary space), and (A-Za-z and ordinary space and period). give these shorter names
// - [ ] word counts (most common, min, max, average, stdev)
// - [ ] most common values (ala sqlite-utils analyze-tables); show up to the 10 most common values (including null). If there are 20 or less values in total, show them all.
// - [ ] frequency of a period in the text (to catch infraspecies categories)
// - [ ] contains any html tags (simple check)
// - [ ] contains json structures (simple check)
// - [x] output report to a file by default, including name of the database and table profiled in the name. Place in the database's folder in ./data-analysis/
// - [ ] Guess if a field is free text (e.g. remarks), taxon related, name(s), controlled vocabulary/code (enum), identifier (e.g. uuid), date, numeric value, etc.
// - [ ] Guess language used (especially those used by IUCN reports: English, Spanish/Castilian, Portuguese, French)
// - [ ] Check urls
// - [ ] Auto archive urls in wayback machine

public sealed class ColNameUsageFieldProfileCommand : Command<ColNameUsageFieldProfileCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }

        [CommandOption("--database <PATH>")]
        [Description("Explicit SQLite database path to profile. Overrides dataset defaults.")]
        public string? DatabasePath { get; init; }

        [CommandOption("--table <NAME>")]
        [Description("Table to profile. Defaults to nameusage for COL or taxonomy for IUCN.")]
        public string? Table { get; init; }

        [CommandOption("--columns <COLUMNS>")]
        public string? Columns { get; init; }

        [CommandOption("--all-columns")]
        [Description("Include non-text columns when calculating field statistics.")]
        public bool IncludeNonText { get; init; }

        [CommandOption("--all-tables")]
        [Description("Profile every non-system, non-FTS table in the database.")]
        public bool AllTables { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Maximum number of rows to scan (0 = entire table).")]
        public long Limit { get; init; }

        [CommandOption("--char-samples <COUNT>")]
        [Description("Maximum distinct non-ASCII/control characters to list per column.")]
        public int MaxCharSamples { get; init; } = 24;

        [CommandOption("--iucn")]
        [Description("Profile the IUCN SQLite database instead of Catalogue of Life.")]
        public bool UseIucnDatabase { get; init; }
    }

    private static void WriteReport(ReportContext context, IEnumerable<ColumnStats> stats, IReportWriter writer) {
        writer.WriteLine($"[bold]{Markup.Escape(context.TableName)}[/] field profile");
        writer.WriteLine($"Dataset: {Markup.Escape(context.DatasetLabel)}");
        writer.WriteLine($"Database: {Markup.Escape(context.DatabasePath)}");
        writer.WriteLine($"Settings: {Markup.Escape(context.SettingsFilePath)}");

        var columnLine = $"Columns profiled: {context.ColumnCount:N0}";
        if (context.IncludeNonTextColumns) {
            columnLine += " (including non-text columns)";
        }
        writer.WriteLine(columnLine);

        writer.WriteLine($"Rows in table: {context.TotalRowCount:N0}");
        if (context.TargetRowCount != context.TotalRowCount) {
            writer.WriteLine($"Rows available after limit: {context.TargetRowCount:N0}");
        }
        writer.WriteLine($"Rows scanned: {context.ProcessedRowCount:N0}");

        if (context.RowLimit.HasValue) {
            writer.WriteLine($"Row limit: {context.RowLimit.Value:N0}");
        }

        writer.WriteLine($"Generated: {context.GeneratedAt:yyyy-MM-dd HH:mm:ss zzz}");
        writer.WriteLine(string.Empty);

        WriteColumnReports(stats, writer);
    }

    private static string CreateReportPath(string databasePath, string tableName, DateTimeOffset timestamp) {
        var directory = Path.GetDirectoryName(databasePath) ?? Directory.GetCurrentDirectory();
        var reportDirectory = Path.Combine(directory, "data-analysis");
        Directory.CreateDirectory(reportDirectory);

        var dbName = Path.GetFileNameWithoutExtension(databasePath);
        var safeTableName = SanitizeForFileName(tableName);
        var fileName = $"{dbName}-{safeTableName}-profile-{timestamp:yyyyMMdd-HHmmss}.txt";
        return Path.Combine(reportDirectory, fileName);
    }

    private static string SanitizeForFileName(string value) {
        var invalid = Path.GetInvalidFileNameChars();
        var builder = new StringBuilder(value.Length);

        foreach (var ch in value) {
            if (invalid.Contains(ch) || char.IsWhiteSpace(ch)) {
                builder.Append('_');
            }
            else {
                builder.Append(ch);
            }
        }

        var sanitized = builder.ToString().Trim('_');
        return string.IsNullOrEmpty(sanitized) ? "table" : sanitized;
    }

    private interface IReportWriter {
        void WriteLine(string value);
    }

    private sealed class ConsoleReportWriter : IReportWriter {
        public void WriteLine(string value) {
            AnsiConsole.MarkupLine(value);
        }
    }

    private sealed class PlainTextReportWriter : IReportWriter {
        private readonly StringBuilder _builder = new();

        public void WriteLine(string value) {
            _builder.AppendLine(StripMarkup(value));
        }

        public string GetContent() => _builder.ToString();

        private static string StripMarkup(string value) {
            if (string.IsNullOrEmpty(value)) {
                return value;
            }

            const char openPlaceholder = '\u0001';
            const char closePlaceholder = '\u0002';

            var normalized = value.Replace("[[", openPlaceholder.ToString(), StringComparison.Ordinal)
                                   .Replace("]]", closePlaceholder.ToString(), StringComparison.Ordinal);

            normalized = Regex.Replace(normalized, @"\[[^\]]+\]", string.Empty);

            return normalized
                .Replace(openPlaceholder.ToString(), "[", StringComparison.Ordinal)
                .Replace(closePlaceholder.ToString(), "]", StringComparison.Ordinal);
        }
    }

    private sealed record ColumnInfo(string Name, string DeclaredType, bool TreatAsText);

    private sealed record ReportContext(
        string SettingsFilePath,
        string DatasetLabel,
        string DatabasePath,
        string TableName,
        int ColumnCount,
        bool IncludeNonTextColumns,
        long TotalRowCount,
        long TargetRowCount,
        long ProcessedRowCount,
        long? RowLimit,
        DateTimeOffset GeneratedAt
    );

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        var datasetLabel = settings.UseIucnDatabase ? "IUCN" : "Catalogue of Life";
        var configuredPath = !string.IsNullOrWhiteSpace(settings.DatabasePath)
            ? settings.DatabasePath
            : settings.UseIucnDatabase
                ? paths.GetIucnDatabasePath()
                : paths.GetColSqlitePath();

        if (string.IsNullOrWhiteSpace(configuredPath)) {
            var guidance = settings.UseIucnDatabase
                ? "Set [bold]Datastore:IUCN_CVS_sqlite[/] in paths.ini or pass --database."
                : "Set [bold]Datastore:COL_sqlite[/] in paths.ini or pass --database.";
            AnsiConsole.MarkupLine($"[red]{datasetLabel} database path is not configured.[/] {guidance}");
            return -1;
        }

        string dbPath;
        try {
            dbPath = Path.GetFullPath(configuredPath);
        }
        catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Failed to resolve database path[/] {Markup.Escape(configuredPath)}: {Markup.Escape(ex.Message)}");
            return -1;
        }

        if (!File.Exists(dbPath)) {
            AnsiConsole.MarkupLine($"[red]{datasetLabel} SQLite database not found at:[/] {Markup.Escape(dbPath)}");
            return -2;
        }

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        var defaultTableName = string.IsNullOrWhiteSpace(settings.Table)
            ? (settings.UseIucnDatabase ? "taxonomy" : "nameusage")
            : settings.Table;

        if (settings.AllTables && !string.IsNullOrWhiteSpace(settings.Columns)) {
            AnsiConsole.MarkupLine("[red]--columns cannot be combined with --all-tables.[/]");
            return -4;
        }

        if (!settings.AllTables && !TableExists(connection, defaultTableName)) {
            AnsiConsole.MarkupLine($"[red]Table {Markup.Escape(defaultTableName)} not found in the database.[/]");
            return -3;
        }

        if (settings.AllTables && !string.IsNullOrWhiteSpace(settings.Table)) {
            AnsiConsole.MarkupLine("[yellow]Ignoring --table because --all-tables was specified.[/]");
        }

        var requestedColumns = settings.AllTables
            ? Array.Empty<string>()
            : ParseColumns(settings.Columns);

        List<string> tablesToProfile;
        if (settings.AllTables) {
            tablesToProfile = GetProfileableTables(connection);
            if (tablesToProfile.Count == 0) {
                AnsiConsole.MarkupLine("[yellow]No eligible tables found to profile.[/]");
                return 0;
            }
        }
        else {
            tablesToProfile = new List<string> { defaultTableName };
        }

        var overallSuccess = true;

        for (var tableIndex = 0; tableIndex < tablesToProfile.Count; tableIndex++) {
            var tableName = tablesToProfile[tableIndex];

            if (tableIndex > 0) {
                AnsiConsole.MarkupLine(string.Empty);
            }

            AnsiConsole.MarkupLine($"[grey]Profiling table:[/] {Markup.Escape(tableName)}");

            var availableColumns = GetColumns(connection, tableName, settings.IncludeNonText);
            var selectedColumns = SelectColumns(availableColumns, requestedColumns);

            if (selectedColumns.Count == 0) {
                AnsiConsole.MarkupLine($"[yellow]No columns selected for profiling in table {Markup.Escape(tableName)}.[/]");
                continue;
            }

            var quotedTableName = QuoteIdentifier(tableName);
            var columnNamesSql = string.Join(", ", selectedColumns.Select(c => QuoteIdentifier(c.Name)));
            var limitClause = settings.Limit > 0 ? $" LIMIT {settings.Limit}" : string.Empty;

            var stats = selectedColumns.ToDictionary(
                c => c.Name,
                c => new ColumnStats(c.Name, c.DeclaredType, settings.MaxCharSamples)
            );

            var totalRowCount = GetRowCount(connection, tableName);
            var targetRowCount = settings.Limit > 0 && settings.Limit < totalRowCount ? settings.Limit : totalRowCount;

            var selectSql = $"SELECT {columnNamesSql} FROM {quotedTableName}{limitClause}";

            using var command = connection.CreateCommand();
            command.CommandText = selectSql;
            command.CommandTimeout = 0;

            long processedRows = 0;
            AnsiConsole.Status()
                .Spinner(Spinner.Known.Dots)
                .Start($"Scanning rows in {tableName}...", ctx => {
                    using var reader = command.ExecuteReader();
                    while (reader.Read()) {
                        cancellationToken.ThrowIfCancellationRequested();
                        processedRows++;

                        for (var i = 0; i < selectedColumns.Count; i++) {
                            var col = selectedColumns[i];
                            var columnStats = stats[col.Name];

                            if (reader.IsDBNull(i)) {
                                columnStats.RegisterNull();
                                continue;
                            }

                            string? value;
                            try {
                                value = reader.GetString(i);
                            }
                            catch (InvalidCastException) {
                                value = Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture);
                            }

                            columnStats.RegisterValue(value);
                        }

                        if (processedRows % 200_000 == 0) {
                            ctx.Status($"Scanning rows in {tableName}... {processedRows:N0}/{targetRowCount:N0}");
                        }
                    }
                });

            var reportContext = new ReportContext(
                paths.SourceFilePath,
                datasetLabel,
                dbPath,
                tableName,
                selectedColumns.Count,
                settings.IncludeNonText,
                totalRowCount,
                targetRowCount,
                processedRows,
                settings.Limit > 0 ? settings.Limit : null,
                DateTimeOffset.Now
            );

            WriteReport(reportContext, stats.Values, new ConsoleReportWriter());

            var plainWriter = new PlainTextReportWriter();
            WriteReport(reportContext, stats.Values, plainWriter);
            var reportContent = plainWriter.GetContent();

            try {
                var reportPath = CreateReportPath(reportContext.DatabasePath, reportContext.TableName, reportContext.GeneratedAt);
                File.WriteAllText(reportPath, reportContent, Encoding.UTF8);
                AnsiConsole.MarkupLine($"[green]Saved report to:[/] {Markup.Escape(reportPath)}");
            }
            catch (Exception ex) {
                overallSuccess = false;
                AnsiConsole.MarkupLine($"[red]Failed to write report for table {Markup.Escape(tableName)}:[/] {Markup.Escape(ex.Message)}");
            }
        }

        return overallSuccess ? 0 : -5;
    }

    private static IReadOnlyList<string> ParseColumns(string? rawColumns) {
        if (string.IsNullOrWhiteSpace(rawColumns)) {
            return Array.Empty<string>();
        }

        return rawColumns
            .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToArray();
    }

    private static List<ColumnInfo> GetColumns(SqliteConnection connection, string table, bool includeNonText) {
        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA table_info({QuoteIdentifier(table)});";

        var result = new List<ColumnInfo>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var columnName = reader.GetString(1);
            var declaredType = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
            var treatAsText = DetermineIfText(declaredType) || includeNonText;
            result.Add(new ColumnInfo(columnName, declaredType, treatAsText));
        }
        return result;
    }

    private static List<string> GetProfileableTables(SqliteConnection connection) {
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name COLLATE NOCASE;";

        var tables = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            var name = reader.GetString(0);
            var definition = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            if (IsFullTextTable(name, definition)) {
                continue;
            }
            tables.Add(name);
        }

        return tables;
    }

    private static bool IsFullTextTable(string name, string? definition) {
        if (!string.IsNullOrWhiteSpace(definition) && definition.IndexOf("USING FTS", StringComparison.OrdinalIgnoreCase) >= 0) {
            return true;
        }

        var lowerName = name.ToLowerInvariant();

        if (lowerName.EndsWith("_fts", StringComparison.Ordinal) || lowerName.Contains("_fts_", StringComparison.Ordinal) || lowerName.StartsWith("fts_", StringComparison.Ordinal)) {
            return true;
        }

        foreach (var suffix in FullTextShadowSuffixes) {
            if (!lowerName.EndsWith(suffix, StringComparison.Ordinal)) {
                continue;
            }

            var prefix = lowerName[..(lowerName.Length - suffix.Length)];
            if (prefix.EndsWith("_fts", StringComparison.Ordinal)) {
                return true;
            }
        }

        return false;
    }

    private static readonly string[] FullTextShadowSuffixes = {
        //"_content",
        //"_data",
        "_docsize",
        "_idx",
        "_map",
        "_segments",
        "_segdir",
        "_stat"
    };

    private static List<ColumnInfo> SelectColumns(IEnumerable<ColumnInfo> available, IReadOnlyList<string> requested) {
        if (requested.Count == 0) {
            return available.Where(c => c.TreatAsText).ToList();
        }

        var lookup = available.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);
        var selected = new List<ColumnInfo>();

        foreach (var name in requested) {
            if (!lookup.TryGetValue(name, out var column)) {
                AnsiConsole.MarkupLine($"[yellow]Column {Markup.Escape(name)} not found; skipping.[/]");
                continue;
            }
            selected.Add(column);
        }
        return selected;
    }

    private static bool DetermineIfText(string declaredType) {
        if (string.IsNullOrWhiteSpace(declaredType)) {
            return true;
        }

        var type = declaredType.Trim().ToUpperInvariant();
        return type.Contains("CHAR") || type.Contains("CLOB") || type.Contains("TEXT");
    }

    private static bool TableExists(SqliteConnection connection, string table) {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=@name LIMIT 1";
        command.Parameters.AddWithValue("@name", table);
        return command.ExecuteScalar() != null;
    }

    private static long GetRowCount(SqliteConnection connection, string table) {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {QuoteIdentifier(table)}";
        command.CommandTimeout = 0;
        var value = command.ExecuteScalar();
        return value is long l ? l : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    private static string QuoteIdentifier(string identifier) {
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }

    private static string FormatCount(long count, long total) {
        if (count == 0 || total == 0) {
            return "-";
        }

        var percentage = (double)count / total * 100;
        return $"{count:N0} ({percentage:F3}%)";
    }

    private static string FormatRuneSamples(IReadOnlyCollection<uint> codes, int maxSamples) {
        if (codes.Count == 0) {
            return "-";
        }

        var ordered = codes.OrderBy(c => c).ToList();
        var sampleCount = Math.Min(maxSamples, ordered.Count);
        var builder = new StringBuilder();
        for (var i = 0; i < sampleCount; i++) {
            if (i > 0) {
                builder.Append(", ");
            }
            builder.Append(DescribeRune(ordered[i]));
        }

        if (ordered.Count > sampleCount) {
            builder.Append($" … (+{ordered.Count - sampleCount} more)");
        }

        return builder.ToString();
    }

    private static bool IsPunctuationCategory(UnicodeCategory category) {
        return category is UnicodeCategory.ConnectorPunctuation
            or UnicodeCategory.DashPunctuation
            or UnicodeCategory.OpenPunctuation
            or UnicodeCategory.ClosePunctuation
            or UnicodeCategory.InitialQuotePunctuation
            or UnicodeCategory.FinalQuotePunctuation
            or UnicodeCategory.OtherPunctuation;
    }

    private static string? GetUnicodeBlock(uint codePoint) {
        if (codePoint <= 0x007F) {
            return null;
        }

        if (codePoint <= 0x00FF) return "Latin-1 Supplement";
        if (codePoint <= 0x017F) return "Latin Extended-A";
        if (codePoint <= 0x024F) return "Latin Extended-B";
        if (codePoint <= 0x02AF) return "IPA Extensions";
        if (codePoint <= 0x02FF) return "Spacing Modifier Letters";
        if (codePoint <= 0x036F) return "Combining Diacritical Marks";
        if (codePoint <= 0x03FF) return "Greek and Coptic";
        if (codePoint <= 0x052F) return "Cyrillic";
        if (codePoint <= 0x058F) return "Armenian";
        if (codePoint <= 0x05FF) return "Hebrew";
        if (codePoint <= 0x06FF) return "Arabic";
        if (codePoint <= 0x074F) return "Syriac";
        if (codePoint <= 0x077F) return "Arabic Supplement";
        if (codePoint <= 0x08FF) return "Arabic Extended";
        if (codePoint <= 0x097F) return "Devanagari";
        if (codePoint <= 0x09FF) return "Bengali";
        if (codePoint <= 0x0A7F) return "Gurmukhi";
        if (codePoint <= 0x0AFF) return "Gujarati";
        if (codePoint <= 0x0B7F) return "Oriya";
        if (codePoint <= 0x0BFF) return "Tamil";
        if (codePoint <= 0x0C7F) return "Telugu";
        if (codePoint <= 0x0CFF) return "Kannada";
        if (codePoint <= 0x0D7F) return "Malayalam";
        if (codePoint <= 0x0DFF) return "Sinhala";
        if (codePoint <= 0x0E7F) return "Thai";
        if (codePoint <= 0x0EFF) return "Lao";
        if (codePoint <= 0x0FFF) return "Tibetan";
        if (codePoint <= 0x109F) return "Myanmar";
        if (codePoint <= 0x10FF) return "Georgian";
        if (codePoint <= 0x11FF) return "Hangul Jamo";
        if (codePoint <= 0x137F) return "Ethiopic";
        if (codePoint <= 0x13FF) return "Cherokee";
        if (codePoint <= 0x167F) return "Canadian Aboriginal Syllabics";
        if (codePoint <= 0x169F) return "Ogham";
        if (codePoint <= 0x16FF) return "Runic";
        if (codePoint <= 0x177F) return "Philippine Scripts";
        if (codePoint <= 0x17FF) return "Khmer";
        if (codePoint <= 0x18AF) return "Mongolian";
        if (codePoint <= 0x1DFF) return "Phonetic and Combining Extensions";
        if (codePoint <= 0x1EFF) return "Latin Extended Additional";
        if (codePoint <= 0x1FFF) return "Greek Extended";
        if (codePoint <= 0x206F) return "General Punctuation";
        if (codePoint <= 0x20CF) return "Currency and Combining Symbols";
        if (codePoint <= 0x214F) return "Letterlike Symbols";
        if (codePoint <= 0x218F) return "Number Forms";
        if (codePoint <= 0x21FF) return "Arrows";
        if (codePoint <= 0x22FF) return "Mathematical Operators";
        if (codePoint <= 0x23FF) return "Miscellaneous Technical";
        if (codePoint <= 0x24FF) return "Enclosed Alphanumerics";
        if (codePoint <= 0x25FF) return "Geometric Shapes";
        if (codePoint <= 0x26FF) return "Miscellaneous Symbols";
        if (codePoint <= 0x27BF) return "Dingbats";
        if (codePoint <= 0x2BFF) return "Misc Symbols and Arrows";
        if (codePoint <= 0x2C7F) return "Latin Extended-C";
        if (codePoint <= 0x2CFF) return "Coptic";
        if (codePoint <= 0x2D2F) return "Georgian Supplement";
        if (codePoint <= 0x2D7F) return "Tifinagh";
        if (codePoint <= 0x2DDF) return "Ethiopic Extended";
        if (codePoint <= 0x2DFF) return "Cyrillic Extended-A";
        if (codePoint <= 0x2E7F) return "Supplemental Punctuation";
        if (codePoint <= 0x2EFF) return "CJK Radicals Supplement";
        if (codePoint <= 0x2FDF) return "Kangxi Radicals";
        if (codePoint <= 0x2FFF) return "Ideographic Description Characters";
        if (codePoint <= 0x303F) return "CJK Symbols and Punctuation";
        if (codePoint <= 0x309F) return "Hiragana";
        if (codePoint <= 0x30FF) return "Katakana";
        if (codePoint <= 0x312F) return "Bopomofo";
        if (codePoint <= 0x318F) return "Hangul Compatibility Jamo";
        if (codePoint <= 0x31EF) return "CJK Strokes";
        if (codePoint <= 0x31FF) return "Katakana Phonetic Extensions";
        if (codePoint <= 0x32FF) return "Enclosed CJK Letters and Months";
        if (codePoint <= 0x33FF) return "CJK Compatibility";
        if (codePoint <= 0x4DBF) return "CJK Unified Ideographs Extension A";
        if (codePoint <= 0x4DFF) return "Yijing Hexagram Symbols";
        if (codePoint <= 0x9FFF) return "CJK Unified Ideographs";
        if (codePoint <= 0xA4CF) return "Yi Syllables and Radicals";
        if (codePoint <= 0xABFF) return "Latin Extended-E";
        if (codePoint <= 0xD7AF) return "Hangul Syllables";
        if (codePoint <= 0xF8FF) return "Private Use Area";
        if (codePoint <= 0xFAFF) return "CJK Compatibility Ideographs";
        if (codePoint <= 0xFE4F) return "CJK Compatibility Forms";
        if (codePoint <= 0xFE6F) return "Small Form Variants";
        if (codePoint <= 0xFEFF) return "Arabic Presentation Forms-B";
        if (codePoint <= 0xFFEF) return "Halfwidth and Fullwidth Forms";
        if (codePoint <= 0xFFFF) return "Specials";

        if (codePoint >= 0x1F300 && codePoint <= 0x1FAFF) {
            return "Emoji and Pictographs";
        }
        if (codePoint >= 0x20000 && codePoint <= 0x2A6DF) return "CJK Unified Ideographs Extension B";
        if (codePoint >= 0x2A700 && codePoint <= 0x2B73F) return "CJK Unified Ideographs Extension C";
        if (codePoint >= 0x2B740 && codePoint <= 0x2B81F) return "CJK Unified Ideographs Extension D";
        if (codePoint >= 0x2B820 && codePoint <= 0x2CEAF) return "CJK Unified Ideographs Extension E";
        if (codePoint >= 0x2CEB0 && codePoint <= 0x2EBEF) return "CJK Unified Ideographs Extension F";
        if (codePoint >= 0x2F800 && codePoint <= 0x2FA1F) return "CJK Compatibility Ideographs Supplement";
        if (codePoint >= 0x30000 && codePoint <= 0x3134F) return "CJK Unified Ideographs Extension G";

        var plane = codePoint >> 16;
        if (plane == 0x01) {
            return "Supplementary Multilingual Plane";
        }
        if (plane == 0x02) {
            return "Supplementary Ideographic Plane";
        }
        if (plane == 0x0E) {
            return "Supplementary Special-purpose Plane";
        }
        if (plane == 0x0F) {
            return "Supplementary Private Use Area-A";
        }
        if (plane == 0x10) {
            return "Supplementary Private Use Area-B";
        }
        return $"Supplementary Plane {plane:X}";
    }

    private static IEnumerable<(uint CodePoint, bool IsValid)> EnumerateCodePoints(string value) {
        if (string.IsNullOrEmpty(value)) {
            yield break;
        }

        for (var i = 0; i < value.Length; i++) {
            var ch = value[i];
            if (char.IsHighSurrogate(ch) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1])) {
                yield return ((uint)char.ConvertToUtf32(ch, value[i + 1]), true);
                i++;
            }
            else {
                var isValid = !char.IsSurrogate(ch);
                yield return ((uint)ch, isValid);
            }
        }
    }

    private static string DescribeRune(uint codePoint) {
        if (!Rune.TryCreate((int)codePoint, out var rune)) {
            return $"U+{codePoint:X4}";
        }

        if (codePoint < 0x20 || codePoint == 0x7F) {
            return $"U+{codePoint:X4} ({GetControlName(codePoint)})";
        }

        var display = rune.ToString();
        return $"U+{codePoint:X4} ({Markup.Escape(display)})";
    }

    private static string GetControlName(uint codePoint) {
        return codePoint switch {
            0 => "NUL",
            1 => "SOH",
            2 => "STX",
            3 => "ETX",
            4 => "EOT",
            5 => "ENQ",
            6 => "ACK",
            7 => "BEL",
            8 => "BS",
            9 => "TAB",
            10 => "LF",
            11 => "VT",
            12 => "FF",
            13 => "CR",
            14 => "SO",
            15 => "SI",
            16 => "DLE",
            17 => "DC1",
            18 => "DC2",
            19 => "DC3",
            20 => "DC4",
            21 => "NAK",
            22 => "SYN",
            23 => "ETB",
            24 => "CAN",
            25 => "EM",
            26 => "SUB",
            27 => "ESC",
            28 => "FS",
            29 => "GS",
            30 => "RS",
            31 => "US",
            127 => "DEL",
            _ => $"CTRL-{codePoint}",
        };
    }

    private static void WriteColumnReports(IEnumerable<ColumnStats> stats, IReportWriter writer) {
        var ordered = stats.OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase).ToList();

        var allNullColumns = new List<ColumnStats>();
        var columnsWithNbsp = new List<ColumnStats>();
        var columnsWithNarrowNbsp = new List<ColumnStats>();
        var columnsWithDirectionalMarks = new List<ColumnStats>();
        var columnsWithDaggers = new List<ColumnStats>();
        var columnsWithTrimIssues = new List<ColumnStats>();
        var columnsWithControlChars = new List<ColumnStats>();
        var columnsWithInvalidUnicode = new List<ColumnStats>();
        var columnsWithNormalizationIssues = new List<ColumnStats>();
        var columnsWithZeroWidthJoiners = new List<ColumnStats>();
        var columnsWithFixedWordCount = new List<ColumnStats>();
        var columnsWithAlmostFixedWordCount = new List<ColumnStats>();

        for (var index = 0; index < ordered.Count; index++) {
            var stat = ordered[index];

            var isAllNull = stat.TotalObserved > 0 && stat.NullCount == stat.TotalObserved;
            if (isAllNull) {
                allNullColumns.Add(stat);
            }
            if (stat.ContainsNoBreakSpace) {
                columnsWithNbsp.Add(stat);
            }
            if (stat.ContainsNarrowNoBreakSpace) {
                columnsWithNarrowNbsp.Add(stat);
            }
            if (stat.ContainsDirectionalMarks) {
                columnsWithDirectionalMarks.Add(stat);
            }
            if (stat.ContainsDagger) {
                columnsWithDaggers.Add(stat);
            }
            if (stat.TrimDifferenceCount > 0) {
                columnsWithTrimIssues.Add(stat);
            }
            if (stat.ControlCharCount > 0) {
                columnsWithControlChars.Add(stat);
            }
            if (stat.InvalidUnicodeCount > 0) {
                columnsWithInvalidUnicode.Add(stat);
            }
            if (stat.NotNfcCount > 0) {
                columnsWithNormalizationIssues.Add(stat);
            }
            if (stat.ContainsZeroWidthJoiner) {
                columnsWithZeroWidthJoiners.Add(stat);
            }

            if (stat.HasWordCountSamples) {
                if (stat.IsFixedWordCount) {
                    columnsWithFixedWordCount.Add(stat);
                }
                else if (stat.IsAlmostFixedWordCount) {
                    columnsWithAlmostFixedWordCount.Add(stat);
                }
            }

            if (index > 0) {
                writer.WriteLine(string.Empty);
            }

            writer.WriteLine($"[bold]{Markup.Escape(stat.Name)}[/] ({Markup.Escape(stat.DeclaredType ?? string.Empty)})");

            if (isAllNull) {
                writer.WriteLine("  All rows are null.");
                continue;
            }

            var nonNull = stat.TotalObserved - stat.NullCount;
            var nonEmpty = nonNull - stat.EmptyCount;
            var avgLength = nonNull > 0 ? stat.SumLength / (double)nonNull : 0d;
            var minLength = stat.MinLength == int.MaxValue ? 0 : stat.MinLength;

            writer.WriteLine($"  Rows observed: {stat.TotalObserved:N0}");
            writer.WriteLine($"  Null: {FormatCount(stat.NullCount, stat.TotalObserved)}");
            writer.WriteLine($"  Empty strings: {FormatCount(stat.EmptyCount, stat.TotalObserved)}");

            if (stat.TrimDifferenceCount > 0) {
                writer.WriteLine("  Leading/trailing whitespace:");
                writer.WriteLine($"    Any difference: {FormatCount(stat.TrimDifferenceCount, stat.TotalObserved)}");
                writer.WriteLine($"    Leading whitespace: {FormatCount(stat.LeadingWhitespaceCount, stat.TotalObserved)}");
                writer.WriteLine($"    Trailing whitespace: {FormatCount(stat.TrailingWhitespaceCount, stat.TotalObserved)}");
            }
            else {
                writer.WriteLine("  Leading/trailing whitespace: -");
            }

            writer.WriteLine($"  Length (min / max / avg): {(nonNull > 0 ? $"{minLength}/{stat.MaxLength}/{avgLength:F1}" : "-")}");

            if (stat.HasWordCountSamples) {
                var minWordCount = stat.MinWordCount == int.MaxValue ? 0 : stat.MinWordCount;
                writer.WriteLine($"  Word count (min / max / avg / stdev): {minWordCount}/{stat.MaxWordCount}/{stat.WordCountAverage:F2}/{stat.WordCountStandardDeviation:F2}");
                if (stat.MostCommonWordCountFrequency > 0) {
                    writer.WriteLine($"  Word count mode: {stat.MostCommonWordCount} ({FormatCount(stat.MostCommonWordCountFrequency, stat.WordCountSampleCount)})");
                }
            }
            else {
                writer.WriteLine("  Word count (min / max / avg / stdev): -");
            }

            var wordPositionLabels = new[] { "First", "Second", "Third", "Fourth" };
            var wordCasingLines = new List<string>();
            for (var wordPosition = 0; wordPosition < ColumnStats.WordCasePositions && wordPosition < wordPositionLabels.Length; wordPosition++) {
                var summary = stat.GetWordCaseSummary(wordPosition);
                if (summary.Total == 0) {
                    continue;
                }

                var parts = summary.Breakdown
                    .Where(item => item.Count > 0)
                    .Select(item => $"{item.Category} {FormatCount(item.Count, summary.Total)}")
                    .ToArray();

                var lineContent = parts.Length > 0 ? string.Join(", ", parts) : "-";
                wordCasingLines.Add($"    {wordPositionLabels[wordPosition]}: {lineContent}");
            }

            if (wordCasingLines.Count > 0) {
                writer.WriteLine("  Word casing (first four words):");
                foreach (var casingLine in wordCasingLines) {
                    writer.WriteLine(casingLine);
                }
            }

            if (nonEmpty > 0) {
                if (stat.NotNfcCount == 0) {
                    writer.WriteLine("  Unicode normalization: all observed values are NFC");
                }
                else {
                    var normalizationLine = $"  Unicode normalization: {FormatCount(stat.NotNfcCount, nonEmpty)} not NFC";
                    if (stat.NfdLikelyCount > 0) {
                        normalizationLine += $", {FormatCount(stat.NfdLikelyCount, nonEmpty)} look like NFD";
                    }
                    writer.WriteLine(normalizationLine);
                }
            }

            var specials = new List<string>();
            if (stat.ContainsNoBreakSpace) {
                specials.Add("NBSP (U+00A0)");
            }
            if (stat.ContainsNarrowNoBreakSpace) {
                specials.Add("Narrow NBSP (U+202F)");
            }
            if (stat.ContainsDirectionalMarks) {
                specials.Add("Directional mark (e.g. U+200E/U+200F)");
            }
            if (stat.ContainsZeroWidthJoiner) {
                specials.Add("Zero-width joiner/non-joiner (U+200C/U+200D)");
            }
            if (stat.ContainsDagger) {
                specials.Add("Dagger (U+2020)");
            }
            if (specials.Count > 0) {
                writer.WriteLine("  Notable characters: " + string.Join(", ", specials));
            }

            writer.WriteLine($"  Rows with non-ASCII: {FormatCount(stat.NonAsciiCount, stat.TotalObserved)} (distinct chars: {stat.NonAsciiDistinctCount:N0})");
            writer.WriteLine($"  Rows with control characters: {FormatCount(stat.ControlCharCount, stat.TotalObserved)} (distinct chars: {stat.ControlDistinctCount:N0})");
            writer.WriteLine($"  Rows with invalid Unicode: {FormatCount(stat.InvalidUnicodeCount, stat.TotalObserved)} (distinct code points: {stat.InvalidUnicodeDistinctCount:N0})");
            writer.WriteLine($"  Rows with punctuation: {FormatCount(stat.PunctuationCount, stat.TotalObserved)}");

            if (stat.PunctuationAsciiDistinctCount > 0) {
                writer.WriteLine("    ASCII punctuation: " + FormatRuneSamples(stat.PunctuationAsciiDistinctCharacters, stat.MaxSampleCount));
            }
            if (stat.PunctuationUnicodeDistinctCount > 0) {
                writer.WriteLine("    Unicode punctuation: " + FormatRuneSamples(stat.PunctuationUnicodeDistinctCharacters, stat.MaxSampleCount));
            }

            if (stat.NonAsciiDistinctCount > 0) {
                writer.WriteLine("  Non-ASCII samples: " + FormatRuneSamples(stat.NonAsciiDistinctCharacters, stat.MaxSampleCount));
            }

            if (stat.ControlDistinctCount > 0) {
                writer.WriteLine("  Control character samples: " + FormatRuneSamples(stat.ControlDistinctCharacters, stat.MaxSampleCount));
            }

            if (stat.InvalidUnicodeDistinctCount > 0) {
                writer.WriteLine("  Invalid code point samples: " + FormatRuneSamples(stat.InvalidUnicodeDistinctCharacters, stat.MaxSampleCount));
            }

            var blocks = stat.UnicodeBlocks
                .OrderByDescending(b => b.RowCount)
                .ThenBy(b => b.Name, StringComparer.Ordinal)
                .ToList();
            if (blocks.Count > 0) {
                writer.WriteLine("  Unicode blocks:");
                foreach (var block in blocks) {
                    writer.WriteLine($"    {block.Name}: rows {block.RowCount:N0}, chars {block.CharacterCount:N0}, distinct {block.DistinctCount:N0}");
                    var samples = FormatRuneSamples(block.DistinctCharacters, stat.MaxSampleCount);
                    if (!string.Equals(samples, "-", StringComparison.Ordinal)) {
                        writer.WriteLine("      Samples: " + samples);
                    }
                }
            }

            if (!string.IsNullOrEmpty(stat.ExampleNonAscii)) {
                writer.WriteLine("  Example non-ASCII value:");
                writer.WriteLine("    " + Markup.Escape(stat.ExampleNonAscii));
            }

            if (!string.IsNullOrEmpty(stat.ExampleInvalidUnicode)) {
                writer.WriteLine("  Example invalid-Unicode value:");
                writer.WriteLine("    " + Markup.Escape(ColumnStats.FormatInvalidSample(stat.ExampleInvalidUnicode)));
            }

            if (!string.IsNullOrEmpty(stat.ExampleTrimDifference)) {
                writer.WriteLine("  Example with surrounding whitespace:");
                writer.WriteLine("    " + Markup.Escape(stat.ExampleTrimDifference));
            }
        }

        if (allNullColumns.Count == 0 && columnsWithNbsp.Count == 0 && columnsWithNarrowNbsp.Count == 0 && columnsWithDirectionalMarks.Count == 0 && columnsWithDaggers.Count == 0 && columnsWithTrimIssues.Count == 0 && columnsWithControlChars.Count == 0 && columnsWithInvalidUnicode.Count == 0 && columnsWithNormalizationIssues.Count == 0 && columnsWithZeroWidthJoiners.Count == 0 && columnsWithFixedWordCount.Count == 0 && columnsWithAlmostFixedWordCount.Count == 0) {
            return;
        }

        writer.WriteLine(string.Empty);
        writer.WriteLine("[bold]Summary[/]");

        PrintSummaryList("All-null columns", allNullColumns, writer);
        PrintSummaryList("Columns containing NBSP", columnsWithNbsp, writer);
        PrintSummaryList("Columns containing narrow NBSP", columnsWithNarrowNbsp, writer);
        PrintSummaryList("Columns containing directional marks", columnsWithDirectionalMarks, writer);
        PrintSummaryList("Columns containing daggers", columnsWithDaggers, writer);
        PrintSummaryList("Columns with untrimmed values", columnsWithTrimIssues, writer);
        PrintSummaryList("Columns with control characters", columnsWithControlChars, writer);
        PrintSummaryList("Columns with invalid Unicode", columnsWithInvalidUnicode, writer);
        PrintSummaryList("Columns with zero-width joiners/non-joiners", columnsWithZeroWidthJoiners, writer);
        PrintSummaryList("Columns with non-NFC text", columnsWithNormalizationIssues, writer);
        PrintSummaryList("Columns with fixed word count", columnsWithFixedWordCount, writer, stat => FormatWordCountSummary(stat, stat.MinWordCount));
        var almostFixedPct = ColumnStats.AlmostFixedWordCountThreshold * 100d;
        PrintSummaryList($"Columns with almost fixed word count (≥ {almostFixedPct:F0}% same)", columnsWithAlmostFixedWordCount, writer, stat => FormatWordCountSummary(stat, stat.MostCommonWordCount));

        static string FormatWordCountSummary(ColumnStats stat, int typicalWordCount) {
            if (typicalWordCount <= 0 || typicalWordCount == int.MaxValue) {
                return Markup.Escape(stat.Name);
            }

            //string label = typicalWordCount == 1 ? " word" : " words";
            string label = "";
            return $"{Markup.Escape(stat.Name)} ({typicalWordCount:N0}{label})";
        }

        static void PrintSummaryList(string title, IReadOnlyCollection<ColumnStats> items, IReportWriter writer, Func<ColumnStats, string>? formatter = null) {
            if (items.Count == 0) {
                return;
            }

            var names = string.Join(", ", items.Select(s => formatter?.Invoke(s) ?? Markup.Escape(s.Name)));
            writer.WriteLine($"  {title}: {names}");
        }
    }

    private sealed class ColumnStats {
        private readonly int _maxCharSamples;
        private readonly HashSet<uint> _nonAsciiDistinct;
        private readonly HashSet<uint> _controlDistinct;
        private readonly HashSet<uint> _punctuationAsciiDistinct;
        private readonly HashSet<uint> _punctuationUnicodeDistinct;
        private readonly HashSet<uint> _invalidUnicodeDistinct;
        private readonly Dictionary<string, UnicodeBlockInfo> _unicodeBlocks;

        public ColumnStats(string name, string declaredType, int maxCharSamples) {
            Name = name;
            DeclaredType = declaredType;
            _maxCharSamples = Math.Max(1, maxCharSamples);
            _nonAsciiDistinct = new HashSet<uint>();
            _controlDistinct = new HashSet<uint>();
            _punctuationAsciiDistinct = new HashSet<uint>();
            _punctuationUnicodeDistinct = new HashSet<uint>();
            _invalidUnicodeDistinct = new HashSet<uint>();
            _unicodeBlocks = new Dictionary<string, UnicodeBlockInfo>(StringComparer.Ordinal);
        }

        public string Name { get; }
        public string DeclaredType { get; }
        public long TotalObserved { get; private set; }
        public long NullCount { get; private set; }
        public long EmptyCount { get; private set; }
        public long TrimDifferenceCount { get; private set; }
        public long LeadingWhitespaceCount { get; private set; }
        public long TrailingWhitespaceCount { get; private set; }
        public long NonAsciiCount { get; private set; }
        public long ControlCharCount { get; private set; }
        public long InvalidUnicodeCount { get; private set; }
        public long PunctuationCount { get; private set; }
        public long SumLength { get; private set; }
        public int MinLength { get; private set; } = int.MaxValue;
        public int MaxLength { get; private set; }
        private const int WordPositionsTracked = 4;
        private const double WordCountAlmostFixedThreshold = 0.95d;
        private static readonly WordCaseCategory[] WordCaseCategoryOrder =
        {
            WordCaseCategory.None,
            WordCaseCategory.AllCaps,
            WordCaseCategory.Title,
            WordCaseCategory.Lower,
            WordCaseCategory.TitleMixed,
            WordCaseCategory.LowerMixed,
            WordCaseCategory.Unicameral
        };

        private readonly long[,] _wordCaseCounts = new long[WordPositionsTracked, WordCaseCategoryOrder.Length];
        private readonly Dictionary<int, long> _wordCountHistogram = new();

        public long NotNfcCount { get; private set; }
        public long NfdLikelyCount { get; private set; }
        public bool ContainsNoBreakSpace { get; private set; }
        public bool ContainsNarrowNoBreakSpace { get; private set; }
        public bool ContainsDirectionalMarks { get; private set; }
        public bool ContainsDagger { get; private set; }
        public bool ContainsZeroWidthJoiner { get; private set; }
        public int MaxSampleCount => _maxCharSamples;
        public IReadOnlyCollection<uint> NonAsciiDistinctCharacters => _nonAsciiDistinct;
        public int NonAsciiDistinctCount => _nonAsciiDistinct.Count;
        public IReadOnlyCollection<uint> ControlDistinctCharacters => _controlDistinct;
        public int ControlDistinctCount => _controlDistinct.Count;
        public IReadOnlyCollection<uint> PunctuationAsciiDistinctCharacters => _punctuationAsciiDistinct;
        public int PunctuationAsciiDistinctCount => _punctuationAsciiDistinct.Count;
        public IReadOnlyCollection<uint> PunctuationUnicodeDistinctCharacters => _punctuationUnicodeDistinct;
        public int PunctuationUnicodeDistinctCount => _punctuationUnicodeDistinct.Count;
        public IReadOnlyCollection<uint> InvalidUnicodeDistinctCharacters => _invalidUnicodeDistinct;
        public int InvalidUnicodeDistinctCount => _invalidUnicodeDistinct.Count;
        public IEnumerable<UnicodeBlockSnapshot> UnicodeBlocks => _unicodeBlocks.Values.Select(info => info.CreateSnapshot());
        public string? ExampleNonAscii { get; private set; }
        public string? ExampleTrimDifference { get; private set; }
        public string? ExampleInvalidUnicode { get; private set; }
        public long WordCountSampleCount { get; private set; }
        public long WordCountSum { get; private set; }
        public double WordCountSumSquares { get; private set; }
        public int MinWordCount { get; private set; } = int.MaxValue;
        public int MaxWordCount { get; private set; }
        public int MostCommonWordCount { get; private set; } = -1;
        public long MostCommonWordCountFrequency { get; private set; }
        public bool HasWordCountSamples => WordCountSampleCount > 0;
        public double WordCountAverage => HasWordCountSamples ? WordCountSum / (double)WordCountSampleCount : 0d;
        public double WordCountStandardDeviation {
            get {
                if (!HasWordCountSamples) {
                    return 0d;
                }

                var mean = WordCountAverage;
                var variance = Math.Max(0d, (WordCountSumSquares / WordCountSampleCount) - (mean * mean));
                return Math.Sqrt(variance);
            }
        }
        public bool IsFixedWordCount => HasWordCountSamples && MinWordCount == MaxWordCount;
        public bool IsAlmostFixedWordCount => HasWordCountSamples && !IsFixedWordCount && MostCommonWordCountFrequency >= WordCountAlmostFixedThreshold * WordCountSampleCount;
        public static int WordCasePositions => WordPositionsTracked;
        public static double AlmostFixedWordCountThreshold => WordCountAlmostFixedThreshold;

        public void RegisterNull() {
            TotalObserved++;
            NullCount++;
        }

        public void RegisterValue(string? value) {
            TotalObserved++;
            if (value is null) {
                NullCount++;
                return;
            }

            WordCountSampleCount++;

            var wordCount = AnalyzeWordData(value);
            WordCountSum += wordCount;
            WordCountSumSquares += (double)wordCount * wordCount;
            if (wordCount < MinWordCount) {
                MinWordCount = wordCount;
            }
            if (wordCount > MaxWordCount) {
                MaxWordCount = wordCount;
            }

            var updatedFrequency = _wordCountHistogram.TryGetValue(wordCount, out var existingFrequency)
                ? existingFrequency + 1
                : 1;
            _wordCountHistogram[wordCount] = updatedFrequency;
            if (updatedFrequency > MostCommonWordCountFrequency ||
                (updatedFrequency == MostCommonWordCountFrequency && (MostCommonWordCount == -1 || wordCount < MostCommonWordCount))) {
                MostCommonWordCount = wordCount;
                MostCommonWordCountFrequency = updatedFrequency;
            }

            var length = value.Length;

            SumLength += length;
            if (length < MinLength) {
                MinLength = length;
            }
            if (length > MaxLength) {
                MaxLength = length;
            }

            if (length == 0) {
                EmptyCount++;
                return;
            }

            CheckNormalization(value);

            var leadingWhitespace = char.IsWhiteSpace(value[0]);
            var trailingWhitespace = char.IsWhiteSpace(value[^1]);
            if (leadingWhitespace || trailingWhitespace) {
                TrimDifferenceCount++;
                if (leadingWhitespace) {
                    LeadingWhitespaceCount++;
                }
                if (trailingWhitespace) {
                    TrailingWhitespaceCount++;
                }
                ExampleTrimDifference ??= Shorten(value);
            }

            var hasNonAscii = false;
            var hasControl = false;
            var sawPunctuation = false;
            var sawNbsp = false;
            var sawNarrowNbsp = false;
            var sawDirectionalMark = false;
            var sawDagger = false;
            var sawZeroWidthJoiner = false;
            var blocksSeen = new HashSet<string>(StringComparer.Ordinal);

            var hasInvalid = false;
            foreach (var cp in EnumerateCodePoints(value)) {
                var code = cp.CodePoint;
                var isValid = cp.IsValid;

                if (!isValid) {
                    hasInvalid = true;
                    _invalidUnicodeDistinct.Add(code);
                    continue;
                }

                if (code > 0x7F) {
                    hasNonAscii = true;
                    _nonAsciiDistinct.Add(code);
                }

                if (code < 0x20 || code == 0x7F) {
                    hasControl = true;
                    _controlDistinct.Add(code);
                }

                if (!Rune.TryCreate((int)code, out var rune)) {
                    continue;
                }
                var category = Rune.GetUnicodeCategory(rune);

                if (IsPunctuationCategory(category)) {
                    sawPunctuation = true;
                    if (code <= 0x7F) {
                        _punctuationAsciiDistinct.Add(code);
                    }
                    else {
                        _punctuationUnicodeDistinct.Add(code);
                    }
                }

                var blockName = GetUnicodeBlock(code);
                if (blockName is not null) {
                    if (!_unicodeBlocks.TryGetValue(blockName, out var block)) {
                        block = new UnicodeBlockInfo(blockName);
                        _unicodeBlocks[blockName] = block;
                    }
                    block.AddCharacter(code);
                    blocksSeen.Add(blockName);
                }

                if (!sawNbsp && code == 0x00A0) {
                    sawNbsp = true;
                }
                else if (!sawNarrowNbsp && code == 0x202F) {
                    sawNarrowNbsp = true;
                }

                if (!sawDirectionalMark && IsDirectionalMark(code)) {
                    sawDirectionalMark = true;
                }

                if (!sawDagger && (code == 0x2020 || code == 0x2021)) {
                    sawDagger = true;
                }

                if (!sawZeroWidthJoiner && (code == 0x200C || code == 0x200D)) {
                    sawZeroWidthJoiner = true;
                }
            }

            if (hasInvalid) {
                InvalidUnicodeCount++;
                ExampleInvalidUnicode ??= Shorten(value);
            }

            foreach (var blockName in blocksSeen) {
                _unicodeBlocks[blockName].IncrementRow();
            }

            if (hasNonAscii) {
                NonAsciiCount++;
                ExampleNonAscii ??= Shorten(value);
            }

            if (hasControl) {
                ControlCharCount++;
            }

            if (sawPunctuation) {
                PunctuationCount++;
            }

            if (sawNbsp) {
                ContainsNoBreakSpace = true;
            }

            if (sawNarrowNbsp) {
                ContainsNarrowNoBreakSpace = true;
            }

            if (sawDirectionalMark) {
                ContainsDirectionalMarks = true;
            }

            if (sawDagger) {
                ContainsDagger = true;
            }

            if (sawZeroWidthJoiner) {
                ContainsZeroWidthJoiner = true;
            }
        }

        private static string Shorten(string input, int maxLength = 160) {
            if (input.Length <= maxLength) {
                return input;
            }
            return input.Substring(0, maxLength) + "…";
        }

        internal static string FormatInvalidSample(string value) {
            if (string.IsNullOrEmpty(value)) {
                return string.Empty;
            }

            var builder = new StringBuilder(value.Length);
            foreach (var (codePoint, isValid) in EnumerateCodePoints(value)) {
                if (isValid && Rune.TryCreate((int)codePoint, out var rune)) {
                    builder.Append(rune.ToString());
                }
                else {
                    builder.Append($"<U+{codePoint:X4}>");
                }
            }

            return builder.ToString();
        }

        private int AnalyzeWordData(string value) {
            var span = value.AsSpan();
            var length = span.Length;
            var index = 0;
            var wordIndex = 0;

            while (index < length) {
                while (index < length && char.IsWhiteSpace(span[index])) {
                    index++;
                }

                if (index >= length) {
                    break;
                }

                var start = index;
                while (index < length && !char.IsWhiteSpace(span[index])) {
                    index++;
                }

                if (wordIndex < WordPositionsTracked) {
                    var wordSpan = span.Slice(start, index - start);
                    var category = CategorizeWord(wordSpan);
                    _wordCaseCounts[wordIndex, (int)category]++;
                }

                wordIndex++;
            }

            for (var position = wordIndex; position < WordPositionsTracked; position++) {
                _wordCaseCounts[position, (int)WordCaseCategory.None]++;
            }

            return wordIndex;
        }

        private static WordCaseCategory CategorizeWord(ReadOnlySpan<char> word) {
            Rune? firstLetter = null;
            var firstIsUpper = false;
            var firstIsLower = false;
            var firstIsTitle = false;

            var sawLetter = false;
            var sawUpper = false;
            var sawLower = false;
            var sawTitle = false;
            var restHasUpper = false;
            var restHasLower = false;
            var restHasTitle = false;

            foreach (var (codePoint, isValid) in EnumerateCodePoints(word.ToString())) {
                if (!isValid || !Rune.TryCreate((int)codePoint, out var rune)) {
                    continue;
                }

                if (!Rune.IsLetter(rune)) {
                    continue;
                }

                var category = Rune.GetUnicodeCategory(rune);
                var isUpper = category == UnicodeCategory.UppercaseLetter;
                var isLower = category == UnicodeCategory.LowercaseLetter;
                var isTitle = category == UnicodeCategory.TitlecaseLetter;

                if (firstLetter is null) {
                    firstLetter = rune;
                    firstIsUpper = isUpper;
                    firstIsLower = isLower;
                    firstIsTitle = isTitle;
                }
                else {
                    restHasUpper |= isUpper;
                    restHasLower |= isLower;
                    restHasTitle |= isTitle;
                }

                sawLetter = true;
                sawUpper |= isUpper;
                sawLower |= isLower;
                sawTitle |= isTitle;
            }

            if (!sawLetter) {
                return WordCaseCategory.Unicameral;
            }

            if (!sawUpper && !sawLower && !sawTitle) {
                return WordCaseCategory.Unicameral;
            }

            if (sawUpper && !sawLower) {
                return WordCaseCategory.AllCaps;
            }

            if (!sawUpper && sawLower && !sawTitle) {
                return WordCaseCategory.Lower;
            }

            var firstUpperOrTitle = firstIsUpper || firstIsTitle;

            if (firstUpperOrTitle) {
                if (!restHasUpper && !restHasTitle) {
                    return WordCaseCategory.Title;
                }

                return WordCaseCategory.TitleMixed;
            }

            if (firstIsLower) {
                if (restHasUpper || restHasTitle) {
                    return WordCaseCategory.LowerMixed;
                }

                return WordCaseCategory.Lower;
            }

            return WordCaseCategory.Unicameral;
        }

        private void CheckNormalization(string value) {
            if (value.Length == 0) {
                return;
            }

            try {
                if (!value.IsNormalized(NormalizationForm.FormC)) {
                    NotNfcCount++;
                    try {
                        if (value.IsNormalized(NormalizationForm.FormD)) {
                            NfdLikelyCount++;
                        }
                    }
                    catch (ArgumentException) {
                        // Ignore invalid sequences when probing for FormD.
                    }
                }
            }
            catch (ArgumentException) {
                NotNfcCount++;
            }
        }

        private static bool IsDirectionalMark(uint codePoint) {
            return codePoint is 0x200E or 0x200F or >= 0x202A and <= 0x202E or >= 0x2066 and <= 0x2069;
        }

        public WordCaseSummary GetWordCaseSummary(int position) {
            var breakdown = new List<WordCaseCount>(WordCaseCategoryOrder.Length);
            long total = 0;

            foreach (var category in WordCaseCategoryOrder) {
                var count = _wordCaseCounts[position, (int)category];
                breakdown.Add(new WordCaseCount(GetWordCaseCategoryLabel(category), count));
                total += count;
            }

            return new WordCaseSummary(total, breakdown);
        }

        private static string GetWordCaseCategoryLabel(WordCaseCategory category) {
            return category switch {
                WordCaseCategory.None => "none",
                WordCaseCategory.AllCaps => "allcaps",
                WordCaseCategory.Title => "title",
                WordCaseCategory.Lower => "lower",
                WordCaseCategory.TitleMixed => "title-mixed",
                WordCaseCategory.LowerMixed => "lower-mixed",
                WordCaseCategory.Unicameral => "unicameral",
                _ => category.ToString().ToLowerInvariant()
            };
        }

        public sealed record WordCaseCount(string Category, long Count);

        public sealed record WordCaseSummary(long Total, IReadOnlyList<WordCaseCount> Breakdown);

        private enum WordCaseCategory {
            None,
            AllCaps,
            Title,
            Lower,
            TitleMixed,
            LowerMixed,
            Unicameral
        }

        public sealed record UnicodeBlockSnapshot(string Name, long RowCount, long CharacterCount, IReadOnlyCollection<uint> DistinctCharacters, int DistinctCount);

        private sealed class UnicodeBlockInfo {
            private readonly HashSet<uint> _distinctCharacters = new();

            public UnicodeBlockInfo(string name) {
                Name = name;
            }

            public string Name { get; }
            public long RowCount { get; private set; }
            public long CharacterCount { get; private set; }

            public void AddCharacter(uint codePoint) {
                CharacterCount++;
                _distinctCharacters.Add(codePoint);
            }

            public void IncrementRow() {
                RowCount++;
            }

            public UnicodeBlockSnapshot CreateSnapshot() {
                return new UnicodeBlockSnapshot(Name, RowCount, CharacterCount, Array.AsReadOnly(_distinctCharacters.ToArray()), _distinctCharacters.Count);
            }
        }
    }
}
