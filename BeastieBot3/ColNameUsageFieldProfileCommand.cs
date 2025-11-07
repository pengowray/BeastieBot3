using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class ColNameUsageFieldProfileCommand : Command<ColNameUsageFieldProfileCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }

        [CommandOption("--table <NAME>")]
        public string Table { get; init; } = "nameusage";

        [CommandOption("--columns <COLUMNS>")]
        public string? Columns { get; init; }

        [CommandOption("--all-columns")]
        [Description("Include non-text columns when calculating field statistics.")]
        public bool IncludeNonText { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Maximum number of rows to scan (0 = entire table).")]
        public long Limit { get; init; }

        [CommandOption("--char-samples <COUNT>")]
        [Description("Maximum distinct non-ASCII/control characters to list per column.")]
        public int MaxCharSamples { get; init; } = 24;
    }

    private sealed record ColumnInfo(string Name, string DeclaredType, bool TreatAsText);

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        var dbPath = paths.GetColSqlitePath();
        if (string.IsNullOrWhiteSpace(dbPath)) {
            AnsiConsole.MarkupLine("[red]COL_sqlite path is not configured. Set [bold]Datastore:COL_sqlite[/] in paths.ini.[/]");
            return -1;
        }

        if (!File.Exists(dbPath)) {
            AnsiConsole.MarkupLine($"[red]COL SQLite database not found at:[/] {Markup.Escape(dbPath)}");
            return -2;
        }

        var connectionString = new SqliteConnectionStringBuilder {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        if (!TableExists(connection, settings.Table)) {
            AnsiConsole.MarkupLine($"[red]Table {Markup.Escape(settings.Table)} not found in the database.[/]");
            return -3;
        }

        var requestedColumns = ParseColumns(settings.Columns);
        var availableColumns = GetColumns(connection, settings.Table, settings.IncludeNonText);
        var selectedColumns = SelectColumns(availableColumns, requestedColumns);

        if (selectedColumns.Count == 0) {
            AnsiConsole.MarkupLine("[yellow]No columns selected for profiling.[/]");
            return 0;
        }

        var quotedTableName = QuoteIdentifier(settings.Table);
        var columnNamesSql = string.Join(", ", selectedColumns.Select(c => QuoteIdentifier(c.Name)));
        var limitClause = settings.Limit > 0 ? $" LIMIT {settings.Limit}" : string.Empty;

        var stats = selectedColumns.ToDictionary(
            c => c.Name,
            c => new ColumnStats(c.Name, c.DeclaredType, settings.MaxCharSamples)
        );

        var totalRowCount = GetRowCount(connection, settings.Table);
        var targetRowCount = settings.Limit > 0 && settings.Limit < totalRowCount ? settings.Limit : totalRowCount;

        AnsiConsole.MarkupLine($"[grey]Using settings from:[/] {Markup.Escape(paths.SourceFilePath)}");
        AnsiConsole.MarkupLine($"[grey]Reading Catalogue of Life data from:[/] {Markup.Escape(dbPath)}");
        AnsiConsole.MarkupLine($"[grey]Profiling table:[/] {Markup.Escape(settings.Table)}");
        AnsiConsole.MarkupLine($"[grey]Columns analysed:[/] {selectedColumns.Count} (text-only: {(!settings.IncludeNonText).ToString().ToLowerInvariant()})");
        AnsiConsole.MarkupLine($"[grey]Rows scheduled for scan:[/] {targetRowCount:N0}{(settings.Limit > 0 && settings.Limit < totalRowCount ? $" (limit {settings.Limit:N0})" : string.Empty)}");

        var selectSql = $"SELECT {columnNamesSql} FROM {quotedTableName}{limitClause}";

        using var command = connection.CreateCommand();
        command.CommandText = selectSql;
        command.CommandTimeout = 0;

        long processedRows = 0;
        AnsiConsole.Status()
            .Spinner(Spinner.Known.Dots)
            .Start("Scanning rows...", ctx => {
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
                        } catch (InvalidCastException) {
                            value = Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture);
                        }

                        columnStats.RegisterValue(value);
                    }

                    if (processedRows % 200_000 == 0) {
                        ctx.Status($"Scanning rows... {processedRows:N0}/{targetRowCount:N0}");
                    }
                }
            });

            AnsiConsole.MarkupLine($"[green]Completed scan of {processedRows:N0} row(s).[/]");

            WriteColumnReports(stats.Values);

        return 0;
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

    private static void WriteColumnReports(IEnumerable<ColumnStats> stats) {
        var ordered = stats.OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase).ToList();
        for (var index = 0; index < ordered.Count; index++) {
            var stat = ordered[index];
            var nonNull = stat.TotalObserved - stat.NullCount;
            var avgLength = nonNull > 0 ? stat.SumLength / (double)nonNull : 0d;
            var minLength = stat.MinLength == int.MaxValue ? 0 : stat.MinLength;

            if (index > 0) {
                AnsiConsole.MarkupLine(string.Empty);
            }

            AnsiConsole.MarkupLine($"[bold]{Markup.Escape(stat.Name)}[/] ({Markup.Escape(stat.DeclaredType ?? string.Empty)})");
            AnsiConsole.MarkupLine($"  Rows observed: {stat.TotalObserved:N0}");
            AnsiConsole.MarkupLine($"  Null: {FormatCount(stat.NullCount, stat.TotalObserved)}");
            AnsiConsole.MarkupLine($"  Empty strings: {FormatCount(stat.EmptyCount, stat.TotalObserved)}");

            if (stat.TrimDifferenceCount > 0) {
                AnsiConsole.MarkupLine("  Leading/trailing whitespace:");
                AnsiConsole.MarkupLine($"    Any difference: {FormatCount(stat.TrimDifferenceCount, stat.TotalObserved)}");
                AnsiConsole.MarkupLine($"    Leading whitespace: {FormatCount(stat.LeadingWhitespaceCount, stat.TotalObserved)}");
                AnsiConsole.MarkupLine($"    Trailing whitespace: {FormatCount(stat.TrailingWhitespaceCount, stat.TotalObserved)}");
            } else {
                AnsiConsole.MarkupLine("  Leading/trailing whitespace: -");
            }

            AnsiConsole.MarkupLine($"  Length (min / max / avg): {(nonNull > 0 ? $"{minLength}/{stat.MaxLength}/{avgLength:F1}" : "-")}");
            AnsiConsole.MarkupLine($"  Rows with non-ASCII: {FormatCount(stat.NonAsciiCount, stat.TotalObserved)} (distinct chars: {stat.NonAsciiDistinctCount:N0})");
            AnsiConsole.MarkupLine($"  Rows with control characters: {FormatCount(stat.ControlCharCount, stat.TotalObserved)} (distinct chars: {stat.ControlDistinctCount:N0})");
            AnsiConsole.MarkupLine($"  Rows with punctuation: {FormatCount(stat.PunctuationCount, stat.TotalObserved)}");

            if (stat.PunctuationAsciiDistinctCount > 0) {
                AnsiConsole.MarkupLine("    ASCII punctuation: " + FormatRuneSamples(stat.PunctuationAsciiDistinctCharacters, stat.MaxSampleCount));
            }
            if (stat.PunctuationUnicodeDistinctCount > 0) {
                AnsiConsole.MarkupLine("    Unicode punctuation: " + FormatRuneSamples(stat.PunctuationUnicodeDistinctCharacters, stat.MaxSampleCount));
            }

            if (stat.NonAsciiDistinctCount > 0) {
                AnsiConsole.MarkupLine("  Non-ASCII samples: " + FormatRuneSamples(stat.NonAsciiDistinctCharacters, stat.MaxSampleCount));
            }

            if (stat.ControlDistinctCount > 0) {
                AnsiConsole.MarkupLine("  Control character samples: " + FormatRuneSamples(stat.ControlDistinctCharacters, stat.MaxSampleCount));
            }

            var blocks = stat.UnicodeBlocks
                .OrderByDescending(b => b.RowCount)
                .ThenBy(b => b.Name, StringComparer.Ordinal)
                .ToList();
            if (blocks.Count > 0) {
                AnsiConsole.MarkupLine("  Unicode blocks:");
                foreach (var block in blocks) {
                    AnsiConsole.MarkupLine($"    {block.Name}: rows {block.RowCount:N0}, chars {block.CharacterCount:N0}, distinct {block.DistinctCount:N0}");
                    var samples = FormatRuneSamples(block.DistinctCharacters, stat.MaxSampleCount);
                    if (!string.Equals(samples, "-", StringComparison.Ordinal)) {
                        AnsiConsole.MarkupLine("      Samples: " + samples);
                    }
                }
            }

            if (!string.IsNullOrEmpty(stat.ExampleNonAscii)) {
                AnsiConsole.MarkupLine("  Example non-ASCII value:");
                AnsiConsole.MarkupLine("    " + Markup.Escape(stat.ExampleNonAscii));
            }

            if (!string.IsNullOrEmpty(stat.ExampleTrimDifference)) {
                AnsiConsole.MarkupLine("  Example with surrounding whitespace:");
                AnsiConsole.MarkupLine("    " + Markup.Escape(stat.ExampleTrimDifference));
            }
        }
    }

    private sealed class ColumnStats {
        private readonly int _maxCharSamples;
        private readonly HashSet<uint> _nonAsciiDistinct;
        private readonly HashSet<uint> _controlDistinct;
        private readonly HashSet<uint> _punctuationAsciiDistinct;
        private readonly HashSet<uint> _punctuationUnicodeDistinct;
        private readonly Dictionary<string, UnicodeBlockInfo> _unicodeBlocks;

        public ColumnStats(string name, string declaredType, int maxCharSamples) {
            Name = name;
            DeclaredType = declaredType;
            _maxCharSamples = Math.Max(1, maxCharSamples);
            _nonAsciiDistinct = new HashSet<uint>();
            _controlDistinct = new HashSet<uint>();
            _punctuationAsciiDistinct = new HashSet<uint>();
            _punctuationUnicodeDistinct = new HashSet<uint>();
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
        public long PunctuationCount { get; private set; }
        public long SumLength { get; private set; }
        public int MinLength { get; private set; } = int.MaxValue;
        public int MaxLength { get; private set; }
        public int MaxSampleCount => _maxCharSamples;
        public IReadOnlyCollection<uint> NonAsciiDistinctCharacters => _nonAsciiDistinct;
        public int NonAsciiDistinctCount => _nonAsciiDistinct.Count;
        public IReadOnlyCollection<uint> ControlDistinctCharacters => _controlDistinct;
        public int ControlDistinctCount => _controlDistinct.Count;
        public IReadOnlyCollection<uint> PunctuationAsciiDistinctCharacters => _punctuationAsciiDistinct;
        public int PunctuationAsciiDistinctCount => _punctuationAsciiDistinct.Count;
        public IReadOnlyCollection<uint> PunctuationUnicodeDistinctCharacters => _punctuationUnicodeDistinct;
        public int PunctuationUnicodeDistinctCount => _punctuationUnicodeDistinct.Count;
        public IEnumerable<UnicodeBlockSnapshot> UnicodeBlocks => _unicodeBlocks.Values.Select(info => info.CreateSnapshot());
        public string? ExampleNonAscii { get; private set; }
        public string? ExampleTrimDifference { get; private set; }

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
            var blocksSeen = new HashSet<string>(StringComparer.Ordinal);

            foreach (var rune in value.EnumerateRunes()) {
                var code = (uint)rune.Value;

                if (code > 0x7F) {
                    hasNonAscii = true;
                    _nonAsciiDistinct.Add(code);
                }

                if (code < 0x20 || code == 0x7F) {
                    hasControl = true;
                    _controlDistinct.Add(code);
                }

                var category = Rune.GetUnicodeCategory(rune);
                if (IsPunctuationCategory(category)) {
                    sawPunctuation = true;
                    if (code <= 0x7F) {
                        _punctuationAsciiDistinct.Add(code);
                    } else {
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
        }

        private static string Shorten(string input, int maxLength = 160) {
            if (input.Length <= maxLength) {
                return input;
            }
            return input.Substring(0, maxLength) + "…";
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
