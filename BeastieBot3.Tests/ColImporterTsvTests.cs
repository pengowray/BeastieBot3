using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;
using BeastieBot3.Col;
using Microsoft.Data.Sqlite;
using Spectre.Console;

namespace BeastieBot3.Tests;

// Pins how ColImporter reads ColDP TSV files: one line is one row and a double quote is an
// ordinary character. With CsvHelper's default RFC 4180 quoting, a NameUsage remarks value that
// started with `"` read on through the following rows up to the next quote, and those rows were
// never imported (6,560 NameUsage rows, 3,323 Reference rows, 458 TypeMaterial rows and 27
// VernacularName rows on COL26.7 XR). Every TSV in the archive goes through the same reader, so
// the fixture puts a quote-led value in two tables.
public class ColImporterTsvTests {
    private const string Tab = "\t";

    [Fact]
    public void Import_ValueStartingWithQuote_KeepsEveryRowAndQuoteLiterally() {
        using var fx = new ColFixture();
        fx.Import(new Dictionary<string, string> {
            ["NameUsage.tsv"] = Tsv(
                "col:ID", "col:parentID", "col:scientificName", "col:remarks",
                "A", "", "Animalia", "",
                "B", "A", "Foo bar", "\"opens a quote and never closes it",
                "C", "A", "Baz qux", "plain",
                "D", "B", "\"Quoted\" name", "says \"hi\" mid-field",
                "E", "B", "Last", "\"wrapped in quotes\""),
            ["VernacularName.tsv"] = Tsv(
                "col:taxonID", "col:name", "col:language",
                "C", "\"Baz\" bug", "eng",
                "E", "qux", "eng"),
        });

        Assert.Equal(5, fx.Scalar("SELECT COUNT(*) FROM nameusage;"));
        Assert.Equal(new[] { null, "Animalia", null }, fx.Row("A"));
        Assert.Equal(new[] { "A", "Foo bar", "\"opens a quote and never closes it" }, fx.Row("B"));
        Assert.Equal(new[] { "A", "Baz qux", "plain" }, fx.Row("C"));
        Assert.Equal(new[] { "B", "\"Quoted\" name", "says \"hi\" mid-field" }, fx.Row("D"));
        Assert.Equal(new[] { "B", "Last", "\"wrapped in quotes\"" }, fx.Row("E"));

        Assert.Equal(2, fx.Scalar("SELECT COUNT(*) FROM vernacularname;"));
        Assert.Equal("\"Baz\" bug", fx.Text("SELECT name FROM vernacularname WHERE taxonID = 'C';"));
        Assert.Equal("qux", fx.Text("SELECT name FROM vernacularname WHERE taxonID = 'E';"));
    }

    [Fact]
    public void Import_RowsWithWrongFieldCount_AreImportedAndReported() {
        using var fx = new ColFixture();
        var tsv = "col:ID\tcol:scientificName\tcol:remarks\n" +
                  "A\tAnimalia\t\n" +
                  "B\tFoo bar\tnote\textra\n" +   // line 3: one field too many
                  "C\tBaz qux\n" +                 // line 4: one field too few
                  "D\tLast\t\n";
        fx.Import(new Dictionary<string, string> { ["NameUsage.tsv"] = tsv });

        Assert.Equal(4, fx.Scalar("SELECT COUNT(*) FROM nameusage;"));
        Assert.Contains(
            "nameusage: 2 rows have a different number of fields than the header, which has 3 fields. " +
            "Values in those rows may be in the wrong columns. Line numbers in NameUsage.tsv: 3, 4.",
            fx.Output);
    }

    [Fact]
    public void Import_MatchingFieldCounts_ReportsNoMismatch() {
        using var fx = new ColFixture();
        fx.Import(new Dictionary<string, string> {
            ["NameUsage.tsv"] = Tsv("col:ID", "col:remarks", "A", "\"x", "B", "y"),
        });

        Assert.Equal(2, fx.Scalar("SELECT COUNT(*) FROM nameusage;"));
        Assert.DoesNotContain("different number of fields", fx.Output);
    }

    // Header and rows from a flat list of values. The header is every value up to the first one
    // without a "col:" prefix; rows follow with the same number of fields.
    private static string Tsv(params string[] values) {
        var width = Array.FindIndex(values, v => !v.StartsWith("col:", StringComparison.Ordinal));
        var sb = new StringBuilder();
        for (var i = 0; i < values.Length; i += width) {
            sb.Append(string.Join(Tab, values, i, width)).Append('\n');
        }
        return sb.ToString();
    }

    private sealed class ColFixture : IDisposable {
        private readonly string _root = Path.Combine(Path.GetTempPath(), "bb3-col-tsv-" + Guid.NewGuid().ToString("N"));
        private readonly StringWriter _out = new();
        private SqliteConnection? _db;

        public string Output => _out.ToString();

        public void Import(IReadOnlyDictionary<string, string> tsvFiles) {
            var colDir = Path.Combine(_root, "col");
            var datastore = Path.Combine(_root, "datastore");
            Directory.CreateDirectory(colDir);
            // A datapackage.json beside the zip stops the importer downloading one.
            File.WriteAllText(Path.Combine(colDir, "datapackage.json"), "{}");

            var zipPath = Path.Combine(colDir, "test.zip");
            using (var zip = ZipFile.Open(zipPath, ZipArchiveMode.Create)) {
                Add(zip, "metadata.yaml", "alias: TEST1\ntitle: Test checklist\n");
                foreach (var (name, content) in tsvFiles) {
                    Add(zip, name, content);
                }
            }

            var console = AnsiConsole.Create(new AnsiConsoleSettings {
                Ansi = AnsiSupport.No,
                ColorSystem = ColorSystemSupport.NoColors,
                Interactive = InteractionSupport.No,
                Out = new AnsiConsoleOutput(_out),
            });
            console.Profile.Width = 1000;

            new ColImporter(console, zipPath, _root, datastore, force: false).Process(CancellationToken.None);

            _db = new SqliteConnection(new SqliteConnectionStringBuilder {
                DataSource = Path.Combine(datastore, "col_coldp_TEST1.sqlite"),
                Mode = SqliteOpenMode.ReadOnly,
            }.ToString());
            _db.Open();
        }

        public long Scalar(string sql) {
            using var cmd = _db!.CreateCommand();
            cmd.CommandText = sql;
            return (long)cmd.ExecuteScalar()!;
        }

        public string? Text(string sql) {
            using var cmd = _db!.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteScalar() as string;
        }

        // parentID, scientificName and remarks of the nameusage row with this ID.
        public string?[] Row(string id) {
            using var cmd = _db!.CreateCommand();
            cmd.CommandText = "SELECT parentID, scientificName, remarks FROM nameusage WHERE ID = @id;";
            cmd.Parameters.AddWithValue("@id", id);
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read(), $"no nameusage row with ID {id}");
            var row = new string?[3];
            for (var i = 0; i < row.Length; i++) {
                row[i] = reader.IsDBNull(i) ? null : reader.GetString(i);
            }
            Assert.False(reader.Read(), $"more than one nameusage row with ID {id}");
            return row;
        }

        private static void Add(ZipArchive zip, string name, string content) {
            using var writer = new StreamWriter(zip.CreateEntry(name).Open(), new UTF8Encoding(false));
            writer.Write(content);
        }

        public void Dispose() {
            _db?.Dispose();
            SqliteConnection.ClearAllPools();
            try {
                Directory.Delete(_root, recursive: true);
            } catch (IOException) {
            } catch (UnauthorizedAccessException) {
            }
        }
    }
}
