using System.Text;
using BeastieBot3.Iucn.SummaryTables;
using BeastieBot3.StatusLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// `statuses japan-import`: the codes of the Ministry of the Environment's categories, the lines of
// the Red List 2020 PDF (built here as PdfLines words with x positions, as PdfPig reads them from
// the PDF), the two layouts of the 5th Red List's CSV files and their encodings, the store, and a
// run with a folder that lacks a file.
public sealed class JapanRedListTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-japan-" + Guid.NewGuid().ToString("N"));

    public JapanRedListTests() {
        Directory.CreateDirectory(_dir);
        File.WriteAllText(Path.Combine(_dir, "paths.ini"), "[Datastore]\n");
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try {
            Directory.Delete(_dir, recursive: true);
        } catch (IOException) {
        }
    }

    // ---- categories ----

    [Theory]
    // The plant CSVs: Roman numerals (U+2160, U+2161), a full-width Ａ, full-width brackets; the B of
    // IB is ASCII in the same files.
    [InlineData("絶滅危惧ⅠＡ類（CR）", "CR")]
    [InlineData("絶滅危惧ⅠB類（EN）", "EN")]
    [InlineData("絶滅危惧Ⅱ類（VU）", "VU")]
    [InlineData("準絶滅危惧（NT）", "NT")]
    [InlineData("野生絶滅（EW）", "EW")]
    [InlineData("絶滅（EX）", "EX")]
    // The animal CSVs' two columns.
    [InlineData("絶滅危惧IA類", "CR")]
    [InlineData("絶滅危惧IB類", "EN")]
    [InlineData("絶滅のおそれのある地域個体群", "LP")]
    [InlineData("EN", "EN")]
    // The PDF's headings: I類 not split into IA and IB, and a space before the bracket.
    [InlineData("絶滅危惧I類（CR+EN）", "CR+EN")]
    [InlineData("情報不足 （DD）", "DD")]
    [InlineData("絶滅危惧Ｉ類", "CR+EN")]
    [InlineData("絶滅危惧I類（CR＋EN）", "CR+EN")]
    public void Category_is_read_from_any_form_the_lists_write(string written, string code) =>
        Assert.Equal(code, JapanRedListCategory.Code(written));

    [Theory]
    [InlineData("絶滅危惧IA類（EN）")]
    [InlineData("絶滅危惧IA類（XX）")]
    [InlineData("カテゴリー外")]
    [InlineData("")]
    public void Category_whose_name_and_code_disagree_or_are_unknown_has_no_code(string written) =>
        Assert.Null(JapanRedListCategory.Code(written));

    [Fact]
    public void Population_is_the_text_before_the_last_no() {
        Assert.Equal("九州地方", JapanRedList.PopulationOf("九州地方のカワネズミ"));
        Assert.Equal("本州の太平洋側湖沼系群", JapanRedList.PopulationOf("本州の太平洋側湖沼系群のニシン"));
        Assert.Null(JapanRedList.PopulationOf("カワネズミ"));
        Assert.Null(JapanRedList.PopulationOf(null));
    }

    [Fact]
    public void Scientific_name_has_full_width_letters_made_ascii_and_spaces_collapsed() {
        Assert.Equal("Utricularia x japonica", JapanRedList.CleanScientificName("Utricularia ｘ japonica"));
        Assert.Equal("Borniopsis sp.", JapanRedList.CleanScientificName("Borniopsis  sp．"));
        Assert.Equal("Lewinskya iwatsukii", JapanRedList.CleanScientificName("Lewinskya iwatsukii "));
        Assert.Equal("Cladonia koyaënsis", JapanRedList.CleanScientificName("Cladonia koyaënsis"));
    }

    // ---- the Red List 2020 PDF ----

    // Lines as PdfLines gives them: each word with its left x; the width does not matter here.
    private static PdfLine Line(int page, params (string Text, double Left)[] words) =>
        new(page, 0, words.Select(w => new PdfWord(w.Text, w.Left, w.Left + 10 * w.Text.Length)).ToList());

    private static IReadOnlyList<PdfLine> PdfFixture() => [
        Line(1, ("別添資料３", 487.8)),
        Line(1, ("【哺乳類】", 496.0)),
        Line(1, ("【哺乳類】環境省レッドリスト2020", 67.1)),
        Line(1, ("●絶滅危惧IA類（CR）", 67.1), ("2種", 310.7)),
        Line(1, ("ツシマヤマネコ", 89.4), ("Prionailurus", 310.2), ("bengalensis", 369.6), ("euptilurus", 424.7)),
        Line(1, ("ラッコ", 89.4), ("Enhydra", 310.2), ("lutris", 351.0)),
        Line(1, ("●野生絶滅（EW）", 67.1), ("0種", 310.7)),
        Line(1, ("―", 89.4)),
        Line(1, ("1", 262.7), ("/", 271.6), ("131", 280.4), ("ページ", 300.4)),
        Line(2, ("別添資料３", 487.8)),
        Line(2, ("【哺乳類】", 496.0)),
        Line(2, ("●情報不足", 67.1), ("（DD）", 135.5), ("1種", 310.7)),
        Line(2, ("ニホンイイズナ（本州亜種）", 89.4), ("Mustela", 310.2), ("nivalis", 348.7), ("namiyei", 381.1)),
        Line(2, ("●絶滅のおそれのある地域個体群（LP）", 67.1), ("2集団", 310.7)),
        Line(2, ("九州地方のカワネズミ", 89.4), ("Chimarrogale", 310.2), ("platycephala", 375.6)),
        Line(2, ("本州の太平洋側湖沼系群のニシン", 89.4), ("Clupea", 310.2), ("pallasii", 348.0)),
        Line(2, ("2", 262.7), ("/", 271.6), ("131", 280.4), ("ページ", 300.4)),
        // Insects: the order before the name, and the scientific name's column further right.
        Line(3, ("【昆虫類】環境省レッドリスト2020", 67.1)),
        Line(3, ("●絶滅のおそれのある地域個体群（LP）", 67.1), ("1集団", 335.0)),
        Line(3, ("トンボ目", 80.0), ("房総半島のシロバネカワトンボ（f.", 120.0), ("edai）を含むアサヒナカワトンボ", 230.0),
            ("Mnais", 334.6), ("pruinosa", 360.0)),
        // Other invertebrates: phylum, class and order before the name; a name that pdftotext puts on
        // the line above its row is on its row here.
        Line(62, ("【その他無脊椎動物】環境省レッドリスト2020", 153.7)),
        Line(62, ("●絶滅危惧I類（CR+EN）", 58.0), ("2種", 352.6)),
        Line(62, ("節足動物門", 76.9), ("蛛形綱（クモ形綱・クモ綱）クモ目", 102.1), ("オキナワキムラグモ（広義）", 165.2),
            ("Ryuthela", 352.4), ("nishihirai", 388.3), ("sensu", 427.2), ("lato", 450.4)),
        Line(62, ("節足動物門", 77.5), ("甲殻綱", 117.0), ("エビ目", 142.6), ("カクレサワガニ", 165.2), ("Amamiku", 352.4), ("occulta", 390.2)),
        // A line with a name and no scientific name is not a row.
        Line(62, ("アユミコケムシ", 165.2)),
    ];

    [Fact]
    public void Pdf_rows_are_read_under_their_group_and_heading() {
        var parse = JapanRedList2020Pdf.Parse(PdfFixture());

        Assert.Equal(8, parse.Rows.Count);
        var cat = parse.Rows[0];
        Assert.Equal(("哺乳類", "CR", "絶滅危惧IA類（CR）", null, "ツシマヤマネコ", "Prionailurus bengalensis euptilurus", 1, 5),
            (cat.Group, cat.Category, cat.CategoryWritten, cat.HigherTaxa, cat.JapaneseName, cat.ScientificName, cat.Page, cat.Line));
        var weasel = parse.Rows[2];
        Assert.Equal(("DD", "情報不足 （DD）", "ニホンイイズナ（本州亜種）", "Mustela nivalis namiyei", 2, 4),
            (weasel.Category, weasel.CategoryWritten, weasel.JapaneseName, weasel.ScientificName, weasel.Page, weasel.Line));
        Assert.Equal(["九州地方のカワネズミ", "本州の太平洋側湖沼系群のニシン"],
            parse.Rows.Where(r => r.Group == "哺乳類" && r.Category == "LP").Select(r => r.JapaneseName));

        var dragonfly = parse.Rows[5];
        Assert.Equal(("昆虫類", "LP", "トンボ目", "房総半島のシロバネカワトンボ（f. edai）を含むアサヒナカワトンボ", "Mnais pruinosa"),
            (dragonfly.Group, dragonfly.Category, dragonfly.HigherTaxa, dragonfly.JapaneseName, dragonfly.ScientificName));

        var spider = parse.Rows[6];
        Assert.Equal(("その他無脊椎動物", "CR+EN", "節足動物門 蛛形綱（クモ形綱・クモ綱）クモ目", "オキナワキムラグモ（広義）", "Ryuthela nishihirai sensu lato"),
            (spider.Group, spider.Category, spider.HigherTaxa, spider.JapaneseName, spider.ScientificName));
        Assert.Equal(("節足動物門 甲殻綱 エビ目", "カクレサワガニ", "Amamiku occulta"),
            (parse.Rows[7].HigherTaxa, parse.Rows[7].JapaneseName, parse.Rows[7].ScientificName));
    }

    [Fact]
    public void Pdf_headings_count_the_rows_read_and_headers_and_footers_are_not_rows() {
        var parse = JapanRedList2020Pdf.Parse(PdfFixture());

        Assert.Equal(
            [("哺乳類", "CR", 2, 2), ("哺乳類", "EW", 0, 0), ("哺乳類", "DD", 1, 1), ("哺乳類", "LP", 2, 2), ("昆虫類", "LP", 1, 1),
                ("その他無脊椎動物", "CR+EN", 2, 2)],
            parse.Headings.Select(h => (h.Group, h.Category!, h.Stated, h.Read)));
        // The line with a name and no scientific name is reported, and not counted under the CR+EN
        // heading.
        var unread = Assert.Single(parse.Unread);
        Assert.Equal("page 62, line 5: アユミコケムシ", unread);
        Assert.DoesNotContain(parse.Rows, r => r.ScientificName.Contains("ページ", StringComparison.Ordinal) || r.JapaneseName.Contains("別添資料", StringComparison.Ordinal));
    }

    // ---- the 5th Red List's CSV files ----

    private static readonly JapanRedListGroup Birds = JapanRedList.Groups.Single(g => g.Key == "birds");
    private static readonly JapanRedListGroup Reptiles = JapanRedList.Groups.Single(g => g.Key == "reptiles");
    private static readonly JapanRedListGroup Plants = JapanRedList.Groups.Single(g => g.Key == "vascular-plants");

    // The animal layout, cut down to a few columns; the Red List 2020 block is put before the 5th
    // Red List's block here, so the reader has to choose the block by its heading.
    private const string AnimalCsv = """
        1,2,3,4,5,6,7,8,9,10,11,12,13
        RL2020,RL2020,RL2020,5thRL,5thRL,5thRL,5thRL,5thRL,5thRL,5thRL,5thRL,5thRL,生息・生育環境区分（陸域（山地・丘陵）中区分）
        和名,学名,カテゴリー,掲載No.,分科会名,カテゴリーJPN,カテゴリーENG,目名,科名,和名,学名,判定基準,高標高地
        マミジロクイナ,Porzana cinerea brevipes,EX,BI0004,鳥類,絶滅,EX,ツル目,クイナ科,マミジロクイナ,Poliolimnas cinereus brevipes,③,0
        三宅島、八丈島、青ヶ島のオカダトカゲ,Plestiodon latiscutatus,LP,RE0063,爬虫類・両生類,絶滅のおそれのある地域個体群,LP,有鱗目,トカゲ科,三宅島、八丈島、青ヶ島のオカダトカゲ,Plestiodon latiscutatus,②,0
        －,－,－,BI0099,鳥類,情報不足,DD,スズメ目,ツグミ科,アカコッコ,Turdus celaenops,－,1
        －,－,－,BI0100,鳥類,絶滅危惧IA類,EN,スズメ目,ツグミ科,ホントウアカヒゲ,Larvivora namiyei,B2ab,0
        """;

    [Fact]
    public void Animal_csv_is_read_from_the_5th_red_list_block() {
        var problems = new List<string>();
        var rows = JapanRedListCsv.Read(new StringReader(AnimalCsv), Birds, problems);

        Assert.Equal(3, rows.Count);
        var rail = rows[0];
        Assert.Equal(("EX", "絶滅", "マミジロクイナ", "Poliolimnas cinereus brevipes", "ツル目 クイナ科", "③", "BI0004", 4, (int?)null),
            (rail.Category, rail.CategoryJapanese, rail.JapaneseName, rail.ScientificName, rail.HigherTaxa, rail.Criteria, rail.ListNumber,
                rail.SourceLine, rail.SourcePage));
        var lizard = rows[1];
        Assert.Equal(("LP", "三宅島、八丈島、青ヶ島"), (lizard.Category, lizard.Population));
        Assert.Null(rows[2].Criteria);
        // The JPN and ENG columns disagree on the last row, so it is left out and reported.
        var problem = Assert.Single(problems);
        Assert.StartsWith("redlist2026_birds.csv, row 7: unknown category", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void Plant_csv_in_shift_jis_is_read_with_the_code_in_brackets() {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        const string text = """
            カテゴリー,分類群,和名,学名
            絶滅危惧ⅠＡ類（CR）,維管束植物,コウヨウザンカズラ,Phlegmariurus cunninghamioides
            絶滅危惧ⅠB類（EN）,維管束植物,ムラサキミミカキグサ,Utricularia ｘ japonica
            """;
        var path = Path.Combine(_dir, "redlist2025_ikansoku.csv");
        File.WriteAllBytes(path, Encoding.GetEncoding(932).GetBytes(text.ReplaceLineEndings("\r\n")));

        var problems = new List<string>();
        var rows = JapanRedListCsv.Read(path, Plants, problems);

        Assert.Empty(problems);
        Assert.Equal([("CR", "絶滅危惧ⅠＡ類（CR）", "コウヨウザンカズラ", "Phlegmariurus cunninghamioides"), ("EN", "絶滅危惧ⅠB類（EN）", "ムラサキミミカキグサ", "Utricularia x japonica")],
            rows.Select(r => (r.Category, r.CategoryJapanese, r.JapaneseName!, r.ScientificName)));
        Assert.All(rows, r => Assert.Null(r.HigherTaxa));
        Assert.Equal([2, 3], rows.Select(r => r.SourceLine));
    }

    [Fact]
    public void Csv_encoding_is_utf8_with_or_without_a_byte_order_mark_else_shift_jis() {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        const string text = "和名,学名";
        Assert.Equal(text, JapanRedListCsv.Decode([.. Encoding.UTF8.Preamble, .. Encoding.UTF8.GetBytes(text)]));
        Assert.Equal(text, JapanRedListCsv.Decode(Encoding.UTF8.GetBytes(text)));
        Assert.Equal(text, JapanRedListCsv.Decode(Encoding.GetEncoding(932).GetBytes(text)));
    }

    [Fact]
    public void Csv_without_name_columns_is_refused() {
        var ex = Assert.Throws<InvalidDataException>(() => JapanRedListCsv.Read(new StringReader("a,b\n1,2\n"), Reptiles, new List<string>()));
        Assert.Contains("redlist2026_reptiles.csv", ex.Message, StringComparison.Ordinal);
    }

    // ---- the store and the run ----

    [Fact]
    public void Rows_replace_the_stored_rows_with_their_group_and_source() {
        var storePath = Path.Combine(_dir, "status_lists.sqlite");
        var rows = JapanRedListCsv.Read(new StringReader(AnimalCsv), Birds, new List<string>());
        var now = new DateTime(2026, 10, 9, 0, 0, 0, DateTimeKind.Utc);
        var source = new StatusSourceInfo(StatusSources.Japan, JapanRedList.Title, JapanRedList.SiteUrl, JapanRedList.Licence,
            JapanRedList.Citation, "japan-redlist-2026-10-09", now, rows.Count);
        using (var store = StatusListStore.Open(storePath)) {
            store.ReplaceJapan(rows, now, source);
            store.ReplaceJapan(rows.Take(2).ToList(), now, source with { RowCount = 2 });
            Assert.Equal(2, store.CountJapan());
        }

        using var connection = new SqliteConnection($"Data Source={storePath};Mode=ReadOnly;Pooling=False");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT row_id, group_key, group_en, group_ja, kingdom, list_version, list_year, category, population, source_file, source_url,
                source_page, source_line
            FROM japan_listing ORDER BY row_id
            """;
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal((1L, "birds", "Birds", "鳥類", "ANIMALIA", "5th Red List", 2026L, "EX", true, "redlist2026_birds.csv",
                "https://ikilog.biodic.go.jp/rdbdata/files/redlist2026/redlist2026_birds.csv", true, 4L),
            (reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5),
                reader.GetInt64(6), reader.GetString(7), reader.IsDBNull(8), reader.GetString(9), reader.GetString(10), reader.IsDBNull(11),
                reader.GetInt64(12)));
        Assert.True(reader.Read());
        Assert.Equal(("LP", "三宅島、八丈島、青ヶ島"), (reader.GetString(7), reader.GetString(8)));
    }

    [Fact]
    public async Task Folder_without_every_file_is_refused_and_the_store_is_not_changed() {
        var folder = Path.Combine(_dir, "japan-redlist-2026-10-09");
        Directory.CreateDirectory(folder);
        File.WriteAllText(Path.Combine(folder, "redlist2026_birds.csv"), AnimalCsv);
        var storePath = Path.Combine(_dir, "status_lists.sqlite");

        var code = await StatusListImport.RunAsync(new JapanImportCommand.Settings {
            File = folder, StorePath = storePath, IniFile = Path.Combine(_dir, "paths.ini"), SettingsDir = _dir,
        }, JapanImportCommand.Spec(), CancellationToken.None);

        Assert.Equal(1, code);
        Assert.False(File.Exists(storePath));
    }

    [Fact]
    public void Every_group_has_one_file_and_the_pdf_covers_the_groups_the_csv_files_do_not() {
        Assert.Equal(13, JapanRedList.Groups.Count);
        Assert.Equal(9, JapanRedList.Files.Count);
        Assert.Equal(["mammals", "fishes", "insects", "molluscs", "other-invertebrates"],
            JapanRedList.Groups.Where(g => g.FromPdf).Select(g => g.Key));
        Assert.Equal(JapanRedList.PdfFileName, JapanRedList.Files[^1].FileName);
    }
}
