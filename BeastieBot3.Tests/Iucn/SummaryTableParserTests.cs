using BeastieBot3.Iucn.SummaryTables;

namespace BeastieBot3.Tests;

// The parser of IUCN's summary tables 7 and 9 over lines built the way PdfLines builds them, one
// test per layout the real tables use (positions taken from the PDFs, rounded).
public sealed class SummaryTableParserTests {
    private const string Bold = "Calibri-Bold";
    private const string Italic = "Calibri-Italic";
    private const string Regular = "Calibri";
    private const string Header = "Arial-BoldMT";

    // A cell: its words from left, 4.5 points a letter and 2 points between words.
    private static IEnumerable<PdfWord> Cell(string text, double left, string font = Regular) {
        foreach (var word in text.Split(' ')) {
            var right = left + word.Length * 4.5;
            yield return new PdfWord(word, left, right, font);
            left = right + 2;
        }
    }

    // A cell centred on a point, as Excel centres the category, reason and version columns.
    private static IEnumerable<PdfWord> Centered(string text, double center, string font = Regular) {
        var words = text.Split(' ');
        var width = words.Sum(w => w.Length * 4.5) + (words.Length - 1) * 2;
        return Cell(text, center - width / 2, font);
    }

    private static PdfLine Line(double baseline, params IEnumerable<PdfWord>[] cells) =>
        new(1, baseline, cells.SelectMany(c => c).OrderBy(w => w.Left).ToList());

    private static PdfLine Text(double baseline, string text) => Line(baseline, Cell(text, 35, Regular));

    // The 2024 to 2026 header: Group, scientific and common names, two categories, reason, version.
    private static IEnumerable<PdfLine> GroupColumnHeader() => new[] {
        Line(597, Centered("IUCN Red List", 422.7, Header), Centered("IUCN Red", 467.8, Header)),
        Line(593, Centered("Reason for", 509, Header), Centered("Red List", 545, Header)),
        Line(589, Cell("Group", 35.5, Header), Cell("Scientific name", 128.1, Header), Cell("Common name", 267.8, Header),
            Centered("(2025)", 422.7, Header), Centered("List (2026)", 467.8, Header)),
        Line(585, Centered("change", 509, Header), Centered("version", 545, Header)),
        Line(581, Centered("Category", 422.7, Header), Centered("Category", 467.8, Header)),
    };

    private static PdfLine GroupColumnRow(double baseline, string group, string scientific, string? common, string old, string @new,
        string reason, string version) =>
        Line(baseline, Cell(group, 35.5, Bold), Cell(scientific, 128.1, Italic), common is null ? [] : Cell(common, 267.8),
            Centered(old, 422.7), Centered(@new, 467.8), Centered(reason, 509), Centered(version, 545));

    [Fact]
    public void Group_column_layout() {
        var lines = new List<PdfLine> {
            Text(800, "IUCN Red List version 2026-1: Table 7"),
            Text(790, "Last updated: 09 July 2026"),
            Text(760, "Table 7: Species changing IUCN Red List Status (2025–2026)"),
            Text(750, "To help Red List users interpret the changes between the Red List updates, a summary of species that have changed category"),
            Text(742, "between 2025 (IUCN Red List version 2025-2) and 2026 (IUCN Red List version 2026-1) and the reasons for these changes."),
            Text(700, "Reasons for change: G - Genuine status change; N - Non-genuine status change; E - Previous listing was an Error."),
        };
        lines.AddRange(GroupColumnHeader());
        lines.Add(GroupColumnRow(570, "MAMMALS", "Capra walie", "Walia Ibex", "VU", "CR", "G", "2026-1"));
        lines.Add(GroupColumnRow(561, "MAMMALS", "Dasycercus cristicauda", "Crested-tailed Mulgara", "NT", "EX", "N", "2026-1"));
        lines.Add(Text(20, "THE IUCN RED LIST OF THREATENED SPECIES"));

        var parse = SummaryTableParser.ParseTable7(lines);

        Assert.True(parse.FoundHeader);
        Assert.Empty(parse.Unread);
        Assert.Equal(2, parse.Rows.Count);
        var row = parse.Rows[0];
        Assert.Equal(("MAMMALS", "Capra walie", "Walia Ibex"), (row.Group, row.ScientificName, row.CommonName));
        Assert.Equal(("VU", "CR", "G", "2026-1"), (row.Old.Category, row.New.Category, row.Reason, row.Version));
        Assert.Null(row.Anomaly);
        Assert.Equal("2026-1", parse.Info.Release);
        Assert.Equal("09 July 2026", parse.Info.LastUpdated);
        Assert.Equal(("2025-2", "2026-1"), (parse.Info.PeriodFrom, parse.Info.PeriodTo));
        Assert.True(parse.Info.DefinesError);
        Assert.Equal("Table 7: Species changing IUCN Red List Status (2025-2026)", parse.Info.Title);
    }

    [Fact]
    public void A_group_name_that_runs_over_the_scientific_name_stays_apart_from_it() {
        // 2025-1: the bold group name is longer than its column and is drawn over the italic name.
        var lines = GroupColumnHeader().ToList();
        lines.Add(Line(570, Cell("INSECTS (Grasshoppers, Locusts, Crickets)", 38, Bold), Cell("Anadrymadusa brevipennis", 125, Italic),
            Cell("Short-winged Tonged Bush-cricket", 267.8), Centered("VU", 422.7), Centered("EN", 467.8), Centered("G", 509),
            Centered("2025-1", 545)));

        var row = Assert.Single(SummaryTableParser.ParseTable7(lines).Rows);

        Assert.Equal("INSECTS (Grasshoppers, Locusts, Crickets)", row.Group);
        Assert.Equal("Anadrymadusa brevipennis", row.ScientificName);
        Assert.Equal("Short-winged Tonged Bush-cricket", row.CommonName);
    }

    [Fact]
    public void A_group_name_that_ends_close_to_the_scientific_name_is_not_joined_to_it() {
        // 2024-1: "MOLLUSCS - Gastropods" ends about 3 points before the scientific name column.
        var lines = GroupColumnHeader().ToList();
        lines.Add(Line(570, Cell("MOLLUSCS - Gastropods", 35.5, Bold), Cell("Acicula hausdorfi", 128.1, Italic),
            Centered("NT", 422.7), Centered("LC", 467.8), Centered("N", 509), Centered("2024-1", 545)));

        var row = Assert.Single(SummaryTableParser.ParseTable7(lines).Rows);

        Assert.Equal(("MOLLUSCS - Gastropods", "Acicula hausdorfi"), (row.Group, row.ScientificName));
        Assert.Null(row.CommonName);
    }

    // The 2007 to 2023 header: no Group column; groups are heading lines.
    private static IEnumerable<PdfLine> HeadingHeader(bool reasonAndVersion = true) {
        var lines = new List<PdfLine> {
            Line(597, Centered("IUCN Red List", 400, Header), Centered("IUCN Red", 450, Header)),
            Line(589, Cell("Scientific name", 40, Header), Cell("Common name", 220, Header), Centered("(2006)", 400, Header),
                Centered("List (2007)", 450, Header)),
            Line(581, Centered("Category", 400, Header), Centered("Category", 450, Header)),
        };
        if (reasonAndVersion) {
            lines.Add(Line(589.5, Cell("Reason for change", 480, Header)));
            lines.Add(Line(585, Centered("version", 580, Header)));
        }
        return lines.OrderByDescending(l => l.Baseline);
    }

    private static PdfLine HeadingRow(double baseline, string scientific, string? common, params string[] right) {
        var cells = new List<IEnumerable<PdfWord>> { Cell(scientific, 40, Italic) };
        if (common is not null) cells.Add(Cell(common, 220));
        double[] centers = { 400, 450, 517, 580 };
        for (var i = 0; i < right.Length; i++) cells.Add(Centered(right[i], centers[i]));
        return Line(baseline, cells.ToArray());
    }

    [Fact]
    public void Heading_layout_with_removed_species_and_possibly_extinct_categories() {
        var lines = HeadingHeader().ToList();
        lines.Add(Line(570, Cell("MAMMALS", 40, Bold)));
        lines.Add(HeadingRow(560, "Lipotes vexillifer", "Baiji", "CR", "CR (PE)", "G", "2007"));
        lines.Add(Line(550, Cell("Species removed from the IUCN Red List for taxonomic reasons:", 40, Bold)));
        lines.Add(HeadingRow(540, "Mauremys iversoni", "Fujian Pond Turtle", "DD", "NR", "hybrid", "2007"));
        lines.Add(Line(530, Cell("BIRDS", 40, Bold)));
        lines.Add(HeadingRow(520, "Andropadus chlorigula", "Green-throated Greenbul", "LC", "NR", "synonym of A. nigriceps", "2007"));
        lines.Add(HeadingRow(510, "Corvus unicolor", "Banggai Crow", "CR (PE)", "CR", "N", "2007"));

        var parse = SummaryTableParser.ParseTable7(lines);

        Assert.Empty(parse.Unread);
        Assert.Equal(4, parse.Rows.Count);
        Assert.Equal(("MAMMALS", "CR", "PE"), (parse.Rows[0].Group, parse.Rows[0].New.Category, parse.Rows[0].New.Tag));
        var removed = parse.Rows[1];
        Assert.Equal("Species removed from the IUCN Red List for taxonomic reasons", removed.Section);
        Assert.Equal(("hybrid", (string?)null, (string?)null), (removed.ReasonText, removed.Reason, removed.Anomaly));
        var synonym = parse.Rows[2];
        Assert.Equal(("BIRDS", (string?)null, "synonym of A. nigriceps"), (synonym.Group, synonym.Section, synonym.ReasonText));
        Assert.Equal(("CR", "PE"), (parse.Rows[3].Old.Category, parse.Rows[3].Old.Tag));
    }

    [Fact]
    public void Genuine_sections_of_the_2008_table_give_the_reason() {
        var lines = HeadingHeader(reasonAndVersion: false).ToList();
        lines.Add(Line(570, Cell("MAMMALS", 40, Bold)));
        lines.Add(Line(560, Cell("Genuine Improvements", 40, Bold)));
        lines.Add(HeadingRow(550, "Gulo gulo", "Wolverine", "VU", "NT"));
        lines.Add(Line(540, Cell("Genuine deteriorations", 40, Bold)));
        lines.Add(HeadingRow(530, "Bos sauveli", "Kouprey", "EN", "CR"));

        var rows = SummaryTableParser.ParseTable7(lines).Rows;

        Assert.Equal(2, rows.Count);
        Assert.Equal(("Genuine improvements", "G"), (rows[0].Section, rows[0].Reason));
        Assert.Equal(("Genuine deteriorations", "G", (string?)null), (rows[1].Section, rows[1].Reason, rows[1].Version));
        Assert.All(rows, r => Assert.Null(r.Anomaly));
    }

    [Fact]
    public void Cells_go_to_the_columns_in_order_when_a_section_is_shifted_from_its_header() {
        // 2014-2: the bird rows sit 30 points left of the header's columns.
        var lines = HeadingHeader().ToList();
        lines.Add(Line(570, Cell("Accipiter erythrauchen", 40, Italic), Cell("Rufous-necked Sparrowhawk", 220),
            Centered("LC", 370), Centered("NT", 425), Centered("G", 470), Centered("2014.2", 560)));

        var row = Assert.Single(SummaryTableParser.ParseTable7(lines).Rows);

        Assert.Equal(("LC", "NT", "G", "2014-2"), (row.Old.Category, row.New.Category, row.Reason, row.Version));
    }

    [Fact]
    public void A_zero_in_the_common_name_column_is_no_common_name() {
        // 2016-2 and 2019-1: a number 0, aligned to the right of the common name column.
        var lines = HeadingHeader().ToList();
        lines.Add(Line(570, Cell("Lonchophylla concava", 40, Italic), Cell("0", 360), Centered("NT", 400), Centered("LC", 450),
            Centered("N", 517), Centered("2016-2", 580)));

        var row = Assert.Single(SummaryTableParser.ParseTable7(lines).Rows);

        Assert.Null(row.CommonName);
        Assert.Equal(("NT", (string?)null), (row.Old.Category, row.Anomaly));
    }

    [Fact]
    public void A_heading_long_enough_to_reach_the_category_columns_is_still_a_heading() {
        var lines = HeadingHeader().ToList();
        lines.Add(Line(570, Cell("CRUSTACEANS (Arthropoda: Branchiopoda, Cephalocardia, and Remipedia)", 40, Bold)));
        lines.Add(HeadingRow(560, "Parhyale plumicornis", null, "DD", "LC", "N", "2020-3"));

        var parse = SummaryTableParser.ParseTable7(lines);

        Assert.Empty(parse.Unread);
        var row = Assert.Single(parse.Rows);
        Assert.Equal("CRUSTACEANS (Arthropoda: Branchiopoda, Cephalocardia, and Remipedia)", row.Group);
    }

    [Fact]
    public void A_cell_IUCN_misprinted_is_kept_with_an_anomaly() {
        var lines = HeadingHeader().ToList();
        lines.Add(HeadingRow(560, "Alzoniella pyrenaica", null, "VY", "DD", "N", "2010.4"));

        var row = Assert.Single(SummaryTableParser.ParseTable7(lines).Rows);

        Assert.Equal("Unread previous category \"VY\"", row.Anomaly);
        Assert.Equal("2010-4", row.Version);
    }

    [Fact]
    public void Table_9_rows_take_the_date_text_of_the_lines_around_them() {
        var lines = new List<PdfLine> {
            Line(600, Centered("IUCN Red List", 400, Header)),
            Line(595, Centered("Year of", 470, Header), Cell("Date last recorded in", 520, Header)),
            Line(590, Cell("Scientific name", 40, Header), Cell("Common name", 220, Header), Centered("(2017-1)", 400, Header)),
            Line(585, Centered("Assessment", 470, Header), Cell("the wild", 540, Header)),
            Line(580, Centered("Category", 400, Header)),
            Line(570, Cell("MAMMALS", 40, Bold)),
            Line(560, Cell("Uromys emmae", 40, Italic), Cell("Emma's Giant Rat", 220), Centered("CR(PE)", 400), Centered("2008", 470),
                Centered("1946", 560)),
            Line(553, Centered("1886-1888 (confirmed);", 560)),
            Line(546, Cell("Uromys imperator", 40, Italic), Cell("Emperor Rat", 220), Centered("CR(PE)", 400), Centered("2008", 470)),
            Line(539, Centered("1960s (possible)", 560)),
            Line(530, Cell("Viverra civettina", 40, Italic), Cell("Malabar Civet", 220), Centered("CR(PEW)", 400), Centered("2015", 470),
                Centered("?", 560)),
        };

        var parse = SummaryTableParser.ParseTable9(lines);

        Assert.Empty(parse.Unread);
        Assert.Equal(3, parse.Rows.Count);
        Assert.Equal(("MAMMALS", "Uromys emmae", "1946", 2008), (parse.Rows[0].Group, parse.Rows[0].ScientificName,
            parse.Rows[0].LastRecorded, parse.Rows[0].YearAssessed));
        Assert.Equal("1886-1888 (confirmed); 1960s (possible)", parse.Rows[1].LastRecorded);
        Assert.Equal(("CR", "PEW"), (parse.Rows[2].Category.Category, parse.Rows[2].Category.Tag));
    }

    [Fact]
    public void A_page_without_a_header_uses_the_columns_of_the_page_before() {
        var lines = GroupColumnHeader().ToList();
        lines.Add(GroupColumnRow(570, "MAMMALS", "Capra walie", "Walia Ibex", "VU", "CR", "G", "2026-1"));
        lines.Add(GroupColumnRow(800, "BIRDS", "Lophura edwardsi", "Vietnam Pheasant", "CR", "CR(PEW)", "G", "2026-1") with { Page = 2 });

        var rows = SummaryTableParser.ParseTable7(lines).Rows;

        Assert.Equal(2, rows.Count);
        Assert.Equal(("BIRDS", 2, "PEW"), (rows[1].Group, rows[1].Page, rows[1].New.Tag));
    }

    [Fact]
    public void Words_are_built_from_letters_in_drawing_order() {
        // The hyphen of "2019‐3" is in a font of its own in the 2019-3 table; "L" of "Lilac" is drawn apart.
        PdfLetter Letter(string value, double left, string font = Regular) => new(value, left, left + 4, 100, font);
        var letters = new List<PdfLetter> {
            Letter("L", 10), Letter("2", 50), Letter("0", 54), Letter("1", 58), Letter("9", 62), Letter("‐", 66, "Symbol"),
            Letter("3", 70), Letter("i", 14), Letter("l", 18), Letter("a", 22), Letter("c", 26),
        };

        var line = Assert.Single(PdfLines.Group(1, letters));

        Assert.Equal(new[] { "Lilac", "2019‐3" }, line.Words.Select(w => w.Text));
    }
}
