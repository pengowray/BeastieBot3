using BeastieBot3.Site.Update;

namespace BeastieBot3.Site.Tests;

// {{IUCN statuses}}: the counts are made again from the updated text, and a box can be added.
public sealed class IucnStatusesSummaryTests {
    private static string Row(string name, string? status) =>
        $"{{{{Species table/row\n|name=[[{name}]] |binomial=A. {name.ToLowerInvariant()}\n"
        + (status is null ? "" : $"|iucn-status={status} |population=Unknown\n")
        + "|direction={{population change unknown}}\n}}\n";

    private static string List(string box, params (string Name, string? Status)[] rows) =>
        "Intro.\n\n==Conventions==\n" + box + "The author citation...\n\n==Classification==\n{{Species table|genus=Alpha}}\n"
        + string.Concat(rows.Select(r => Row(r.Name, r.Status))) + "|}\n";

    private static StatusUpdateResult Result(string text) => new(text, [], 0, []);

    private static readonly (string, string?)[] Rows = [
        ("One", "LC"), ("Two", "LC"), ("Three", "EN"), ("Four", "CR(PE)"), ("Five", "NE"), ("Six", null),
        ("Seven", "LR/nt"), ("Eight", "DD<ref name=\"x\"/>"), ("Nine", "NA"),
    ];

    [Fact]
    public void Counts_species_table_rows_as_editors_do() {
        var counts = IucnStatusesSummary.Count(new WikitextScanner(List("", Rows)));
        Assert.True(counts.FromRows);
        Assert.Equal(8, counts.Items);
        Assert.Equal(1, counts.Uncounted);
        Assert.Equal(new Dictionary<string, int> {
            ["ex"] = 0, ["ew"] = 0, ["cr"] = 1, ["en"] = 1, ["vu"] = 0, ["nt"] = 1, ["lc"] = 2, ["dd"] = 1, ["ne"] = 2,
        }, counts.ByKey);
    }

    [Fact]
    public void Updates_the_counts_in_place() {
        var box = "{{IUCN statuses|ex=0|ew=0|cr=1|en=0|vu=0|nt=1| lc = 5 |dd=1|ne=2}}\n";
        var input = List(box, Rows);
        var result = IucnStatusesSummary.Apply(input, Result(input), add: false);

        Assert.Contains("{{IUCN statuses|ex=0|ew=0|cr=1|en=1|vu=0|nt=1| lc = 2 |dd=1|ne=2}}\n", result.Text);
        var finding = Assert.Single(result.Findings);
        Assert.Equal((StatusItemKind.StatusSummary, StatusOutcome.Updated, 4), (finding.Kind, finding.Outcome, finding.Line));
        Assert.Equal(box.TrimEnd('\n'), finding.Before);
        Assert.Equal("en 0 → 1, lc 5 → 2", finding.Notes.Single(n => n.Kind == StatusNoteKind.SummaryChanged).Detail);
        Assert.Equal(1, finding.Notes.Single(n => n.Kind == StatusNoteKind.SummaryUncounted).Id);
        Assert.Equal(input.Replace(box, "", StringComparison.Ordinal),
            result.Text.Replace("{{IUCN statuses|ex=0|ew=0|cr=1|en=1|vu=0|nt=1| lc = 2 |dd=1|ne=2}}\n", "", StringComparison.Ordinal));
    }

    [Fact]
    public void Current_counts_are_left_as_they_are() {
        var input = List("{{IUCN statuses|ex=0|ew=0|cr=1|en=1|vu=0|nt=1|lc=2|dd=1|ne=2}}\n", Rows);
        var result = IucnStatusesSummary.Apply(input, Result(input), add: false);
        Assert.Equal(input, result.Text);
        Assert.Equal(StatusOutcome.Current, Assert.Single(result.Findings).Outcome);
    }

    // A count the box lacks is added unless it is 0, or is dd or ne and the box hides them.
    [Fact]
    public void Missing_counts_are_added_at_the_end() {
        var input = List("{{IUCN statuses|lc=2|suppress-others=y}}\n", Rows);
        var result = IucnStatusesSummary.Apply(input, Result(input), add: false);
        Assert.Contains("{{IUCN statuses|lc=2|suppress-others=y|cr=1|en=1|nt=1}}", result.Text);
    }

    [Fact]
    public void Two_boxes_are_left_as_they_are() {
        var input = List("{{IUCN statuses|lc=1}}\n{{IUCN statuses|lc=1}}\n", Rows);
        var result = IucnStatusesSummary.Apply(input, Result(input), add: false);
        Assert.Equal(input, result.Text);
        Assert.All(result.Findings, f => Assert.Equal(StatusNoteKind.SummaryTwoOrMore, Assert.Single(f.Notes).Kind));
    }

    // With no species table rows, the {{IUCN status}} templates are counted.
    [Fact]
    public void Without_rows_status_templates_are_counted() {
        var input = "{{IUCN statuses|lc=0}}\n* ''Alpha one'' {{IUCN status|LC|1/2|1}}\n* ''Alpha two'' {{IUCN status|VU}}\n";
        var result = IucnStatusesSummary.Apply(input, Result(input), add: false);
        Assert.StartsWith("{{IUCN statuses|lc=1|vu=1}}", result.Text);
        Assert.Equal("templates", result.Findings.Single().Notes.Single(n => n.Kind == StatusNoteKind.SummaryCountedFrom).Detail);
    }

    [Fact]
    public void Nothing_to_count_leaves_the_box() {
        var input = "{{IUCN statuses|lc=4}}\nNo list here.\n";
        var result = IucnStatusesSummary.Apply(input, Result(input), add: false);
        Assert.Equal(input, result.Text);
        Assert.Equal(StatusNoteKind.SummaryNothingToCount, Assert.Single(Assert.Single(result.Findings).Notes).Kind);
    }

    [Fact]
    public void A_list_without_a_box_gets_one_under_Conventions_only_when_asked() {
        var input = List("", Rows);
        var offered = IucnStatusesSummary.Apply(input, Result(input), add: false);
        Assert.True(offered.SummaryMissing);
        Assert.Equal(input, offered.Text);

        var added = IucnStatusesSummary.Apply(input, Result(input), add: true);
        Assert.Contains("==Conventions==\n{{IUCN statuses|ex=0|ew=0|cr=1|en=1|vu=0|nt=1|lc=2|dd=1|ne=2}}\nThe author citation", added.Text);
        var finding = Assert.Single(added.Findings);
        Assert.Equal((StatusOutcome.Updated, 4, ""), (finding.Outcome, finding.Line, finding.Before));
        Assert.Equal("Conventions", finding.Notes.Single(n => n.Kind == StatusNoteKind.SummaryAdded).Detail);
    }

    [Fact]
    public void Without_Conventions_the_box_goes_under_the_heading_above_the_table() {
        var input = "Intro.\n\n==Species==\n{{Species table|genus=Alpha}}\n" + Row("One", "LC") + "|}\n";
        var added = IucnStatusesSummary.Apply(input, Result(input), add: true);
        Assert.StartsWith("Intro.\n\n==Species==\n{{IUCN statuses|ex=0|ew=0|cr=0|en=0|vu=0|nt=0|lc=1|dd=0|ne=0}}\n{{Species table", added.Text);
    }

    // The finding goes among the others by line.
    [Fact]
    public void The_finding_is_put_in_line_order() {
        var input = List("{{IUCN statuses|lc=0}}\n", Rows);
        var before = new StatusFinding(StatusItemKind.SpeciesTableRow, 2, StatusOutcome.Current, "x", null, null, []);
        var after = new StatusFinding(StatusItemKind.SpeciesTableRow, 9, StatusOutcome.Current, "y", null, null, []);
        var result = IucnStatusesSummary.Apply(input, new StatusUpdateResult(input, [before, after], 0, []), add: false);
        Assert.Equal([2, 4, 9], result.Findings.Select(f => f.Line));
    }

    [Fact]
    public void Edit_summary_says_the_box_changed() {
        var input = List("{{IUCN statuses|lc=0}}\n", Rows);
        var result = IucnStatusesSummary.Apply(input, Result(input), add: false);
        Assert.Equal("IUCN Red List 2026-1: {{IUCN statuses}} counts updated (assisted by Beastie Bot Species Status)",
            EditSummary.For(result, "2026-1"));
    }
}
