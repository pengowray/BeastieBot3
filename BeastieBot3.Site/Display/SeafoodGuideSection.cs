using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;

namespace BeastieBot3.Site.Display;

/// The colour of a seafood guide's rating label: each guide's own colour for the rating. Amber is
/// GoodFish's "Eat Less" and Seafood Watch's yellow "Good Alternative"; grey is GoodFish's "Under
/// Review" and any rating the site does not know.
public enum SeafoodRatingColour { Green, Amber, Red, Blue, Grey }

/// One rating as a coloured label, with the guide's words.
public sealed record SeafoodRatingLabel(string Text, SeafoodRatingColour Colour) {
    /// The label's CSS class (site.css, .seafood-rating-*).
    public string CssClass => "seafood-rating seafood-rating-" + Colour.ToString().ToLowerInvariant();

    /// Where the label sorts: Seafood Watch's and GoodFish's own scale from green to red, then
    /// Seafood Watch's blue Certified (an eco-certification, apart from its own scale), then grey.
    public int Order => (int)Colour;
}

/// A cell's lines; NoValue: the guide gives none, and the cell shows "not given".
public sealed record SeafoodCell(IReadOnlyList<string> Lines, bool NoValue = false) {
    public static readonly SeafoodCell NotGiven = new([SiteText.SeafoodNotGiven], NoValue: true);
}

/// One row of a guide's table. Ratings: one label, or two for a GoodFish page with two.
/// Certification: for a Seafood Watch Certified rating, who certified it ("Marine Stewardship
/// Council (MSC)", from "Marine Stewardship Council (MSC) Certified"), shown under the label. Name: the guide's name for the seafood, linked to
/// Url. ListedNameHtml: the guide's scientific name as HTML, when it is not the taxon's own. Note:
/// GoodFish's note, which on a page with two ratings usually says which to choose. PageChanged:
/// GoodFish's page change date ("18 July 2024").
public sealed record SeafoodRatingLine(
    IReadOnlyList<SeafoodRatingLabel> Ratings,
    string? Certification,
    SeafoodCell Where,
    SeafoodCell Method,
    SeafoodCell Fishery,
    string Name,
    string? ListedNameHtml,
    string Url,
    string? Note,
    string? PageChanged);

/// How many of a guide's ratings on the page have one rating.
public sealed record SeafoodRatingCount(SeafoodRatingLabel Label, int Count);

/// One guide's table. A column after Rating and Where shows only when one of the rows has a value for
/// it. A row's note is on a line of its own under the row. Counts: the number of each rating, for the
/// line that opens a closed table. Collapsed: the table has more rows than SeafoodGuideSection.OpenRows
/// and starts closed under that line. Note: the guide's note (its name linked to its website, its
/// publisher, the download date and where its dates are), shown under the table; empty for a guide
/// the site does not know.
public sealed record SeafoodGuideTable(
    string Guide,
    string Heading,
    string FisheryHeading,
    bool ShowMethod,
    bool ShowFishery,
    bool ShowPageChanged,
    IReadOnlyList<SeafoodRatingLine> Rows,
    string TotalText,
    IReadOnlyList<SeafoodRatingCount> Counts,
    bool Collapsed,
    IReadOnlyList<NoteSegment> Note) {
    /// The number of columns, for the row of a note, which spans them all.
    public int ColumnCount => 3 + (ShowMethod ? 1 : 0) + (ShowFishery ? 1 : 0) + (ShowPageChanged ? 1 : 0);
}

/// The species page's "Seafood guide ratings" section: one table per guide (GoodFish, then Seafood
/// Watch), each with its note under it. Never mixed with the other conservation statuses: a rating
/// judges a fishery or farm, not the species.
public sealed record SeafoodGuideSection(IReadOnlyList<SeafoodGuideTable> Tables) {
    /// The most rows a table shows open. Most taxa have 1 to 5 ratings in a guide, and 10 rows fit on
    /// one screen; a few Seafood Watch species have 30 to 55 (yellowfin tuna 55), whose table would be
    /// several screens long, so it starts closed under the count of each rating.
    public const int OpenRows = 10;

    /// rows: the taxon's seafood_rating rows, in any order. downloaded: the date the site database's
    /// copy of a guide was downloaded ("28 October 2026"), or null when unknown. Null when there are no
    /// rows, so the page has no section.
    public static SeafoodGuideSection? Build(IReadOnlyList<SeafoodRatingRow> rows, Func<string, string?> downloaded) {
        if (rows.Count == 0) {
            return null;
        }
        var tables = rows
            .GroupBy(r => r.Guide)
            .OrderBy(g => GuideOrder(g.Key))
            .ThenBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => BuildTable(g.Key, g.ToList(), downloaded(g.Key)))
            .ToList();
        return new SeafoodGuideSection(tables);
    }

    private static int GuideOrder(string guide) => guide switch {
        SeafoodGuides.GoodFish => 0,
        SeafoodGuides.SeafoodWatch => 1,
        _ => 2,
    };

    private static SeafoodGuideTable BuildTable(string guide, IReadOnlyList<SeafoodRatingRow> rows, string? downloaded) {
        var lines = rows
            .Select(r => (Row: r, Line: BuildLine(r)))
            .OrderBy(x => x.Line.Ratings.Min(l => l.Order))
            .ThenBy(x => string.Join(" ", x.Line.Where.Lines), StringComparer.OrdinalIgnoreCase)
            .ThenBy(x => string.Join(" ", x.Line.Method.Lines), StringComparer.OrdinalIgnoreCase)
            .ThenBy(x => x.Line.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(x => x.Row.RatingId, StringComparer.Ordinal)
            .Select(x => x.Line)
            .ToList();
        var counts = lines
            .SelectMany(l => l.Ratings)
            .GroupBy(l => l)
            .OrderBy(g => g.Key.Order)
            .ThenBy(g => g.Key.Text, StringComparer.Ordinal)
            .Select(g => new SeafoodRatingCount(g.Key, g.Count()))
            .ToList();
        var showPageChanged = lines.Any(l => l.PageChanged is not null);
        return new SeafoodGuideTable(
            guide,
            SiteText.SeafoodGuideHeading(guide),
            SiteText.ColSeafoodFishery(guide),
            ShowMethod: lines.Any(l => !l.Method.NoValue),
            ShowFishery: lines.Any(l => !l.Fishery.NoValue),
            ShowPageChanged: showPageChanged,
            lines,
            // Two ratings on one GoodFish page count as two, so the total is the sum of the counts.
            SiteText.SeafoodRatingsTotal(counts.Sum(c => c.Count)),
            counts,
            Collapsed: lines.Count > OpenRows,
            Note(guide, downloaded, showPageChanged));
    }

    private static SeafoodRatingLine BuildLine(SeafoodRatingRow row) {
        var ratings = Split(row.Ratings).Select(text => Label(text, row.Color)).ToList();
        if (ratings.Count == 0) {
            ratings.Add(Label(row.Ratings, row.Color));
        }
        // A Seafood Watch Certified rating's fishery starts with the certification.
        var fishery = Split(row.Fishery);
        string? certification = null;
        if (ratings.Any(r => r.Colour == SeafoodRatingColour.Blue) && fishery.Count > 0
            && fishery[0].EndsWith("Certified", StringComparison.OrdinalIgnoreCase)) {
            certification = fishery[0][..^"Certified".Length].TrimEnd();
            fishery = fishery.Skip(1).ToList();
        }
        return new SeafoodRatingLine(
            ratings,
            certification,
            Where: Cell(new[] { row.Region, row.WaterBody }.Where(s => !string.IsNullOrWhiteSpace(s)).Select(s => s!.Trim()).ToList()),
            Method: Cell(MethodText(row) is { } method ? [method] : []),
            Fishery: Cell(fishery),
            row.CommonName,
            row.ListedName is { } listed ? OtherStatusSection.ListedNameHtml(listed) : null,
            row.Url,
            string.IsNullOrWhiteSpace(row.Note) ? null : row.Note.Trim(),
            SiteFormat.TryParseDate(row.Modified, out var modified) ? SiteFormat.Date(modified) : null);
    }

    private static SeafoodCell Cell(IReadOnlyList<string> lines) => lines.Count == 0 ? SeafoodCell.NotGiven : new SeafoodCell(lines);

    /// GoodFish's method as written ("Wild Caught", "Farmed"), which says whether it is wild or
    /// farmed. Seafood Watch's methods, joined, with whether they are wild or farmed:
    /// "Drifting longlines (wild)", "Boat dredges, Ropes (wild and farmed)".
    public static string? MethodText(SeafoodRatingRow row) {
        var methods = string.Join(", ", Split(row.Method));
        if (row.Guide == SeafoodGuides.GoodFish) {
            return methods.Length == 0 ? null : methods;
        }
        var production = SiteText.SeafoodProduction(row.Production);
        return (methods.Length, production) switch {
            (0, null) => null,
            (0, { } p) => char.ToUpperInvariant(p[0]) + p[1..],
            (_, null) => methods,
            _ => $"{methods} ({production})",
        };
    }

    /// The label of one rating: Seafood Watch's colour when the row has one, else the colour of the
    /// rating's words in either guide.
    public static SeafoodRatingLabel Label(string text, string? color) {
        text = text.Trim();
        var colour = color?.Trim().ToLowerInvariant() switch {
            "green" => SeafoodRatingColour.Green,
            "yellow" => SeafoodRatingColour.Amber,
            "red" => SeafoodRatingColour.Red,
            "blue" => SeafoodRatingColour.Blue,
            _ => text.ToLowerInvariant() switch {
                "better choice" or "best choice" => SeafoodRatingColour.Green,
                "eat less" or "good alternative" => SeafoodRatingColour.Amber,
                "say no" or "avoid" => SeafoodRatingColour.Red,
                "certified" => SeafoodRatingColour.Blue,
                _ => SeafoodRatingColour.Grey,
            },
        };
        return new SeafoodRatingLabel(text, colour);
    }

    private static List<string> Split(string? joined) =>
        joined is null ? [] : joined.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();

    /// The note under a guide's table: its name linked to its website, its publisher, the download
    /// date, and where its dates are. Empty for a guide the site does not know.
    private static IReadOnlyList<NoteSegment> Note(string guide, string? downloaded, bool pageDates) => guide switch {
        SeafoodGuides.GoodFish => [
            new(SiteText.GoodFishNoteStart(downloaded)),
            new(SiteText.GoodFishFullName, SiteText.GoodFishUrl),
            new(SiteText.GoodFishNoteEnd(pageDates)),
        ],
        SeafoodGuides.SeafoodWatch => [
            new(SiteText.SeafoodWatchNoteStart(downloaded)),
            new(SiteText.SeafoodWatchName, SiteText.SeafoodWatchUrl),
            new(SiteText.SeafoodWatchNoteEnd),
        ],
        _ => [],
    };
}
