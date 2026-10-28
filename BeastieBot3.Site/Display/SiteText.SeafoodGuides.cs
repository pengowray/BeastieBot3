using System.Globalization;
using BeastieBot3.Shared.SiteData;

namespace BeastieBot3.Site.Display;

// The species page's section of seafood guides' ratings (seafood_rating, SeafoodGuideSection).
public static partial class SiteText {
    public const string HeadingSeafoodGuides = "Seafood guide ratings";
    public const string SeafoodGuidesIntro =
        "Each rating is a seafood guide's advice to shoppers about buying this species when it is caught or farmed in a particular place by a particular method. "
        + "The guide judges the fishery or farm by things such as the health of the fish stock, bycatch, habitat damage and management, not by the species' risk of extinction.";

    /// The heading of a guide's table: the guide and its country.
    public static string SeafoodGuideHeading(string guide) => guide switch {
        SeafoodGuides.GoodFish => "GoodFish (Australia)",
        SeafoodGuides.SeafoodWatch => "Seafood Watch (United States)",
        _ => guide,
    };

    public const string ColSeafoodRating = "Rating";
    public const string ColSeafoodWhere = "Where caught or farmed";
    public const string ColSeafoodMethod = "Method";
    /// The fishery column: GoodFish lists fisheries and farms, Seafood Watch fisheries and fish stocks.
    public static string ColSeafoodFishery(string guide) => guide == SeafoodGuides.GoodFish ? "Fishery or farm" : "Fishery or stock";
    public const string ColSeafoodName = "Name in guide";
    public const string ColSeafoodNameTitle = "The guide's name for this seafood, linked to the guide's page for this rating.";
    /// Before GoodFish's note, on its own line under the rating it belongs to.
    public const string SeafoodNoteLabel = "GoodFish note:";
    /// GoodFish's date: when its page last changed, which is not the date of an assessment.
    public const string ColSeafoodPageChanged = "Page last changed";
    public const string ColSeafoodPageChangedTitle =
        "The date GoodFish last changed its web page for this rating. The rating may have been decided before this date.";
    public const string SeafoodListedNameTitle = "The scientific name the guide uses, which is different from the one on this page.";

    /// In the "Where caught or farmed" column, when the guide gives no place; and in the fishery
    /// column, when the guide gives no fishery or stock.
    public const string SeafoodNotGiven = "not given";

    /// After a Seafood Watch method: whether it is wild-caught or farmed. production: 'wild',
    /// 'farmed' or 'wild|farmed'.
    public static string? SeafoodProduction(string? production) => production switch {
        "wild" => "wild",
        "farmed" => "farmed",
        "wild|farmed" => "wild and farmed",
        _ => null,
    };

    /// The start of the line that opens a guide's closed table: "55 ratings:", "1 rating:". The
    /// count of each rating follows it.
    public static string SeafoodRatingsTotal(int total) =>
        total == 1 ? "1 rating:" : $"{total.ToString("N0", CultureInfo.InvariantCulture)} ratings:";

    /// The link to a guide's website in its note, and the website.
    public const string GoodFishFullName = "GoodFish, Australia's Sustainable Seafood Guide";
    public const string GoodFishUrl = "https://goodfish.org.au/";
    public const string SeafoodWatchName = "Seafood Watch";
    public const string SeafoodWatchUrl = "https://www.seafoodwatch.org/";

    /// The note under the GoodFish table, in two parts around the link to GoodFish: "GoodFish
    /// ratings were downloaded on 28 October 2026 from [GoodFish, Australia's Sustainable Seafood
    /// Guide], published by the Australian Marine Conservation Society. GoodFish does not give the
    /// date of each rating; ...". downloaded: "28 October 2026", or null when unknown. pageDates: the
    /// table has the "Page last changed" column.
    public static string GoodFishNoteStart(string? downloaded) =>
        downloaded is null ? "GoodFish ratings are from " : $"GoodFish ratings were downloaded on {downloaded} from ";
    public static string GoodFishNoteEnd(bool pageDates) =>
        ", published by the Australian Marine Conservation Society. GoodFish does not give the date of each rating"
        + (pageDates ? "; the dates in the table are when GoodFish last changed each rating's web page." : ".");

    /// The note under the Seafood Watch table, in two parts around the link to Seafood Watch.
    public static string SeafoodWatchNoteStart(string? downloaded) =>
        downloaded is null ? "Seafood Watch ratings are from " : $"Seafood Watch ratings were downloaded on {downloaded} from ";
    public const string SeafoodWatchNoteEnd =
        ", published by the Monterey Bay Aquarium. The date of each rating is in the rating's full report on the Seafood Watch website.";
}
