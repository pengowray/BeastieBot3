using System.Text.Json;
using BeastieBot3.Shared.Wikitext;

namespace BeastieBot3.Site.Pages;

/// The citation template the site writes for an IUCN assessment.
public enum ReferenceTemplate {
    /// {{cite iucn}}, from the citation parts.
    CiteIucn,
    /// {{cite Q}} when the assessment has a Wikidata item, else {{cite iucn}}.
    CiteQ,
}

/// The one place that picks between {{cite iucn}} and {{cite Q}} for an assessment: the group
/// page's species tables, the taxonbox status_ref on taxon pages and the status update page all
/// call Render. The query value is "cite=q" on every page; "cite=iucn" or no value is the default.
public static class IucnReference {
    public const string QueryKey = "cite";
    public const string CiteQValue = "q";
    public const string CiteIucnValue = "iucn";

    public static ReferenceTemplate FromQuery(string? value) =>
        string.Equals(value?.Trim(), CiteQValue, StringComparison.OrdinalIgnoreCase) ? ReferenceTemplate.CiteQ : ReferenceTemplate.CiteIucn;

    public static string QueryValue(ReferenceTemplate template) => template == ReferenceTemplate.CiteQ ? CiteQValue : CiteIucnValue;

    /// Whether Render writes {{cite Q}} for an assessment with this item.
    public static bool UsesCiteQ(ReferenceTemplate template, string? itemQid) =>
        template == ReferenceTemplate.CiteQ && ItemId(itemQid) is not null;

    /// The citation, wrapped in <ref> as the options say; null when neither template can be made
    /// (no citation parts and no item, or {{cite iucn}} chosen and no citation parts).
    /// itemProperties is assessment.wikidata_item_properties: {{cite Q}} gets |access-date= only when
    /// the item has a full work URL (P953), because CS1 reports an access date without a URL.
    public static string? Render(ReferenceTemplate template, IucnCitationParts? parts, string? itemQid, string? itemProperties,
        CiteIucnOptions citeIucn, CiteQOptions citeQ) {
        if (UsesCiteQ(template, itemQid)) {
            var hasUrl = (itemProperties ?? string.Empty).Split(' ', StringSplitOptions.RemoveEmptyEntries).Contains("P953");
            return WikidataCitation.CiteQ(ItemId(itemQid)!, citeQ with { ItemHasUrl = hasUrl });
        }
        return parts is null ? null : CiteIucnRenderer.Render(parts, citeIucn);
    }

    /// The citation parts in assessment.citation_json; null when there are none or they cannot be read.
    public static IucnCitationParts? ReadParts(string? json) {
        try {
            return IucnCitationParts.FromJson(json);
        } catch (JsonException) {
            return null;
        }
    }

    // "Q123" when the value is an item id, else null.
    private static string? ItemId(string? qid) {
        var value = qid?.Trim();
        return value is { Length: > 1 } && (value[0] is 'Q' or 'q') && value[1..].All(char.IsAsciiDigit) ? "Q" + value[1..] : null;
    }
}
