using System;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using BeastieBot3.Shared.Wikitext;

// The item that would be created for one IUCN assessment publication when Wikidata has none, so a
// P141 statement can cite that assessment in full. Modelled on the ~5,000 assessment items already
// there (scholarly articles with published in = IUCN Red List and main subject = the taxon), with
// authors as name strings in citation order. Class, label and title language come from config,
// because they are what the modelling proposal to WikiProject Taxonomy has to settle.

namespace BeastieBot3.WikidataEdits;

internal static class AssessmentItemPayloadBuilder {
    public static JsonObject Build(IucnGlobalAssessment a, string taxonItemQid, WikidataIucnEditConfig config) {
        var c = config.AssessmentItem;
        var year = a.YearPublished?.ToString(CultureInfo.InvariantCulture) ?? "";
        string Fill(string template) => template
            .Replace("{name}", a.ScientificName)
            .Replace("{year}", year)
            .Replace("{taxon_id}", a.TaxonId.ToString(CultureInfo.InvariantCulture))
            .Replace("{assessment_id}", a.AssessmentId.ToString(CultureInfo.InvariantCulture));

        var claims = new JsonArray {
            WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.ItemSnak("P31", c.InstanceOf), "normal"),
            WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.MonolingualSnak("P1476", a.ScientificName, c.TitleLanguage), "normal"),
            WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.ItemSnak("P1433", config.RedListItem), "normal"),
            WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.ItemSnak("P123", c.Publisher), "normal"),
            WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.ItemSnak("P921", taxonItemQid), "normal"),
            WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.ItemSnak("P407", c.Language), "normal"),
            WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.StringSnak("P953", a.Url), "normal"),
        };
        if (a.YearPublished is { } y) {
            claims.Add(WbEditPayloadBuilder.Statement(
                WbEditPayloadBuilder.TimeSnak("P577", new DateTime(y, 1, 1, 0, 0, 0, DateTimeKind.Utc), precision: 9), "normal"));
        }
        if (!string.IsNullOrWhiteSpace(a.Doi)) {
            // Wikidata stores DOIs upper-case.
            claims.Add(WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.StringSnak("P356", a.Doi.ToUpperInvariant()), "normal"));
        }

        var ordinal = 0;
        var authorTypes = c.AuthorCreditTypes;
        foreach (var credit in authorTypes.SelectMany(t => a.Credits.Where(cr => string.Equals(cr.Type, t, StringComparison.OrdinalIgnoreCase)))) {
            ordinal++;
            // An organisation with an item (IucnAuthorItems) is P50, named as printed (P1932).
            var authorItem = IucnAuthorItems.ItemFor(credit.Name);
            var author = authorItem is null
                ? WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.StringSnak("P2093", credit.Name), "normal")
                : WbEditPayloadBuilder.Statement(WbEditPayloadBuilder.ItemSnak("P50", authorItem), "normal");
            WbEditPayloadBuilder.AddQualifier(author, WbEditPayloadBuilder.StringSnak("P1545", ordinal.ToString(CultureInfo.InvariantCulture)));
            if (authorItem is not null) {
                WbEditPayloadBuilder.AddQualifier(author, WbEditPayloadBuilder.StringSnak("P1932", credit.Name.Trim()));
            }
            claims.Add(author);
        }

        return new JsonObject {
            ["labels"] = new JsonObject { ["en"] = new JsonObject { ["language"] = "en", ["value"] = Fill(c.Label) } },
            ["descriptions"] = new JsonObject { ["en"] = new JsonObject { ["language"] = "en", ["value"] = Fill(c.Description) } },
            ["claims"] = claims,
        };
    }
}
