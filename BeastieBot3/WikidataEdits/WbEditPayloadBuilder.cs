using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text.Json.Nodes;

// Turns planned actions into the JSON that Wikidata's wbeditentity API takes, built on the item
// snapshot the plan was made from. A changed statement is sent whole (wbeditentity replaces a
// statement by id), so it starts as a copy of the cached statement; the edit carries the cached
// revision as baserevid, and Wikidata rejects it if the item has changed since.
//
// Ids that don't exist yet (the 2026.1 release item, an assessment item still to be created) are
// written as "CREATE:..." placeholders with no numeric-id. A dry-run payload with a placeholder is
// for reading, not sending; applying resolves them first.

namespace BeastieBot3.WikidataEdits;

internal sealed record WbEdit(string Qid, long BaseRevId, string Summary, JsonObject Data);

internal static class WbEditPayloadBuilder {
    private static readonly Regex ItemId = new(@"^Q[1-9][0-9]*$", RegexOptions.CultureInvariant);

    public static WbEdit Build(
        WdTaxonItem item,
        IucnGlobalAssessment assessment,
        StatusEditPlan plan,
        EditVariant variant,
        WikidataIucnEditConfig config) {
        var changed = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        var added = new List<JsonObject>();
        var all = item.ConservationStatuses.Concat(item.IucnTaxonIds).ToDictionary(s => s.Id, StringComparer.Ordinal);

        JsonObject Existing(string statementId) {
            if (!changed.TryGetValue(statementId, out var statement)) {
                statement = (JsonObject)all[statementId].Raw.DeepClone();
                changed[statementId] = statement;
            }
            return statement;
        }

        foreach (var action in plan.Actions.TryGetValue(variant, out var list) ? list : Array.Empty<PlannedAction>()) {
            switch (action) {
                case AddTaxonIdClaim a:
                    added.Add(Statement(StringSnak("P627", a.TaxonId.ToString(CultureInfo.InvariantCulture)), "normal",
                        ReleaseReference(assessment, config, includeTaxonId: false, includeUrl: false)));
                    break;
                case DeprecateTaxonIdClaim d: {
                    var s = Existing(d.StatementId);
                    s["rank"] = "deprecated";
                    AddQualifier(s, ItemSnak("P2241", IucnStatusEditPlanner.WithdrawnIdentifierValue));
                    break;
                }
                case AddStatusStatement a:
                    added.Add(Statement(ItemSnak("P141", a.ValueQid), a.Rank,
                        ReleaseReference(assessment, config, includeTaxonId: true, includeUrl: config.ReferenceAssessmentUrl),
                        AssessmentReference(plan.AssessmentRef)));
                    break;
                case AddStatusReferences r: {
                    var s = Existing(r.StatementId);
                    var refs = s["references"] as JsonArray ?? new JsonArray();
                    if (r.ReleaseReference) refs.Add(ReleaseReference(assessment, config, includeTaxonId: true, includeUrl: config.ReferenceAssessmentUrl));
                    if (r.AssessmentReference) refs.Add(AssessmentReference(plan.AssessmentRef));
                    s["references"] = refs;
                    break;
                }
                case SetStatusRank r:
                    Existing(r.StatementId)["rank"] = r.Rank;
                    break;
                case ReplaceStatusValue r: {
                    var s = Existing(r.StatementId);
                    s["mainsnak"] = ItemSnak("P141", r.ValueQid);
                    s["references"] = new JsonArray(
                        ReleaseReference(assessment, config, includeTaxonId: true, includeUrl: config.ReferenceAssessmentUrl),
                        AssessmentReference(plan.AssessmentRef));
                    break;
                }
            }
        }

        var claims = new JsonArray();
        foreach (var s in changed.Values) claims.Add(s);
        foreach (var s in added) claims.Add(s);
        return new WbEdit(item.Qid, item.LastRevId, Summary(assessment, config), new JsonObject { ["claims"] = claims });
    }

    public static string Summary(IucnGlobalAssessment a, WikidataIucnEditConfig config) =>
        config.EditSummary
            .Replace("{release}", config.Release)
            .Replace("{code}", a.CategoryCode)
            .Replace("{taxon_id}", a.TaxonId.ToString(CultureInfo.InvariantCulture))
            .Replace("{assessment_id}", a.AssessmentId.ToString(CultureInfo.InvariantCulture));

    // ------------------------------------------------------------ references

    /// stated in: the release; IUCN taxon ID; the assessment page (carries the assessment id); retrieved.
    public static JsonObject ReleaseReference(IucnGlobalAssessment a, WikidataIucnEditConfig config, bool includeTaxonId, bool includeUrl) {
        var snaks = new List<JsonObject> { ItemSnak("P248", config.EditionRef) };
        if (includeTaxonId) snaks.Add(StringSnak("P627", a.TaxonId.ToString(CultureInfo.InvariantCulture)));
        if (includeUrl) snaks.Add(StringSnak("P854", a.Url));
        snaks.Add(TimeSnak("P813", a.DownloadedAtUtc, precision: 11));
        return Reference(snaks);
    }

    /// stated in: the item for this assessment publication.
    public static JsonObject AssessmentReference(string assessmentRef) => Reference(new[] { ItemSnak("P248", assessmentRef) });

    private static JsonObject Reference(IEnumerable<JsonObject> snakList) {
        var snaks = new JsonObject();
        var order = new JsonArray();
        foreach (var snak in snakList) {
            var property = (string)snak["property"]!;
            if (snaks[property] is not JsonArray values) {
                values = new JsonArray();
                snaks[property] = values;
                order.Add(property);
            }
            values.Add(snak);
        }
        return new JsonObject { ["snaks"] = snaks, ["snaks-order"] = order };
    }

    // ------------------------------------------------------------ statements and snaks

    public static JsonObject Statement(JsonObject mainsnak, string rank, params JsonObject[] references) {
        var statement = new JsonObject {
            ["mainsnak"] = mainsnak,
            ["type"] = "statement",
            ["rank"] = rank,
        };
        if (references.Length > 0) statement["references"] = new JsonArray(references.Cast<JsonNode>().ToArray());
        return statement;
    }

    public static void AddQualifier(JsonObject statement, JsonObject snak) {
        var property = (string)snak["property"]!;
        var qualifiers = statement["qualifiers"] as JsonObject ?? new JsonObject();
        var order = statement["qualifiers-order"] as JsonArray ?? new JsonArray();
        if (qualifiers[property] is not JsonArray values) {
            values = new JsonArray();
            qualifiers[property] = values;
            order.Add(property);
        }
        values.Add(snak);
        statement["qualifiers"] = qualifiers;
        statement["qualifiers-order"] = order;
    }

    public static JsonObject ItemSnak(string property, string idOrPlaceholder) {
        var value = new JsonObject { ["entity-type"] = "item" };
        if (ItemId.IsMatch(idOrPlaceholder)) {
            value["numeric-id"] = long.Parse(idOrPlaceholder.AsSpan(1), CultureInfo.InvariantCulture);
        }
        value["id"] = idOrPlaceholder;
        return Snak(property, value, "wikibase-entityid");
    }

    public static JsonObject StringSnak(string property, string value) => Snak(property, JsonValue.Create(value), "string");

    public static JsonObject MonolingualSnak(string property, string text, string language) =>
        Snak(property, new JsonObject { ["text"] = text, ["language"] = language }, "monolingualtext");

    /// precision 11 = day, 9 = year.
    public static JsonObject TimeSnak(string property, DateTime date, int precision) {
        var time = precision >= 11
            ? $"+{date:yyyy-MM-dd}T00:00:00Z"
            : $"+{date:yyyy}-00-00T00:00:00Z";
        return Snak(property, new JsonObject {
            ["time"] = time,
            ["timezone"] = 0,
            ["before"] = 0,
            ["after"] = 0,
            ["precision"] = precision,
            ["calendarmodel"] = "http://www.wikidata.org/entity/Q1985727",
        }, "time");
    }

    private static JsonObject Snak(string property, JsonNode? value, string type) => new() {
        ["snaktype"] = "value",
        ["property"] = property,
        ["datavalue"] = new JsonObject { ["value"] = value, ["type"] = type },
    };
}
