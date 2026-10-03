namespace BeastieBot3.Shared.Wikitext;

// IUCN Red List category codes <-> the items Wikidata allows as values of P141 (IUCN conservation
// status). Shared by the Wikidata status dry run (BeastieBot3/WikidataEdits, through
// WikidataIucnStatusValues) and the public site's QuickStatements batches (WikidataStatusEdit).
//
// Taken from the "one-of constraint" (Q21510859, status: mandatory) on Property:P141, checked
// 2026-09-13 at property revision 2507262614 (last modified 2026-06-18). Its allowed values are
// exactly the nine below plus novalue, with English labels from wbgetentities the same day.
//
// Wikidata has no allowed values for the IUCN 2.3 "Lower Risk" subcategories:
//   - LR/nt and LR/lc map to the NT and LC values. IUCN's own summary tables count them that way
//     ("NT or LR/nt", "LC or LR/lc"), and so does the cache: on items whose P627 claim is a taxon
//     IUCN 2026-1 lists as LR/nt, all 500 P141 statements say NT; for LR/lc, all 218 say LC.
//   - LR/cd has no mapping (QidForCode returns null). Q158862 "conservation dependent" exists but is
//     not an allowed value, and IUCN keeps LR/cd in a column of its own. On items for LR/cd taxa,
//     122 of 125 P141 statements say LC, 2 say Q6693756 "Lower Risk" and 1 says VU. The Wikipedia
//     list generator folds LR/cd into NT; whether an edit should do the same is the planner's
//     decision, not this map's.
//
// "Possibly extinct" is a flag on CR, not a category, so it is not mapped either. Q85304919
// "possibly extinct in the wild" exists, is not an allowed value, and was on no cached statement.
//
// Values in use in the cache (wikidata_p141_statements, 2026-09-13) that are not allowed:
// Q6693756 "Lower Risk" on 2 items (Q2388306, Q132198534) and Q56660246, the Czech Red List's
// "endangered", on 1 item (Q104248290). One P141 statement is novalue (Q215836, qualified with
// P585 2021), which the constraint permits.

/// One Wikidata item used, or usable, as a P141 value. LabelEn is Wikidata's English label
/// ("critically endangered", but "Data Deficient"). IucnCode is the IUCN code it stands for; null
/// for items that aren't a Red List category.
public sealed record WikidataStatusValue(string Qid, string LabelEn, string? IucnCode, bool Allowed);

public static class WikidataStatusValues {
    public const string CheckedOn = "2026-09-13";
    public const long CheckedPropertyRevision = 2507262614;

    public const string ExtinctQid = "Q237350";
    public const string ExtinctInTheWildQid = "Q239509";
    public const string CriticallyEndangeredQid = "Q219127";
    public const string EndangeredQid = "Q96377276";
    public const string VulnerableQid = "Q278113";
    public const string NearThreatenedQid = "Q719675";
    public const string LeastConcernQid = "Q211005";
    public const string DataDeficientQid = "Q3245245";
    public const string NotEvaluatedQid = "Q3350324";

    /// Not allowed as a P141 value; LR/cd in IUCN's 2.3 categories.
    public const string ConservationDependentQid = "Q158862";
    /// Not allowed; "former IUCN Red List category", in use on 2 cached items.
    public const string LowerRiskQid = "Q6693756";
    /// Not allowed; no cached statement uses it.
    public const string PossiblyExtinctInTheWildQid = "Q85304919";
    /// Not allowed; the Czech Republic Red List's "endangered", in use on 1 cached item.
    public const string CzechEndangeredQid = "Q56660246";

    /// The allowed values, in Red List category order.
    public static IReadOnlyList<WikidataStatusValue> AllowedValues { get; } = new[] {
        new WikidataStatusValue(ExtinctQid, "extinct", "EX", true),
        new WikidataStatusValue(ExtinctInTheWildQid, "extinct in the wild", "EW", true),
        new WikidataStatusValue(CriticallyEndangeredQid, "critically endangered", "CR", true),
        new WikidataStatusValue(EndangeredQid, "endangered", "EN", true),
        new WikidataStatusValue(VulnerableQid, "vulnerable", "VU", true),
        new WikidataStatusValue(NearThreatenedQid, "near threatened", "NT", true),
        new WikidataStatusValue(LeastConcernQid, "least concern", "LC", true),
        new WikidataStatusValue(DataDeficientQid, "Data Deficient", "DD", true),
        new WikidataStatusValue(NotEvaluatedQid, "not evaluated", "NE", true),
    };

    /// Items that turn up, or could, as P141 values but violate the constraint.
    public static IReadOnlyList<WikidataStatusValue> DisallowedValues { get; } = new[] {
        new WikidataStatusValue(ConservationDependentQid, "conservation dependent", "LR/cd", false),
        new WikidataStatusValue(LowerRiskQid, "Lower Risk", null, false),
        new WikidataStatusValue(PossiblyExtinctInTheWildQid, "possibly extinct in the wild", null, false),
        new WikidataStatusValue(CzechEndangeredQid, "endangered (Czech Republic Red List)", null, false),
    };

    private static readonly Dictionary<string, string> QidByCode = BuildQidByCode();
    private static readonly Dictionary<string, WikidataStatusValue> ByQid = BuildByQid();

    /// The P141 value to write for an IUCN code. Case-insensitive; accepts the 3.1 codes, NE, and
    /// the 2.3 codes LR/nt and LR/lc (as NT and LC). Null for LR/cd and for anything with no
    /// allowed value (RE, NA, "CR(PE)", unknown text): pass the plain category code.
    public static string? QidForCode(string? iucnCode) {
        if (string.IsNullOrWhiteSpace(iucnCode)) {
            return null;
        }

        return QidByCode.TryGetValue(iucnCode.Trim(), out var qid) ? qid : null;
    }

    /// The IUCN code an allowed P141 value stands for (EX, EW, CR, EN, VU, NT, LC, DD, NE).
    /// Null for anything else, including the disallowed values: use Describe for those.
    public static string? CodeForQid(string? qid) {
        if (string.IsNullOrWhiteSpace(qid)) {
            return null;
        }

        return ByQid.TryGetValue(qid.Trim(), out var value) && value.Allowed ? value.IucnCode : null;
    }

    /// True when the constraint allows the item as a P141 value.
    public static bool IsAllowedValue(string? qid) =>
        !string.IsNullOrWhiteSpace(qid) && ByQid.TryGetValue(qid.Trim(), out var value) && value.Allowed;

    /// Any item listed here, allowed or not; null for an item this map doesn't know.
    public static WikidataStatusValue? Describe(string? qid) =>
        !string.IsNullOrWhiteSpace(qid) && ByQid.TryGetValue(qid.Trim(), out var value) ? value : null;

    /// IUCN 2.3 codes (LR/cd, LR/nt, LR/lc), which a current assessment can still carry.
    public static bool IsLowerRiskCode(string? iucnCode) =>
        !string.IsNullOrWhiteSpace(iucnCode) && iucnCode.Trim().StartsWith("LR/", StringComparison.OrdinalIgnoreCase);

    private static Dictionary<string, string> BuildQidByCode() {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var value in AllowedValues) {
            map[value.IucnCode!] = value.Qid;
        }

        map["LR/nt"] = NearThreatenedQid;
        map["LR/lc"] = LeastConcernQid;
        return map;
    }

    private static Dictionary<string, WikidataStatusValue> BuildByQid() {
        var map = new Dictionary<string, WikidataStatusValue>(StringComparer.OrdinalIgnoreCase);
        foreach (var value in AllowedValues) {
            map[value.Qid] = value;
        }

        foreach (var value in DisallowedValues) {
            map[value.Qid] = value;
        }

        return map;
    }
}
