using System.Collections.Generic;
using BeastieBot3.Shared.Wikitext;

// IUCN Red List category codes <-> the items Wikidata allows as values of P141 (IUCN conservation
// status), for the status dry run. The table and the reasons behind it (LR/nt and LR/lc as NT and
// LC, no value for LR/cd, the disallowed values in use) are in BeastieBot3.Shared
// (Wikitext/WikidataStatusValues.cs), which the public site's QuickStatements batches use too.

namespace BeastieBot3.WikidataEdits;

internal static class WikidataIucnStatusValues {
    public const string CheckedOn = WikidataStatusValues.CheckedOn;
    public const long CheckedPropertyRevision = WikidataStatusValues.CheckedPropertyRevision;

    public const string ExtinctQid = WikidataStatusValues.ExtinctQid;
    public const string ExtinctInTheWildQid = WikidataStatusValues.ExtinctInTheWildQid;
    public const string CriticallyEndangeredQid = WikidataStatusValues.CriticallyEndangeredQid;
    public const string EndangeredQid = WikidataStatusValues.EndangeredQid;
    public const string VulnerableQid = WikidataStatusValues.VulnerableQid;
    public const string NearThreatenedQid = WikidataStatusValues.NearThreatenedQid;
    public const string LeastConcernQid = WikidataStatusValues.LeastConcernQid;
    public const string DataDeficientQid = WikidataStatusValues.DataDeficientQid;
    public const string NotEvaluatedQid = WikidataStatusValues.NotEvaluatedQid;

    public const string ConservationDependentQid = WikidataStatusValues.ConservationDependentQid;
    public const string LowerRiskQid = WikidataStatusValues.LowerRiskQid;
    public const string PossiblyExtinctInTheWildQid = WikidataStatusValues.PossiblyExtinctInTheWildQid;
    public const string CzechEndangeredQid = WikidataStatusValues.CzechEndangeredQid;

    public static IReadOnlyList<WikidataStatusValue> AllowedValues => WikidataStatusValues.AllowedValues;

    public static IReadOnlyList<WikidataStatusValue> DisallowedValues => WikidataStatusValues.DisallowedValues;

    /// See WikidataStatusValues.QidForCode.
    public static string? QidForCode(string iucnCode) => WikidataStatusValues.QidForCode(iucnCode);

    public static string? CodeForQid(string qid) => WikidataStatusValues.CodeForQid(qid);

    public static bool IsAllowedValue(string qid) => WikidataStatusValues.IsAllowedValue(qid);

    public static WikidataStatusValue? Describe(string qid) => WikidataStatusValues.Describe(qid);

    public static bool IsLowerRiskCode(string iucnCode) => WikidataStatusValues.IsLowerRiskCode(iucnCode);
}
