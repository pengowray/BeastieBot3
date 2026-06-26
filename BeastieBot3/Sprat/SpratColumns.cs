using System;
using System.Collections.Generic;

// Canonical column contract for the imported SPRAT table. The SPRAT report CSV ships with a
// two-row header (a category-grouping row followed by the real column-name row) and messy header
// text (spaces, dots, parentheses, and EIGHT identically-named "Listed Name" columns). Rather than
// depend on the exact output of a generic sanitiser, SpratImporter maps the columns it cares about
// to the stable names declared here, so the query layer (SpratListQueryService) and tests can rely
// on them. Unknown/uninteresting headers (the per-act "Listed Name" columns, presence-by-territory,
// migratory-agreement columns, …) are auto-sanitised and de-duplicated by the importer.

namespace BeastieBot3.Sprat;

/// <summary>Stable destination table + column names for the imported SPRAT dataset.</summary>
internal static class SpratColumns {
    public const string Table = "sprat_species";

    // Identity + taxonomy
    public const string SpratTaxonId = "sprat_taxon_id";
    public const string ScientificName = "scientific_name";
    public const string CommonName = "common_name";
    public const string Kingdom = "kingdom";
    public const string Phylum = "phylum";
    public const string ClassName = "class_name";
    public const string OrderName = "order_name";
    public const string Family = "family";
    public const string Genus = "genus";
    public const string TaxonGroup = "taxon_group";
    /// <summary>The IUCN-side accepted name SPRAT carries, used to resolve an IUCN assessment id when the
    /// SPRAT scientific name doesn't match the IUCN release directly (synonyms, spelling drift).</summary>
    public const string IucnListedName = "IUCN_Red_List_Listed_Names";

    // Conservation status, one column per listing system. Each holds the system's own raw category
    // text (e.g. "Critically Endangered", "Rare", "Near Threatened") or NULL/empty when unlisted.
    public const string EpbcStatus = "epbc_status";
    public const string IucnStatus = "iucn_status";
    public const string ActStatus = "act_status";
    public const string NswStatus = "nsw_status";
    public const string NtStatus = "nt_status";
    public const string QldStatus = "qld_status";
    public const string SaStatus = "sa_status";
    public const string TasStatus = "tas_status";
    public const string VicStatus = "vic_status";
    public const string WaStatus = "wa_status";

    /// <summary>A listing system: a short display label (for the multi-system annotation) and the
    /// table column that holds its raw status text. Ordered EPBC → IUCN → states, the order the
    /// inline status annotation uses.</summary>
    public readonly record struct ListingSystem(string Key, string Label, string Column);

    public static readonly IReadOnlyList<ListingSystem> Systems = new[] {
        new ListingSystem("epbc", "EPBC", EpbcStatus),
        new ListingSystem("iucn", "IUCN", IucnStatus),
        new ListingSystem("act", "ACT", ActStatus),
        new ListingSystem("nsw", "NSW", NswStatus),
        new ListingSystem("nt", "NT", NtStatus),
        new ListingSystem("qld", "Qld", QldStatus),
        new ListingSystem("sa", "SA", SaStatus),
        new ListingSystem("tas", "Tas.", TasStatus),
        new ListingSystem("vic", "Vic.", VicStatus),
        new ListingSystem("wa", "WA", WaStatus),
    };

    /// <summary>Columns worth a single-column index (taxonomy/status filters + name joins).</summary>
    public static readonly IReadOnlyList<string> IndexedColumns = new[] {
        ScientificName, EpbcStatus, IucnStatus, Kingdom, ClassName,
    };

    /// <summary>
    /// Exact (trimmed) SPRAT header text → canonical column name, for the columns the lists need.
    /// Built case-insensitively. Headers not present here are auto-sanitised by the importer.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string> HeaderMap =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) {
            ["Taxon ID"] = SpratTaxonId,
            ["Scientific Name"] = ScientificName,
            ["Common Name"] = CommonName,
            ["Kingdom"] = Kingdom,
            ["Phylum"] = Phylum,
            ["Class"] = ClassName,
            ["Order"] = OrderName,
            ["Family"] = Family,
            ["Genus"] = Genus,
            ["Taxon Group"] = TaxonGroup,
            ["EPBC Threat Status"] = EpbcStatus,
            ["IUCN Red List"] = IucnStatus,
            ["ACT NC Act"] = ActStatus,
            ["NSW TSC Act and FM Act"] = NswStatus,
            ["NT TPWC Act"] = NtStatus,
            ["Qld NC Act"] = QldStatus,
            ["SA NPW Act"] = SaStatus,
            ["Tas. TSP Act"] = TasStatus,
            ["Vic. FFG Act (Advisory Lists)"] = VicStatus,
            ["WA WC Act"] = WaStatus,
        };
}
