using System.Collections.Generic;
using System.Text.RegularExpressions;
using BeastieBot3.WikipediaLists;

// The taxonomic groups the Australia threatened-species lists are generated for, Phase 1. Each
// produces one "List of threatened <group> of Australia" page, mirroring the existing IUCN per-group
// list structure but scoped to SPRAT. Filters are expressed directly against the SPRAT taxonomy
// columns (which carry mixed-case values like "Animalia"/"Mammalia"/"Magnoliopsida"), and the
// listing style mirrors the conventions in taxa-groups.yml (Style C common-name-only for
// mammals/birds/fish, Style A scientific-focus for invertebrates/plants). Kept in code for Phase 1;
// a YAML catalogue can follow if these need per-group prose/category tuning.

namespace BeastieBot3.Sprat;

/// <summary>A taxonomy predicate over the SPRAT table columns.</summary>
internal sealed record SpratTaxonFilter(
    string? Kingdom = null,
    IReadOnlyList<string>? Classes = null,
    IReadOnlyList<string>? ExcludeClasses = null,
    IReadOnlyList<string>? ExcludePhyla = null,
    IReadOnlyList<string>? Orders = null,
    IReadOnlyList<string>? ExcludeOrders = null);

/// <summary>One Australia list page definition.</summary>
internal sealed record SpratListGroup(
    string Id,
    string TaxaName,       // plural, lower-case: "mammals" → "List of threatened mammals of Australia"
    string Adjective,      // singular adjective for prose: "mammal"
    ListingStyle Style,
    SpratTaxonFilter Filter) {

    public string Title => $"List of threatened {TaxaName} of Australia";
    public string OutputFile => $"List_of_threatened_{Slug(TaxaName)}_of_Australia.wikitext";

    private static string Slug(string s) => Regex.Replace(s, "[^A-Za-z0-9]+", "_").Trim('_');
}

internal static class SpratListGroups {
    public static readonly IReadOnlyList<SpratListGroup> All = new[] {
        new SpratListGroup("mammals", "mammals", "mammal", ListingStyle.CommonNameOnly,
            new SpratTaxonFilter(Kingdom: "Animalia", Classes: new[] { "Mammalia" })),

        new SpratListGroup("birds", "birds", "bird", ListingStyle.CommonNameOnly,
            new SpratTaxonFilter(Kingdom: "Animalia", Classes: new[] { "Aves" })),

        new SpratListGroup("reptiles", "reptiles", "reptile", ListingStyle.CommonNameFocus,
            new SpratTaxonFilter(Kingdom: "Animalia", Classes: new[] { "Reptilia" })),

        new SpratListGroup("amphibians", "amphibians", "amphibian", ListingStyle.CommonNameFocus,
            new SpratTaxonFilter(Kingdom: "Animalia", Classes: new[] { "Amphibia" })),

        // Fish: the bony + cartilaginous + jawless fish classes present in SPRAT.
        new SpratListGroup("fish", "fish", "fish", ListingStyle.CommonNameFocus,
            new SpratTaxonFilter(Kingdom: "Animalia",
                Classes: new[] { "Actinopterygii", "Chondrichthyes", "Sarcopterygii", "Myxini", "Petromyzonti", "Cephalaspidomorphi" })),

        // Invertebrates: all animals except the chordates (true paraphyly → exclude Chordata).
        new SpratListGroup("invertebrates", "invertebrates", "invertebrate", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Animalia", ExcludePhyla: new[] { "Chordata" })),

        // Plants (~4,800 members) are broken out by class, and the big dicot class (Magnoliopsida,
        // ~3,300) is further split by its five largest orders — all valid current orders; the many
        // smaller (some deprecated-name) orders stay together under "other dicots" so deprecated order
        // names never appear in a page title. Monocots and the ferns/conifers/cycads/mosses catch-all
        // are kept whole.
        new SpratListGroup("dicots-myrtales", "Myrtales", "Myrtales", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae", Classes: new[] { "Magnoliopsida" }, Orders: new[] { "Myrtales" })),
        new SpratListGroup("dicots-fabales", "Fabales", "Fabales", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae", Classes: new[] { "Magnoliopsida" }, Orders: new[] { "Fabales" })),
        new SpratListGroup("dicots-asterales", "Asterales", "Asterales", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae", Classes: new[] { "Magnoliopsida" }, Orders: new[] { "Asterales" })),
        new SpratListGroup("dicots-sapindales", "Sapindales", "Sapindales", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae", Classes: new[] { "Magnoliopsida" }, Orders: new[] { "Sapindales" })),
        new SpratListGroup("dicots-proteales", "Proteales", "Proteales", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae", Classes: new[] { "Magnoliopsida" }, Orders: new[] { "Proteales" })),
        new SpratListGroup("dicots-other", "other dicots", "dicot", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae", Classes: new[] { "Magnoliopsida" },
                ExcludeOrders: new[] { "Myrtales", "Fabales", "Asterales", "Sapindales", "Proteales" })),

        new SpratListGroup("monocots", "monocots", "monocot", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae", Classes: new[] { "Liliopsida" })),

        new SpratListGroup("other-plants", "ferns and other plants", "plant", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae", ExcludeClasses: new[] { "Magnoliopsida", "Liliopsida" })),
    };
}
