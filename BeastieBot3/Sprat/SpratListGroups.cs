using System.Collections.Generic;
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
    IReadOnlyList<string>? ExcludePhyla = null);

/// <summary>One Australia list page definition.</summary>
internal sealed record SpratListGroup(
    string Id,
    string TaxaName,       // plural, lower-case: "mammals" → "List of threatened mammals of Australia"
    string Adjective,      // singular adjective for prose: "mammal"
    ListingStyle Style,
    SpratTaxonFilter Filter) {

    public string Title => $"List of threatened {TaxaName} of Australia";
    public string OutputFile => $"List_of_threatened_{TaxaName.Replace(' ', '_')}_of_Australia.wikitext";
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

        new SpratListGroup("plants", "plants", "plant", ListingStyle.ScientificNameFocus,
            new SpratTaxonFilter(Kingdom: "Plantae")),
    };
}
