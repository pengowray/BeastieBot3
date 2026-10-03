using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

// Which kingdom a Wikipedia page is about, as far as the page itself says, so that a taxon is not
// matched or linked to the article about a taxon with the same name in another kingdom: the plant
// Ficus variegata to "Ficus variegata (gastropod)", or the palm Gaussia princeps to "Gaussia
// princeps (crustacean)". A page names its kingdom in four places:
// - the taxobox kingdom parameter, which only old-style taxoboxes have ("[[Animal]]ia");
// - the bracketed word after the taxobox genus or taxon parameter: an automatic taxobox gives
//   "genus = Ficus (gastropod)" when the genus name is shared with another kingdom;
// - the bracketed word at the end of the page title: "Gaussia princeps (crustacean)";
// - the "<group> described in <year>" categories: "Birds described in 1789".
// Each word in the tables below names a group that lies entirely inside the kingdoms given for
// it, and a word that is not in a table tells nothing. A page counts as about another kingdom only
// when at least one place names a kingdom and no place names the taxon's own kingdom.

namespace BeastieBot3.Wikipedia;

internal static class WikiPageKingdom {
    public const string Animalia = "ANIMALIA";
    public const string Plantae = "PLANTAE";
    public const string Fungi = "FUNGI";
    public const string Chromista = "CHROMISTA";

    private static readonly string[] Animal = [Animalia];
    // IUCN puts the brown algae in CHROMISTA and the red and green algae in PLANTAE, and Wikipedia
    // has long filed algae with plants, so plant words also allow CHROMISTA.
    private static readonly string[] PlantOrAlga = [Plantae, Chromista];
    // IUCN puts lichens in FUNGI.
    private static readonly string[] Fungus = [Fungi];

    // Words in title and taxobox brackets, in the order a list link prefers them: for a plant,
    // "(plant)" before "(tree)" or "(palm)".
    private static readonly (string Word, string[] Kingdoms)[] QualifierWords = [
        ("plant", PlantOrAlga), ("tree", PlantOrAlga), ("palm", PlantOrAlga), ("shrub", PlantOrAlga),
        ("herb", PlantOrAlga), ("grass", PlantOrAlga), ("sedge", PlantOrAlga), ("orchid", PlantOrAlga),
        ("cactus", PlantOrAlga), ("fern", PlantOrAlga), ("cycad", PlantOrAlga), ("conifer", PlantOrAlga),
        ("moss", PlantOrAlga), ("liverwort", PlantOrAlga), ("hornwort", PlantOrAlga),
        ("alga", PlantOrAlga), ("seaweed", PlantOrAlga),
        ("fungus", Fungus), ("mushroom", Fungus), ("lichen", Fungus),
        ("animal", Animal),
        ("fish", Animal), ("shark", Animal), ("sturgeon", Animal), ("flounder", Animal),
        ("bird", Animal), ("goose", Animal), ("duck", Animal), ("parrot", Animal), ("owl", Animal),
        ("mammal", Animal), ("antelope", Animal), ("bat", Animal), ("rodent", Animal), ("tenrec", Animal),
        ("whale", Animal), ("dolphin", Animal), ("monkey", Animal),
        ("reptile", Animal), ("lizard", Animal), ("snake", Animal), ("turtle", Animal), ("tortoise", Animal),
        ("gecko", Animal), ("skink", Animal),
        ("amphibian", Animal), ("frog", Animal), ("toad", Animal), ("salamander", Animal),
        ("insect", Animal), ("moth", Animal), ("butterfly", Animal), ("beetle", Animal), ("fly", Animal),
        ("ant", Animal), ("bee", Animal), ("wasp", Animal), ("damselfly", Animal), ("dragonfly", Animal),
        ("mantis", Animal), ("cricket", Animal), ("grasshopper", Animal), ("cicada", Animal),
        ("spider", Animal), ("scorpion", Animal), ("millipede", Animal), ("centipede", Animal),
        ("crustacean", Animal), ("crab", Animal), ("shrimp", Animal), ("prawn", Animal), ("crayfish", Animal),
        ("lobster", Animal), ("copepod", Animal), ("amphipod", Animal), ("isopod", Animal),
        ("mollusc", Animal), ("gastropod", Animal), ("snail", Animal), ("slug", Animal), ("bivalve", Animal),
        ("mussel", Animal), ("clam", Animal), ("cephalopod", Animal), ("squid", Animal), ("octopus", Animal),
        ("worm", Animal), ("annelid", Animal), ("echinoderm", Animal), ("starfish", Animal),
        ("coral", Animal), ("cnidarian", Animal), ("jellyfish", Animal), ("sponge", Animal),
    ];

    // The first word of "<group> described in <year>" categories.
    private static readonly Dictionary<string, string[]> CategoryGroups = new(StringComparer.OrdinalIgnoreCase) {
        ["Plants"] = PlantOrAlga, ["Algae"] = PlantOrAlga,
        ["Fungi"] = Fungus, ["Lichens"] = Fungus,
        ["Animals"] = Animal, ["Fish"] = Animal, ["Birds"] = Animal, ["Mammals"] = Animal,
        ["Reptiles"] = Animal, ["Amphibians"] = Animal, ["Insects"] = Animal, ["Moths"] = Animal,
        ["Butterflies"] = Animal, ["Beetles"] = Animal, ["Arthropods"] = Animal, ["Spiders"] = Animal,
        ["Arachnids"] = Animal, ["Crustaceans"] = Animal, ["Molluscs"] = Animal, ["Gastropods"] = Animal,
        ["Bivalves"] = Animal, ["Cephalopods"] = Animal, ["Corals"] = Animal, ["Echinoderms"] = Animal,
        ["Starfish"] = Animal, ["Sponges"] = Animal, ["Cnidarians"] = Animal,
    };

    private static readonly Dictionary<string, (int Rank, string[] Kingdoms)> QualifierIndex =
        QualifierWords.Select((entry, rank) => (entry, rank))
            .ToDictionary(x => x.entry.Word, x => (x.rank, x.entry.Kingdoms), StringComparer.OrdinalIgnoreCase);

    private static readonly Regex TrailingQualifier = new(@"\(([^()]+)\)\s*$", RegexOptions.CultureInvariant);
    private static readonly Regex DescribedIn = new(@"^(\S+) described in \d", RegexOptions.CultureInvariant);
    private static readonly Regex WikiLink = new(@"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", RegexOptions.CultureInvariant);

    /// <summary>
    /// The kingdoms the bracketed word at the end of <paramref name="text"/> allows ("Ficus
    /// (gastropod)", "Gaussia princeps (plant)"), or null when it has none or the word is not in the
    /// table. A qualifier of several words is read by its last word ("bush cricket").
    /// </summary>
    public static IReadOnlyList<string>? FromQualifier(string? text) => QualifierEntry(text)?.Kingdoms;

    /// <summary>The kingdom a taxobox kingdom value names ("[[Animal]]ia", "[[Fungus|Fungi]]"), or null.</summary>
    public static IReadOnlyList<string>? FromTaxoboxKingdom(string? value) {
        if (string.IsNullOrWhiteSpace(value)) {
            return null;
        }
        var text = WikiLink.Replace(value, "$1").Trim();
        if (text.StartsWith("Anim", StringComparison.OrdinalIgnoreCase)) return Animal;
        if (text.StartsWith("Plant", StringComparison.OrdinalIgnoreCase)) return PlantOrAlga;
        if (text.StartsWith("Fung", StringComparison.OrdinalIgnoreCase)) return Fungus;
        if (text.StartsWith("Chromis", StringComparison.OrdinalIgnoreCase)) return [Chromista];
        return null;
    }

    /// <summary>The kingdoms a "&lt;group&gt; described in &lt;year&gt;" category allows, or null for any other category.</summary>
    public static IReadOnlyList<string>? FromCategory(string? category) {
        if (string.IsNullOrWhiteSpace(category)) {
            return null;
        }
        var match = DescribedIn.Match(category.Trim());
        return match.Success && CategoryGroups.TryGetValue(match.Groups[1].Value, out var kingdoms) ? kingdoms : null;
    }

    /// <summary>
    /// Why the page described by <paramref name="evidence"/> is about a taxon in a kingdom other than
    /// <paramref name="taxonKingdom"/> (an IUCN kingdom name, such as PLANTAE), or null when it is not,
    /// when the page names no kingdom, or when the taxon's kingdom is not known.
    /// <paramref name="otherTitle"/> is a further title for the page to read the bracketed word of:
    /// the title of a redirect to it.
    /// </summary>
    public static string? Conflict(string? taxonKingdom, WikiPageKingdomEvidence evidence, string? otherTitle = null) {
        if (string.IsNullOrWhiteSpace(taxonKingdom)) {
            return null;
        }
        var kingdom = taxonKingdom.Trim().ToUpperInvariant();
        var found = new List<(string Source, IReadOnlyList<string> Kingdoms)>();
        void Add(string source, IReadOnlyList<string>? kingdoms) {
            if (kingdoms is not null) {
                found.Add((source, kingdoms));
            }
        }

        Add($"taxobox kingdom \"{evidence.TaxoboxKingdom}\"", FromTaxoboxKingdom(evidence.TaxoboxKingdom));
        Add($"taxobox genus \"{evidence.TaxoboxGenus}\"", FromQualifier(evidence.TaxoboxGenus));
        Add($"taxobox taxon \"{evidence.TaxoboxTaxon}\"", FromQualifier(evidence.TaxoboxTaxon));
        Add($"title \"{evidence.PageTitle}\"", FromQualifier(evidence.PageTitle));
        if (!string.IsNullOrWhiteSpace(otherTitle) && !string.Equals(otherTitle, evidence.PageTitle, StringComparison.Ordinal)) {
            Add($"title \"{otherTitle}\"", FromQualifier(otherTitle));
        }
        foreach (var category in evidence.Categories) {
            Add($"category \"{category}\"", FromCategory(category));
        }

        if (found.Count == 0 || found.Any(f => f.Kingdoms.Contains(kingdom, StringComparer.Ordinal))) {
            return null;
        }
        var named = found.SelectMany(f => f.Kingdoms).Distinct(StringComparer.Ordinal).OrderBy(k => k, StringComparer.Ordinal);
        return $"Page is about a taxon in {string.Join(" or ", named)}, not {kingdom} ({string.Join("; ", found.Select(f => f.Source))})";
    }

    /// <summary>
    /// The titles among <paramref name="titles"/> that are <paramref name="name"/> followed by a
    /// bracketed word for a group in <paramref name="taxonKingdom"/> ("Ficus variegata (plant)" for a
    /// plant), best first: in the order of the word table, so "(plant)" comes before "(tree)", and one
    /// title for each word ("(tree)" is kept and "(Tree)" dropped).
    /// </summary>
    public static IReadOnlyList<string> QualifiedTitlesFor(string name, string? taxonKingdom, IEnumerable<string> titles) {
        if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(taxonKingdom)) {
            return [];
        }
        var kingdom = taxonKingdom.Trim().ToUpperInvariant();
        var prefix = name.Trim() + " (";
        var fitting = new List<(int Rank, bool NotLowerCase, string Title, string Word)>();
        foreach (var title in titles) {
            if (!title.StartsWith(prefix, StringComparison.Ordinal) || !title.EndsWith(')')) {
                continue;
            }
            var qualifier = title[prefix.Length..^1].Trim();
            if (qualifier.Contains('(') || qualifier.Contains(')')) {
                continue;
            }
            var entry = QualifierEntry(title);
            if (entry is { } e && e.Kingdoms.Contains(kingdom, StringComparer.Ordinal)) {
                fitting.Add((e.Rank, !string.Equals(qualifier, qualifier.ToLowerInvariant(), StringComparison.Ordinal), title,
                    qualifier.ToLowerInvariant()));
            }
        }
        return fitting
            .OrderBy(f => f.Rank).ThenBy(f => f.NotLowerCase).ThenBy(f => f.Title, StringComparer.Ordinal)
            .DistinctBy(f => f.Word)
            .Select(f => f.Title)
            .ToList();
    }

    private static (int Rank, string[] Kingdoms)? QualifierEntry(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        var match = TrailingQualifier.Match(text.Trim());
        if (!match.Success) {
            return null;
        }
        var qualifier = match.Groups[1].Value.Trim();
        if (QualifierIndex.TryGetValue(qualifier, out var whole)) {
            return whole;
        }
        var lastSpace = qualifier.LastIndexOf(' ');
        return lastSpace > 0 && QualifierIndex.TryGetValue(qualifier[(lastSpace + 1)..], out var last) ? last : null;
    }
}

/// <summary>
/// What a downloaded page says about its kingdom (<see cref="WikiPageKingdom"/>): its title, its
/// taxobox kingdom, genus and taxon parameters, and its "&lt;group&gt; described in &lt;year&gt;" categories.
/// </summary>
internal sealed record WikiPageKingdomEvidence(
    string PageTitle,
    string? TaxoboxKingdom,
    string? TaxoboxGenus,
    string? TaxoboxTaxon,
    IReadOnlyList<string> Categories);
