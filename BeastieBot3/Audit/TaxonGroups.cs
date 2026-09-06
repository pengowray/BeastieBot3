using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Audit.Model;

// Turns the raw IUCN taxonomy ladder (kingdom / phylum / class / order) into the one friendly
// group label the audit tables show in place of the Class and Family columns: "Mammals",
// "Mammals: Bats", "Plants: Cycads". The coarse Group tier is deliberately small (a reader can
// hold the whole set in their head, and it is what the per-report count line lists); Detail is a
// finer split added only where it earns its place.
//
// The vocabulary is a static table rather than rules/taxa-groups.yml: that file holds query
// filters for Wikipedia list generation and covers only about a third of the classes IUCN uses.
// Names shared with it are copied verbatim so the two products agree.

namespace BeastieBot3.Audit;

internal readonly record struct TaxonGroup(string Group, string? Detail) {
    public string Label => string.IsNullOrEmpty(Detail) ? Group : $"{Group}: {Detail}";
}

internal static class TaxonGroups {
    // Shown when the ladder carries no usable class or kingdom. IUCN writes "NOT ASSIGNED" into
    // these columns, which must never reach the page as if it were a name.
    public const string Unplaced = "Unplaced";

    // The coarse tier, in the order the count line and the group sort key use: vertebrates,
    // invertebrates, then the other kingdoms. Any group missing here sorts last, alphabetically.
    private static readonly string[] GroupOrder = {
        "Mammals", "Birds", "Reptiles", "Amphibians", "Fish",
        "Insects", "Molluscs", "Crustaceans", "Arachnids", "Corals", "Invertebrates",
        "Plants", "Fungi", "Algae", Unplaced,
    };

    private static readonly Dictionary<string, int> GroupRank =
        GroupOrder.Select((g, i) => (g, i)).ToDictionary(t => t.g, t => t.i, StringComparer.OrdinalIgnoreCase);

    // class -> (group, detail). Keys are the IUCN class values, upper-cased.
    private static readonly Dictionary<string, TaxonGroup> ByClass = new(StringComparer.OrdinalIgnoreCase) {
        // Vertebrates
        ["MAMMALIA"] = new("Mammals", null),
        ["AVES"] = new("Birds", null),
        ["REPTILIA"] = new("Reptiles", null),
        ["AMPHIBIA"] = new("Amphibians", null),
        ["ACTINOPTERYGII"] = new("Fish", "Ray-finned fishes"),
        ["CHONDRICHTHYES"] = new("Fish", "Sharks and rays"),
        ["SARCOPTERYGII"] = new("Fish", "Lobe-finned fishes"),
        ["MYXINI"] = new("Fish", "Hagfishes"),
        ["PETROMYZONTI"] = new("Fish", "Lampreys"),

        // Invertebrates with a group of their own
        ["INSECTA"] = new("Insects", null),
        ["GASTROPODA"] = new("Molluscs", "Gastropods"),
        ["BIVALVIA"] = new("Molluscs", "Bivalves"),
        ["CEPHALOPODA"] = new("Molluscs", "Cephalopods"),
        ["POLYPLACOPHORA"] = new("Molluscs", "Chitons"),
        ["SOLENOGASTRES"] = new("Molluscs", "Solenogasters"),
        ["MONOPLACOPHORA"] = new("Molluscs", "Monoplacophorans"),
        ["MALACOSTRACA"] = new("Crustaceans", null),
        ["BRANCHIOPODA"] = new("Crustaceans", "Branchiopods"),
        ["MAXILLOPODA"] = new("Crustaceans", "Maxillopods"),
        ["HEXANAUPLIA"] = new("Crustaceans", "Copepods"),
        ["OSTRACODA"] = new("Crustaceans", "Ostracods"),
        ["THEOCOSTRACA"] = new("Crustaceans", "Barnacles"),
        ["ARACHNIDA"] = new("Arachnids", null),
        ["ANTHOZOA"] = new("Corals", null),
        ["HYDROZOA"] = new("Corals", "Hydrozoans"),

        // The invertebrate long tail
        ["CLITELLATA"] = new("Invertebrates", "Segmented worms"),
        ["POLYCHAETA"] = new("Invertebrates", "Bristle worms"),
        ["DIPLOPODA"] = new("Invertebrates", "Millipedes"),
        ["CHILOPODA"] = new("Invertebrates", "Centipedes"),
        ["COLLEMBOLA"] = new("Invertebrates", "Springtails"),
        ["MEROSTOMATA"] = new("Invertebrates", "Horseshoe crabs"),
        ["HOLOTHUROIDEA"] = new("Invertebrates", "Sea cucumbers"),
        ["ASTEROIDEA"] = new("Invertebrates", "Sea stars"),
        ["ECHINOIDEA"] = new("Invertebrates", "Sea urchins"),
        ["NEMERTEA"] = new("Invertebrates", "Ribbon worms"),
        ["UDEONYCHOPHORA"] = new("Invertebrates", "Velvet worms"),
        ["TURBELLARIA"] = new("Invertebrates", "Flatworms"),
        ["DEMOSPONGIAE"] = new("Invertebrates", "Sponges"),
        ["CALCAREA"] = new("Invertebrates", "Sponges"),
        ["HEXACTINELLIDA"] = new("Invertebrates", "Glass sponges"),

        // Plants
        ["MAGNOLIOPSIDA"] = new("Plants", "Dicotyledons"),
        ["LILIOPSIDA"] = new("Plants", "Monocotyledons"),
        ["PINOPSIDA"] = new("Plants", "Conifers"),
        ["CYCADOPSIDA"] = new("Plants", "Cycads"),
        ["GNETOPSIDA"] = new("Plants", "Gnetophytes"),
        ["GINKGOOPSIDA"] = new("Plants", "Ginkgo"),
        ["POLYPODIOPSIDA"] = new("Plants", "Ferns"),
        ["LYCOPODIOPSIDA"] = new("Plants", "Clubmosses"),
        ["BRYOPSIDA"] = new("Plants", "Mosses"),
        ["SPHAGNOPSIDA"] = new("Plants", "Peat mosses"),
        ["POLYTRICHOPSIDA"] = new("Plants", "Mosses"),
        ["ANDREAEOPSIDA"] = new("Plants", "Mosses"),
        ["TETRAPHIDOPSIDA"] = new("Plants", "Mosses"),
        ["OEDIPODIOPSIDA"] = new("Plants", "Mosses"),
        ["TAKAKIOPSIDA"] = new("Plants", "Mosses"),
        ["JUNGERMANNIOPSIDA"] = new("Plants", "Liverworts"),
        ["MARCHANTIOPSIDA"] = new("Plants", "Liverworts"),
        ["HAPLOMITRIOPSIDA"] = new("Plants", "Liverworts"),
        ["ANTHOCEROTOPSIDA"] = new("Plants", "Hornworts"),
        ["CHAROPHYCEAE"] = new("Algae", "Stoneworts"),
        ["FLORIDEOPHYCEAE"] = new("Algae", "Red algae"),
        ["ULVOPHYCEAE"] = new("Algae", "Green algae"),
        ["CHLOROPHYCEAE"] = new("Algae", "Green algae"),
        ["PHAEOPHYCEAE"] = new("Algae", "Brown algae"),

        // Fungi
        ["AGARICOMYCETES"] = new("Fungi", "Mushrooms"),
        ["LECANOROMYCETES"] = new("Fungi", "Lichens"),
        ["ARTHONIOMYCETES"] = new("Fungi", "Lichens"),
        ["PEZIZOMYCETES"] = new("Fungi", "Cup fungi"),
        ["SORDARIOMYCETES"] = new("Fungi", "Sac fungi"),
        ["LEOTIOMYCETES"] = new("Fungi", "Sac fungi"),
        ["EUROTIOMYCETES"] = new("Fungi", "Sac fungi"),
        ["DOTHIDEOMYCETES"] = new("Fungi", "Sac fungi"),
        ["GEOGLOSSOMYCETES"] = new("Fungi", "Earth tongues"),
        ["USTILAGINOMYCETES"] = new("Fungi", "Smut fungi"),
        ["EXOBASIDIOMYCETES"] = new("Fungi", "Smut fungi"),
        ["DACRYMYCETES"] = new("Fungi", "Jelly fungi"),
        ["WALLEMIOMYCETES"] = new("Fungi", "Wallemia"),
    };

    // Finer detail, applied only where the order split is one a reader already thinks in.
    private static readonly Dictionary<string, string> ByOrder = new(StringComparer.OrdinalIgnoreCase) {
        ["MAMMALIA|CHIROPTERA"] = "Bats",
        ["MAMMALIA|RODENTIA"] = "Rodents",
        ["MAMMALIA|PRIMATES"] = "Primates",
        ["MAMMALIA|CETACEA"] = "Whales and dolphins",
        ["MAMMALIA|CARNIVORA"] = "Carnivores",
        ["MAMMALIA|ARTIODACTYLA"] = "Even-toed ungulates",
        ["MAMMALIA|PERISSODACTYLA"] = "Odd-toed ungulates",
        ["AMPHIBIA|ANURA"] = "Frogs and toads",
        ["AMPHIBIA|CAUDATA"] = "Salamanders",
        ["AMPHIBIA|GYMNOPHIONA"] = "Caecilians",
        ["REPTILIA|TESTUDINES"] = "Turtles and tortoises",
        ["REPTILIA|CROCODYLIA"] = "Crocodiles",
        ["REPTILIA|SQUAMATA"] = "Lizards and snakes",
        ["REPTILIA|RHYNCHOCEPHALIA"] = "Tuatara",
        ["INSECTA|ODONATA"] = "Dragonflies and damselflies",
        ["INSECTA|LEPIDOPTERA"] = "Butterflies and moths",
        ["INSECTA|COLEOPTERA"] = "Beetles",
        ["INSECTA|HYMENOPTERA"] = "Bees, wasps and ants",
        ["INSECTA|ORTHOPTERA"] = "Grasshoppers and crickets",
        ["MALACOSTRACA|DECAPODA"] = "Crabs, lobsters and shrimps",
        ["ARACHNIDA|ARANEAE"] = "Spiders",
        ["ARACHNIDA|SCORPIONES"] = "Scorpions",
        ["ANTHOZOA|SCLERACTINIA"] = "Stony corals",
    };

    // IUCN writes this into class and order columns where nothing is recorded.
    private static bool IsMissing(string? value) =>
        string.IsNullOrWhiteSpace(value) || value.Trim().Equals("NOT ASSIGNED", StringComparison.OrdinalIgnoreCase);

    public static TaxonGroup For(string? kingdom, string? phylum, string? className, string? orderName) {
        if (!IsMissing(className) && ByClass.TryGetValue(className!.Trim(), out var known)) {
            if (!IsMissing(orderName) &&
                ByOrder.TryGetValue($"{className.Trim().ToUpperInvariant()}|{orderName!.Trim().ToUpperInvariant()}", out var detail)) {
                return known with { Detail = detail };
            }
            return known;
        }

        // An unrecognised class (a new one in a later release) still lands in the right kingdom,
        // and keeps its own name so the row can be found; better than dropping it into "Unplaced".
        var fallback = FromKingdom(kingdom, phylum);
        return IsMissing(className) ? fallback : fallback with { Detail = Titleise(className!) };
    }

    private static TaxonGroup FromKingdom(string? kingdom, string? phylum) {
        if (IsMissing(kingdom)) {
            return new TaxonGroup(Unplaced, null);
        }
        return kingdom!.Trim().ToUpperInvariant() switch {
            "ANIMALIA" => new TaxonGroup("Invertebrates", null),
            "PLANTAE" => new TaxonGroup("Plants", null),
            "FUNGI" => new TaxonGroup("Fungi", null),
            "CHROMISTA" => new TaxonGroup("Algae", null),
            _ => new TaxonGroup(Unplaced, null),
        };
    }

    // IUCN stores higher ranks in upper case; a bare class name shown to a reader reads better
    // capitalised the Linnaean way.
    private static string Titleise(string value) {
        var trimmed = value.Trim();
        return trimmed.Length <= 1
            ? trimmed.ToUpperInvariant()
            : char.ToUpperInvariant(trimmed[0]) + trimmed[1..].ToLowerInvariant();
    }

    public static TaxonGroup For(AuditFinding f) {
        // A class-rank finding borrows its ladder from one sample assessment below it, so its order
        // is whichever order that sample happened to be in. Ignore it: labelling the class MAMMALIA
        // "Mammals: Bats" because the sample was a bat would be wrong, not merely coarse.
        var order = string.Equals(f.Rank, "class", StringComparison.OrdinalIgnoreCase) ? null : f.Order;
        return For(f.Kingdom, f.Phylum, f.Class, order);
    }

    public static string Label(AuditFinding f) => For(f).Label;

    public static string GroupOf(AuditFinding f) => For(f).Group;

    // Sorts the column by the canonical group order, then by detail, so rows of one group stay
    // together instead of scattering alphabetically ("Fish: Hagfishes" next to "Fish: Lampreys").
    public static string SortKey(AuditFinding f) {
        var g = For(f);
        var rank = GroupRank.TryGetValue(g.Group, out var i) ? i : GroupOrder.Length;
        return $"{rank:D2}|{g.Group.ToLowerInvariant()}|{(g.Detail ?? "").ToLowerInvariant()}";
    }

    // Counts by the coarse tier only, most first, for the one-line summary that replaces the old
    // "By class" table.
    public static IReadOnlyList<(string Group, int Count)> Counts(IEnumerable<AuditFinding> findings) => findings
        .GroupBy(GroupOf)
        .Select(g => (Group: g.Key, Count: g.Count()))
        .OrderByDescending(t => t.Count)
        .ThenBy(t => GroupRank.TryGetValue(t.Group, out var i) ? i : GroupOrder.Length)
        .ThenBy(t => t.Group, StringComparer.OrdinalIgnoreCase)
        .ToList();

    public static string CountLine(IEnumerable<AuditFinding> findings) =>
        string.Join(", ", Counts(findings).Select(t => $"{t.Group} ({t.Count:N0})"));
}
