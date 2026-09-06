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
// Names shared with it are copied verbatim, with three deliberate departures. Gastropods and
// Bivalves become Detail under a Molluscs group, so cephalopods have a home and "Molluscs (7)" is
// what the count line says. Crustaceans, Corals and Mosses keep their names but cover the natural
// group rather than the single class the yml filters on. Brown algae are Chromista in IUCN's
// kingdom column and red and green algae are Plantae, but all of them read as "Algae" here:
// "Chromista" is jargon outside taxonomy and "Plants: Red algae" surprises everyone else. Anyone
// reconciling against IUCN's own kingdom tables should know those rows sit apart.

namespace BeastieBot3.Audit;

internal readonly record struct TaxonGroup(string Group, string? Detail) {
    public string Label => string.IsNullOrEmpty(Detail) ? Group : $"{Group}: {Detail}";
}

internal static class TaxonGroups {
    // Shown when the ladder carries no usable kingdom or class. It describes the record, not the
    // taxon: "Unplaced" would read to this audience as incertae sedis, which is a claim about the
    // organism and not what the blank column means.
    public const string NoTaxonomy = "No taxonomy";

    // Residual buckets. "Other invertebrates" rather than "Invertebrates" because the count line
    // also lists Insects, Molluscs and the rest, and a bare "Invertebrates (3)" next to
    // "Insects (12)" reads as a superset count.
    private const string OtherInvertebrates = "Other invertebrates";
    private const string OtherAnimals = "Other animals";

    // The coarse tier, in the order the count line and the group sort key use: vertebrates,
    // invertebrates, then the other kingdoms, residual buckets last. A group missing here sorts
    // last, alphabetically.
    private static readonly string[] GroupOrder = {
        "Mammals", "Birds", "Reptiles", "Amphibians", "Fish",
        "Insects", "Molluscs", "Crustaceans", "Arachnids", "Corals", OtherInvertebrates,
        "Plants", "Fungi", "Algae", OtherAnimals, NoTaxonomy,
    };

    private static readonly Dictionary<string, int> GroupRank =
        GroupOrder.Select((g, i) => (g, i)).ToDictionary(t => t.g, t => t.i, StringComparer.OrdinalIgnoreCase);

    // class -> (group, detail), keyed on the IUCN class value.
    private static readonly Dictionary<string, TaxonGroup> ByClass = new(StringComparer.OrdinalIgnoreCase) {
        // Vertebrates
        ["MAMMALIA"] = new("Mammals", null),
        ["AVES"] = new("Birds", null),
        ["REPTILIA"] = new("Reptiles", null),
        ["AMPHIBIA"] = new("Amphibians", null),
        ["ACTINOPTERYGII"] = new("Fish", "Ray-finned fishes"),
        ["CHONDRICHTHYES"] = new("Fish", "Sharks and rays"),
        ["SARCOPTERYGII"] = new("Fish", "Lungfishes and coelacanths"),
        ["MYXINI"] = new("Fish", "Hagfishes"),
        ["PETROMYZONTI"] = new("Fish", "Lampreys"),
        // The class older IUCN releases filed lampreys under.
        ["CEPHALASPIDOMORPHI"] = new("Fish", "Lampreys"),

        // Insects
        ["INSECTA"] = new("Insects", null),

        // Molluscs
        ["GASTROPODA"] = new("Molluscs", "Gastropods"),
        ["BIVALVIA"] = new("Molluscs", "Bivalves"),
        ["CEPHALOPODA"] = new("Molluscs", "Cephalopods"),
        ["POLYPLACOPHORA"] = new("Molluscs", "Chitons"),
        ["SOLENOGASTRES"] = new("Molluscs", "Solenogasters"),
        ["MONOPLACOPHORA"] = new("Molluscs", "Monoplacophorans"),

        // Crustaceans. Maxillopoda is an abandoned grouping and every order IUCN files under it is
        // a copepod order, so it is labelled for what is actually in it.
        ["MALACOSTRACA"] = new("Crustaceans", null),
        ["BRANCHIOPODA"] = new("Crustaceans", "Fairy shrimps and water fleas"),
        ["MAXILLOPODA"] = new("Crustaceans", "Copepods"),
        ["HEXANAUPLIA"] = new("Crustaceans", "Copepods"),
        ["OSTRACODA"] = new("Crustaceans", "Ostracods"),
        ["THECOSTRACA"] = new("Crustaceans", "Barnacles"),
        // IUCN's spelling of Thecostraca.
        ["THEOCOSTRACA"] = new("Crustaceans", "Barnacles"),

        ["ARACHNIDA"] = new("Arachnids", null),

        // Corals. Anthozoa also holds anemones, zoanthids, sea pens and black corals (about 55 of
        // its rows); "Corals" is the gloss both this project and IUCN use for the class.
        ["ANTHOZOA"] = new("Corals", null),
        // Every assessed hydrozoan is a fire or lace coral, assessed alongside the stony corals.
        ["HYDROZOA"] = new("Corals", "Hydrocorals"),

        // The invertebrate long tail
        ["CLITELLATA"] = new(OtherInvertebrates, "Earthworms and leeches"),
        ["POLYCHAETA"] = new(OtherInvertebrates, "Bristle worms"),
        ["DIPLOPODA"] = new(OtherInvertebrates, "Millipedes"),
        ["CHILOPODA"] = new(OtherInvertebrates, "Centipedes"),
        ["COLLEMBOLA"] = new(OtherInvertebrates, "Springtails"),
        ["MEROSTOMATA"] = new(OtherInvertebrates, "Horseshoe crabs"),
        ["HOLOTHUROIDEA"] = new(OtherInvertebrates, "Sea cucumbers"),
        ["ASTEROIDEA"] = new(OtherInvertebrates, "Starfish"),
        ["ECHINOIDEA"] = new(OtherInvertebrates, "Sea urchins"),
        ["NEMERTEA"] = new(OtherInvertebrates, "Ribbon worms"),
        ["UDEONYCHOPHORA"] = new(OtherInvertebrates, "Velvet worms"),
        ["TURBELLARIA"] = new(OtherInvertebrates, "Flatworms"),
        ["DEMOSPONGIAE"] = new(OtherInvertebrates, "Sponges"),
        ["CALCAREA"] = new(OtherInvertebrates, "Sponges"),
        ["HEXACTINELLIDA"] = new(OtherInvertebrates, "Sponges"),

        // Plants
        ["MAGNOLIOPSIDA"] = new("Plants", "Dicotyledons"),
        ["LILIOPSIDA"] = new("Plants", "Monocotyledons"),
        ["POLYPODIOPSIDA"] = new("Plants", "Ferns"),
        ["PINOPSIDA"] = new("Plants", "Conifers"),
        ["CYCADOPSIDA"] = new("Plants", "Cycads"),
        // Quillworts are the majority of this class, so "Clubmosses" alone would mislabel it.
        ["LYCOPODIOPSIDA"] = new("Plants", "Clubmosses and quillworts"),
        ["GNETOPSIDA"] = new("Plants", "Gnetophytes"),
        ["GINKGOOPSIDA"] = new("Plants", "Ginkgo"),
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

        // Algae, which IUCN splits across the Plantae and Chromista kingdoms.
        ["CHAROPHYCEAE"] = new("Algae", "Stoneworts"),
        ["ULVOPHYCEAE"] = new("Algae", "Green algae"),
        ["CHLOROPHYCEAE"] = new("Algae", "Green algae"),
        ["FLORIDEOPHYCEAE"] = new("Algae", "Red algae"),
        ["PHAEOPHYCEAE"] = new("Algae", "Brown algae"),

        // Fungi. The four mixed ascomycete classes get no detail: "sac fungi" is the gloss for the
        // whole phylum, which the lichen classes above share, so it would name the wrong level.
        ["AGARICOMYCETES"] = new("Fungi", "Mushrooms"),
        ["LECANOROMYCETES"] = new("Fungi", "Lichens"),
        ["ARTHONIOMYCETES"] = new("Fungi", "Lichens"),
        ["PEZIZOMYCETES"] = new("Fungi", "Cup fungi"),
        ["GEOGLOSSOMYCETES"] = new("Fungi", "Earth tongues"),
        ["USTILAGINOMYCETES"] = new("Fungi", "Smut fungi"),
        ["EXOBASIDIOMYCETES"] = new("Fungi", "Smut fungi"),
        ["DACRYMYCETES"] = new("Fungi", "Jelly fungi"),
        ["SORDARIOMYCETES"] = new("Fungi", null),
        ["LEOTIOMYCETES"] = new("Fungi", null),
        ["EUROTIOMYCETES"] = new("Fungi", null),
        ["DOTHIDEOMYCETES"] = new("Fungi", null),
        ["WALLEMIOMYCETES"] = new("Fungi", null),
    };

    // Finer detail, keyed "CLASS|ORDER". An order earns a label when it has a household English
    // name and either holds roughly a thousand assessed taxa, is nearly the whole of its class, or
    // sits in a class small enough that every order can be labelled. Within a class that has any
    // override the dominant order is labelled too, so a bare group always means "outside the
    // labelled orders" and never silently means the biggest one.
    private static readonly Dictionary<string, string> ByOrder = new(StringComparer.OrdinalIgnoreCase) {
        ["MAMMALIA|CHIROPTERA"] = "Bats",
        ["MAMMALIA|RODENTIA"] = "Rodents",
        ["MAMMALIA|PRIMATES"] = "Primates",
        ["AMPHIBIA|ANURA"] = "Frogs and toads",
        ["AMPHIBIA|CAUDATA"] = "Salamanders",
        ["AMPHIBIA|GYMNOPHIONA"] = "Caecilians",
        ["REPTILIA|SQUAMATA"] = "Lizards and snakes",
        ["REPTILIA|TESTUDINES"] = "Turtles and tortoises",
        ["REPTILIA|CROCODYLIA"] = "Crocodilians",
        // One species, kept only because its three sibling orders are labelled: a bare "Reptiles"
        // would otherwise mean tuatara, which nobody would guess.
        ["REPTILIA|RHYNCHOCEPHALIA"] = "Tuatara",
        ["INSECTA|ODONATA"] = "Dragonflies and damselflies",
        ["INSECTA|LEPIDOPTERA"] = "Butterflies and moths",
        ["INSECTA|COLEOPTERA"] = "Beetles",
        ["INSECTA|HYMENOPTERA"] = "Bees, wasps and ants",
        ["INSECTA|ORTHOPTERA"] = "Grasshoppers and crickets",
        ["INSECTA|DIPTERA"] = "Flies",
        ["MALACOSTRACA|DECAPODA"] = "Crabs, lobsters and shrimps",
        ["ARACHNIDA|ARANEAE"] = "Spiders",
        ["ANTHOZOA|SCLERACTINIA"] = "Stony corals",
    };

    // IUCN writes this into class and order columns where nothing is recorded.
    private static bool IsMissing(string? value) =>
        string.IsNullOrWhiteSpace(value) || value.Trim().Equals("NOT ASSIGNED", StringComparison.OrdinalIgnoreCase);

    public static TaxonGroup For(string? kingdom, string? phylum, string? className, string? orderName) {
        if (!IsMissing(className) && ByClass.TryGetValue(className!.Trim(), out var known)) {
            if (!IsMissing(orderName) &&
                ByOrder.TryGetValue($"{className.Trim()}|{orderName!.Trim()}", out var detail)) {
                return known with { Detail = detail };
            }
            return known;
        }

        // A class this table does not know (a rename, or one added in a later release) still lands
        // in the right kingdom and keeps its own name, so the row can be found and the table
        // extended. Only "NOT ASSIGNED" is hidden; a real name is a name.
        var fallback = FromKingdom(kingdom, phylum);
        return IsMissing(className) ? fallback : fallback with { Detail = SentenceCase(className!) };
    }

    private static TaxonGroup FromKingdom(string? kingdom, string? phylum) {
        if (IsMissing(kingdom)) {
            return new TaxonGroup(NoTaxonomy, null);
        }
        return kingdom!.Trim().ToUpperInvariant() switch {
            // An unrecognised chordate class is a vertebrate, so it must not land in an
            // invertebrate bucket; anything else in Animalia is an invertebrate by definition.
            "ANIMALIA" => new TaxonGroup(
                IsMissing(phylum) || phylum!.Trim().Equals("CHORDATA", StringComparison.OrdinalIgnoreCase)
                    ? OtherAnimals
                    : OtherInvertebrates, null),
            "PLANTAE" => new TaxonGroup("Plants", null),
            "FUNGI" => new TaxonGroup("Fungi", null),
            "CHROMISTA" => new TaxonGroup("Algae", null),
            _ => new TaxonGroup(SentenceCase(kingdom), null),
        };
    }

    // IUCN stores higher ranks in upper case; a bare Linnaean name shown to a reader reads better
    // capitalised the usual way.
    private static string SentenceCase(string value) {
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
    // A bare group means "outside the labelled orders", so it sorts after them rather than first.
    public static string SortKey(AuditFinding f) {
        var g = For(f);
        var rank = GroupRank.TryGetValue(g.Group, out var i) ? i : GroupOrder.Length;
        var detail = string.IsNullOrEmpty(g.Detail) ? "~" : g.Detail.ToLowerInvariant();
        return $"{rank:D2}|{g.Group.ToLowerInvariant()}|{detail}";
    }

    // Counts by the coarse tier only, biggest first, for the one-line summary that replaces the old
    // "By class" table. Every group is listed: there are at most sixteen, so no cap is needed.
    public static IReadOnlyList<(string Group, int Count)> Counts(IEnumerable<AuditFinding> findings) => findings
        .GroupBy(GroupOf)
        .Select(g => (Group: g.Key, Count: g.Count()))
        .OrderByDescending(t => t.Count)
        .ThenBy(t => GroupRank.TryGetValue(t.Group, out var i) ? i : GroupOrder.Length)
        .ThenBy(t => t.Group, StringComparer.OrdinalIgnoreCase)
        .ToList();

    // The class values the table above does not cover, so a release that renames or adds a class
    // says so at the end of a run instead of quietly labelling those rows by kingdom alone.
    public static IReadOnlyList<string> UnknownClasses(IEnumerable<AuditFinding> findings) => findings
        .Select(f => f.Class)
        .Where(c => !IsMissing(c) && !ByClass.ContainsKey(c!.Trim()))
        .Select(c => c!.Trim())
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .OrderBy(c => c, StringComparer.OrdinalIgnoreCase)
        .ToList();

    public static string CountLine(IEnumerable<AuditFinding> findings) =>
        string.Join(", ", Counts(findings).Select(t => $"{t.Group} ({t.Count:N0})"));
}
