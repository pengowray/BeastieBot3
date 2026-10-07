namespace BeastieBot3.Site.Display;

/// A taxon the home page can suggest, as the site database has it.
public sealed record ExampleTaxon(long TaxonId, string ScientificName, string? CommonName, string? Category);

/// One suggestion on the home page: the name to show, in italics when it is a scientific name, and
/// the taxon page it links to. A common name is in the link's q, as when a search for it goes to
/// the taxon page, so the page links to all the search results for it.
public sealed record HomeExample(string Text, bool Italic, string Url);

/// The search suggestions on the home page, picked again for every visit. The first is always a
/// large, well-known animal and one is always a plant; a bat is included more often than the size of
/// its pool would give, and threatened species are more likely to be picked than others. The pools
/// are scientific names; a name the site database does not have as a species in the release is
/// left out, and the database gives the common name and category.
public static class HomeExamples {
    public const int Count = 5;

    /// The chance that the suggestions include a bat.
    public const double BatChance = 0.5;

    /// The chance that a suggestion shows the scientific name when the taxon also has a common name.
    public const double ScientificNameChance = 0.3;

    public static readonly IReadOnlyList<string> Megafauna = [
        "Ursus maritimus", "Panthera tigris", "Loxodonta africana", "Loxodonta cyclotis", "Elephas maximus",
        "Ailuropoda melanoleuca", "Panthera uncia", "Gorilla beringei", "Gorilla gorilla", "Pongo pygmaeus",
        "Pongo abelii", "Pan troglodytes", "Pan paniscus", "Acinonyx jubatus", "Panthera leo", "Panthera onca",
        "Balaenoptera musculus", "Physeter macrocephalus", "Diceros bicornis", "Rhinoceros sondaicus",
        "Dicerorhinus sumatrensis", "Rhincodon typus", "Carcharodon carcharias", "Dermochelys coriacea",
        "Varanus komodoensis", "Phocoena sinus", "Giraffa camelopardalis", "Okapia johnstoni",
        "Pseudoryx nghetinhensis", "Gavialis gangeticus", "Equus grevyi", "Lycaon pictus", "Bison bonasus",
        "Hippopotamus amphibius", "Ursus arctos", "Tremarctos ornatus", "Tapirus indicus", "Bos gaurus",
        "Bubalus mindorensis", "Gymnogyps californianus", "Trichechus manatus", "Dugong dugon",
    ];

    public static readonly IReadOnlyList<string> Plants = [
        "Wollemia nobilis", "Welwitschia mirabilis", "Adansonia grandidieri", "Dracaena cinnabari",
        "Sequoiadendron giganteum", "Sequoia sempervirens", "Araucaria araucana", "Dionaea muscipula",
        "Ginkgo biloba", "Nepenthes rajah", "Lodoicea maldivica", "Metasequoia glyptostroboides",
        "Encephalartos woodii", "Pinus longaeva", "Abies nebrodensis", "Swietenia macrophylla",
        "Dalbergia nigra", "Kokia cookei", "Brighamia insignis", "Cypripedium calceolus",
    ];

    public static readonly IReadOnlyList<string> Bats = [
        "Pteropus mariannus", "Pteropus rodricensis", "Craseonycteris thonglongyai", "Pteropus poliocephalus",
        "Myotis sodalis", "Acerodon jubatus", "Pteropus conspicillatus", "Mystacina tuberculata",
        "Macroderma gigas", "Eumops floridanus", "Pteropus livingstonii", "Leptonycteris nivalis",
        "Desmodus rotundus", "Myotis grisescens", "Rhinolophus ferrumequinum", "Pteropus vampyrus",
        "Hypsignathus monstrosus",
    ];

    public static readonly IReadOnlyList<string> Others = [
        "Ambystoma mexicanum", "Strigops habroptilus", "Eretmochelys imbricata", "Manis javanica",
        "Danaus plexippus", "Latimeria chalumnae", "Pithecophaga jefferyi", "Geronticus eremita",
        "Sphenodon punctatus", "Dryococelus australis", "Incilius periglenes", "Thylacinus cynocephalus",
        "Raphus cucullatus", "Acropora palmata", "Balaeniceps rex", "Ornithorhynchus anatinus",
        "Phascolarctos cinereus", "Myrmecobius fasciatus", "Andrias davidianus", "Pangasianodon gigas",
        "Huso huso", "Ectopistes migratorius", "Rhinoptilus bitorquatus", "Nipponia nippon",
        "Polyodon spathula", "Mantella aurantiaca", "Psephurus gladius", "Hippocampus capensis",
        "Margaritifera margaritifera", "Atelopus zeteki", "Platalea minor", "Calidris pygmaea",
        "Pristis pristis", "Lipotes vexillifer", "Dendrolagus goodfellowi", "Aptenodytes forsteri",
        "Hapalemur aureus", "Propithecus candidus", "Daubentonia madagascariensis",
    ];

    /// Every name in the pools, for the database lookup.
    public static IEnumerable<string> AllNames => Megafauna.Concat(Plants).Concat(Bats).Concat(Others);

    /// Up to Count suggestions: a megafauna species first, then a plant, a bat (with BatChance) and
    /// other species in random order. available: the pool names the database has, by scientific name.
    public static IReadOnlyList<HomeExample> Pick(Random random, IReadOnlyDictionary<string, ExampleTaxon> available) {
        var picked = new List<ExampleTaxon>();
        var hasFirst = Draw(random, Megafauna, available, picked);
        Draw(random, Plants, available, picked);
        if (random.NextDouble() < BatChance) {
            Draw(random, Bats, available, picked);
        }
        // Bats are left out here, so BatChance is the chance of a bat.
        var others = Plants.Concat(Others).ToList();
        while (picked.Count < Count && Draw(random, others, available, picked)) {
        }
        var skip = hasFirst ? 1 : 0;
        random.Shuffle(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(picked)[skip..]);

        var examples = picked.Select(t => Show(t, t.CommonName is null || random.NextDouble() < ScientificNameChance)).ToList();
        // Show at least one scientific name, so the suggestions show that both kinds of name work.
        if (examples.Count > 1 && examples.All(e => !e.Italic)) {
            var i = random.Next(examples.Count);
            examples[i] = Show(picked[i], scientific: true);
        }
        return examples;
    }

    /// How much more likely a taxon is to be picked than a Least Concern one: threatened most, Near
    /// Threatened in between.
    public static int Weight(string? category) => category switch {
        "CR" or "CR(PE)" or "CR(PEW)" or "EN" or "VU" => 4,
        "NT" or "LR/nt" or "LR/cd" => 2,
        _ => 1,
    };

    private static HomeExample Show(ExampleTaxon taxon, bool scientific) =>
        scientific
            ? new HomeExample(taxon.ScientificName, true, $"/species/{taxon.TaxonId}")
            : new HomeExample(taxon.CommonName!, false, $"/species/{taxon.TaxonId}?q={Uri.EscapeDataString(taxon.CommonName!)}");

    // Adds one taxon from the pool that is not picked yet, weighted by category. False when the pool
    // has none left.
    private static bool Draw(Random random, IReadOnlyList<string> pool, IReadOnlyDictionary<string, ExampleTaxon> available,
        List<ExampleTaxon> picked) {
        var candidates = pool.Distinct()
            .Select(name => available.GetValueOrDefault(name))
            .OfType<ExampleTaxon>()
            .Where(t => !picked.Contains(t))
            .ToList();
        if (candidates.Count == 0) {
            return false;
        }
        var ticket = random.Next(candidates.Sum(t => Weight(t.Category)));
        foreach (var taxon in candidates) {
            ticket -= Weight(taxon.Category);
            if (ticket < 0) {
                picked.Add(taxon);
                return true;
            }
        }
        throw new InvalidOperationException("Unreachable: the ticket is below the total weight.");
    }
}
