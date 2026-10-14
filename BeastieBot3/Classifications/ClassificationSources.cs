using System.IO.Compression;

// The classifications `classifications import` reads, each with where to download it, its licence
// and its reader:
//   avilist  AviList (avilist.org, CC BY 4.0): the workbook of v2025b; orders, families, genera and
//            species, with each bird's BirdLife id (the IUCN taxon id).
//   ioc      IOC World Bird List 15.2, the last version, from ChecklistBank (dataset 2036, ColDP;
//            CC BY 3.0 on worldbirdnames.org): infraclass, order, family, genus.
//   itis     ITIS from ChecklistBank (dataset 2144, ColDP, CC0): every rank ITIS has, with TSNs as ids.
//   powo     Plants of the World Online from ChecklistBank (dataset 2000, DwC-A, CC BY): species and
//            genera linked by parent, families, orders, subclasses, classes from columns; IPNI ids.
//   worms    World Register of Marine Species from ChecklistBank (dataset 2011, DwC-A, CC BY 4.0):
//            every rank WoRMS has, with AphiaIDs as ids. WoRMS does not allow the whole database to
//            be passed on; the site keeps only the groups above IUCN's species.

namespace BeastieBot3.Classifications;

internal sealed record ClassificationParse(string? Version, List<ClassificationNode> Nodes, List<ClassificationName> Names, List<ClassificationLink> Links);

internal sealed record ClassificationSource(string Key, string Title, string Url, string FileName, string Licence, Func<string, ClassificationParse> Parse);

internal static class ClassificationSources {
    public const string AviList = "avilist";
    public const string Ioc = "ioc";
    public const string Itis = "itis";
    public const string Powo = "powo";
    public const string Worms = "worms";

    public static readonly IReadOnlyList<ClassificationSource> All = [
        new(AviList, "AviList v2025b",
            "https://www.avilist.org/wp-content/uploads/2026/06/AviList-v2025b-10Jun2026-extended.xlsx", "AviList-v2025b-10Jun2026-extended.xlsx",
            "CC BY 4.0", path => {
                var (tree, links) = AviListClassification.Read(path);
                var (nodes, names) = tree.Build();
                return new ClassificationParse("v2025b", nodes, names, links);
            }),
        new(Ioc, "IOC World Bird List (ChecklistBank dataset 2036)",
            "https://api.checklistbank.org/dataset/2036/archive.zip", "ioc_clb2036.zip", "CC BY 3.0", Coldp),
        new(Itis, "ITIS (ChecklistBank dataset 2144)",
            "https://api.checklistbank.org/dataset/2144/archive.zip", "itis_clb2144.zip", "CC0", Coldp),
        new(Powo, "Plants of the World Online (ChecklistBank dataset 2000)",
            "https://api.checklistbank.org/dataset/2000/archive.zip", "powo_clb2000.zip", "CC BY",
            path => Dwc(path, "urn:lsid:ipni.org:names:")),
        new(Worms, "World Register of Marine Species (ChecklistBank dataset 2011)",
            "https://api.checklistbank.org/dataset/2011/archive.zip", "worms_clb2011.zip", "CC BY 4.0",
            path => Dwc(path, "urn:lsid:marinespecies.org:taxname:")),
    ];

    public static ClassificationSource? Find(string key) => All.FirstOrDefault(s => string.Equals(s.Key, key, StringComparison.OrdinalIgnoreCase));

    private static ClassificationParse Coldp(string path) {
        var (nodes, names) = ColdpClassification.Read(path).Build();
        return new ClassificationParse(ColdpClassification.Version(path), nodes, names, []);
    }

    // The archive's version: the date its taxon file was written.
    private static ClassificationParse Dwc(string path, string idPrefix) {
        var (nodes, names) = DwcClassification.Read(path, idPrefix).Build();
        string? version;
        using (var zip = ZipFile.OpenRead(path)) {
            version = zip.Entries.FirstOrDefault(e => e.FullName.Equals("taxon.txt", StringComparison.OrdinalIgnoreCase))?.LastWriteTime.ToString("yyyy-MM-dd");
        }
        return new ClassificationParse(version, nodes, names, []);
    }
}
