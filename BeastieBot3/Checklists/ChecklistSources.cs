using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using BeastieBot3.Shared.SiteData;
using CsvHelper;
using CsvHelper.Configuration;

// The country checklists `checklists import` reads, each with where to download it, its licence,
// and a parser that turns the file into ChecklistArea rows (and synonyms):
//   mdd          Mammal Diversity Database (Zenodo, CC BY 4.0): countryDistribution, country names
//                separated by "|", "?" for uncertain; each species' id and its subspecies column
//                (MddSubspecies).
//   wcvp         World Checklist of Vascular Plants (Kew, CC BY): wcvp_distribution.csv, TDWG level-3
//                areas with introduced / extinct / location_doubtful flags; synonyms from wcvp_names.csv.
//   reptiledb    The Reptile Database on ChecklistBank (dataset 1008, ColDP, CC BY): Distribution.tsv,
//                free text ("N China (W Xinjiang), Kyrgyzstan") split into place names; the accepted
//                subspecies (Taxon.tsv rows of rank subspecies under a species) with their authorship.
//   amphibiaweb  AmphibiaWeb's names file (CC BY-NC 4.0): isocc and intro_isocc, ISO alpha-2 codes.
//                AmphibiaWeb asks to be contacted before large downloads; the default URL is the
//                monthly snapshot they publish on GitHub.

namespace BeastieBot3.Checklists;

internal sealed record ChecklistParse(string? Version, IReadOnlyList<ChecklistArea> Rows, IReadOnlyList<(string Name, string Accepted)> Synonyms,
    IReadOnlyList<string> Notes) {
    /// English common names and synonyms of the source's species.
    public IReadOnlyList<ChecklistName> Names { get; init; } = [];

    /// The source's accepted species with their record ids (mdd and reptiledb).
    public IReadOnlyList<ChecklistSpecies> Species { get; init; } = [];

    /// The subspecies the source lists under its species (mdd and reptiledb), and the number of
    /// subspecies entries the parser could not read.
    public IReadOnlyList<ChecklistInfraspecific> Infraspecific { get; init; } = [];
    public int InfraspecificNotRead { get; init; }
}

/// ExtraFiles: more files the parser reads from the same folder (published path, URL).
internal sealed record ChecklistSource(string Key, string Title, string Url, string FileName, string Licence, Func<string, ChecklistParse> Parse) {
    public IReadOnlyList<(string FileName, string Url)> ExtraFiles { get; init; } = [];
}

internal static partial class ChecklistSources {
    public static readonly IReadOnlyList<ChecklistSource> All = [
        new("mdd", "Mammal Diversity Database",
            "https://zenodo.org/api/records/21654811/files/MDD_v2.5_6904species.csv/content", "MDD_v2.5_6904species.csv", "CC BY 4.0", ParseMdd) {
            ExtraFiles = [(MddSynonymsFile, "https://zenodo.org/api/records/21654811/files/Species_Syn_v2.5.csv/content")],
        },
        new("wcvp", "World Checklist of Vascular Plants (Kew)",
            "https://sftp.kew.org/pub/data-repositories/WCVP/wcvp.zip", "wcvp.zip", "CC BY", ParseWcvp),
        new("reptiledb", "The Reptile Database (ChecklistBank dataset 1008)",
            "https://api.checklistbank.org/dataset/1008/archive.zip", "reptiledb_clb1008.zip", "CC BY", ParseReptileDatabase),
        new("amphibiaweb", "AmphibiaWeb",
            "https://raw.githubusercontent.com/AmphibiaWeb/taxonomy-archive/master/amphib_names_20260401.txt", "amphib_names_20260401.txt",
            "CC BY-NC 4.0", ParseAmphibiaWeb),
    ];

    public static ChecklistSource? Find(string key) => All.FirstOrDefault(s => string.Equals(s.Key, key, StringComparison.OrdinalIgnoreCase));

    /// The title of a source, GBIF included.
    public static string TitleOf(string key) => key == GbifChecklist.Source ? GbifChecklist.Title : Find(key)?.Title ?? key;

    // ---------------------------------------------------------------- MDD

    public const string MddSynonymsFile = "MDD_Species_Syn_v2.5.csv";

    internal static ChecklistParse ParseMdd(string path) {
        ChecklistParse parse;
        using (var reader = new StreamReader(path, Encoding.UTF8)) {
            parse = ParseMdd(reader, Regex.Match(Path.GetFileName(path), @"v\d+(\.\d+)*") is { Success: true } v ? v.Value : null);
        }
        var synonyms = Path.Combine(Path.GetDirectoryName(path) ?? ".", MddSynonymsFile);
        if (File.Exists(synonyms)) {
            using var reader = new StreamReader(synonyms, Encoding.UTF8);
            parse = parse with { Names = [.. parse.Names, .. ParseMddSynonyms(reader)] };
        }
        return parse;
    }

    // Species_Syn: one row per name of a species. Kept: the synonyms, and the original combination of
    // each valid species ("Rattus latidens" for Abditomys latidens), with the author and year; left
    // out: nomina dubia, species inquirendae, hybrids, unavailable and composite names. An original
    // combination's authority has no brackets (MDD_authority_parentheses is about the current name).
    internal static IEnumerable<ChecklistName> ParseMddSynonyms(TextReader text) {
        using var csv = new CsvReader(text, new CsvConfiguration(CultureInfo.InvariantCulture) { BadDataFound = null, MissingFieldFound = null });
        csv.Read();
        csv.ReadHeader();
        var names = new List<ChecklistName>();
        while (csv.Read()) {
            var validity = csv.GetField("MDD_validity");
            var species = csv.GetField("MDD_species")?.Replace('_', ' ').Trim();
            var name = csv.GetField("MDD_original_combination")?.Trim();
            if (validity is not ("synonym" or "species") || string.IsNullOrEmpty(species) || string.IsNullOrEmpty(name) || name == species) {
                continue;
            }
            var author = csv.GetField("MDD_author")?.Trim();
            var year = csv.GetField("MDD_year")?.Trim();
            string? authority = string.IsNullOrEmpty(author) || author == "NA" ? null
                : string.IsNullOrEmpty(year) || year == "NA" ? author : $"{author}, {year}";
            names.Add(new ChecklistName(species, name, ChecklistNameTypes.Synonym, authority));
        }
        return names;
    }

    internal static ChecklistParse ParseMdd(TextReader text, string? version) {
        using var csv = new CsvReader(text, new CsvConfiguration(CultureInfo.InvariantCulture) { BadDataFound = null, MissingFieldFound = null });
        csv.Read();
        csv.ReadHeader();
        var rows = new List<ChecklistArea>();
        var names = new List<ChecklistName>();
        var species = new List<ChecklistSpecies>();
        var subspecies = new List<ChecklistInfraspecific>();
        var subspeciesNotRead = 0;
        while (csv.Read()) {
            var name = csv.GetField("sciName")?.Replace('_', ' ').Trim();
            if (!string.IsNullOrEmpty(name)) {
                // id: the species' MDD id, which its page on the MDD website has in its address.
                species.Add(new ChecklistSpecies(name, csv.GetField("id")?.Trim() is { Length: > 0 } id && id != "NA" ? id : null));
                var (read, notRead) = MddSubspecies.Parse(name, csv.GetField("subspecies"));
                subspecies.AddRange(read);
                subspeciesNotRead += notRead;
                // English names: mainCommonName, then otherCommonNames separated by "|".
                var common = new[] { csv.GetField("mainCommonName") }.Concat((csv.GetField("otherCommonNames") ?? "").Split('|'));
                foreach (var c in common.Select(c => c?.Trim()).Where(c => c is { Length: > 0 } && c != "NA").Distinct()) {
                    names.Add(new ChecklistName(name, c!, ChecklistNameTypes.Common));
                }
            }
            var countries = csv.GetField("countryDistribution");
            if (string.IsNullOrEmpty(name) || string.IsNullOrWhiteSpace(countries) || countries == "NA") {
                continue;
            }
            foreach (var part in countries.Split('|', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
                if (part is "NA" or "Domesticated") {
                    continue;
                }
                var uncertain = part.EndsWith('?');
                rows.Add(new ChecklistArea(name, part.TrimEnd('?').Trim(), ChecklistSchemes.Name, uncertain ? ChecklistOrigins.Uncertain : ChecklistOrigins.Native));
            }
        }
        return new ChecklistParse(version, rows, [], []) {
            Names = names, Species = species, Infraspecific = subspecies, InfraspecificNotRead = subspeciesNotRead,
        };
    }

    // ---------------------------------------------------------------- WCVP

    internal static ChecklistParse ParseWcvp(string path) {
        using var zip = ZipFile.OpenRead(path);
        var namesEntry = zip.GetEntry("wcvp_names.csv") ?? throw new InvalidDataException("wcvp_names.csv is not in the zip.");
        var distributionEntry = zip.GetEntry("wcvp_distribution.csv") ?? throw new InvalidDataException("wcvp_distribution.csv is not in the zip.");
        using var names = new StreamReader(namesEntry.Open(), Encoding.UTF8);
        using var distribution = new StreamReader(distributionEntry.Open(), Encoding.UTF8);
        return ParseWcvp(names, distribution, $"WCVP ({namesEntry.LastWriteTime:yyyy-MM-dd})");
    }

    internal static ChecklistParse ParseWcvp(TextReader names, TextReader distribution, string? version) {
        // plant_name_id -> taxon_name of accepted species; synonyms of species to their accepted name.
        var accepted = new Dictionary<string, string>(StringComparer.Ordinal);
        var synonymOf = new List<(string Name, string AcceptedId)>();
        var header = Header(names.ReadLine(), '|');
        int id = header["plant_name_id"], rank = header["taxon_rank"], status = header["taxon_status"], name = header["taxon_name"],
            acceptedId = header["accepted_plant_name_id"];
        for (var line = names.ReadLine(); line is not null; line = names.ReadLine()) {
            var f = line.Split('|');
            if (f.Length <= Math.Max(acceptedId, name)) {
                continue;
            }
            if (f[status] == "Accepted" && f[rank] == "Species") {
                accepted[f[id]] = f[name];
            } else if (f[status] == "Synonym" && f[acceptedId].Length > 0) {
                synonymOf.Add((f[name], f[acceptedId]));
            }
        }
        var rows = new List<ChecklistArea>();
        header = Header(distribution.ReadLine(), '|');
        int plant = header["plant_name_id"], area = header["area_code_l3"], introduced = header["introduced"], extinct = header["extinct"],
            doubtful = header["location_doubtful"];
        for (var line = distribution.ReadLine(); line is not null; line = distribution.ReadLine()) {
            var f = line.Split('|');
            if (f.Length <= doubtful || f[area].Length == 0 || !accepted.TryGetValue(f[plant], out var taxon)) {
                continue;
            }
            var origin = f[doubtful] == "1" ? ChecklistOrigins.Uncertain
                : f[introduced] == "1" ? ChecklistOrigins.Introduced
                : f[extinct] == "1" ? ChecklistOrigins.Extinct
                : ChecklistOrigins.Native;
            rows.Add(new ChecklistArea(taxon, f[area], ChecklistSchemes.Tdwg3, origin));
        }
        var synonyms = synonymOf.Where(s => accepted.ContainsKey(s.AcceptedId)).Select(s => (s.Name, accepted[s.AcceptedId])).ToList();
        return new ChecklistParse(version, rows, synonyms, []);
    }

    // ---------------------------------------------------------------- The Reptile Database (ColDP)

    internal static ChecklistParse ParseReptileDatabase(string path) {
        using var zip = ZipFile.OpenRead(path);
        TextReader Open(string file) => new StreamReader((zip.GetEntry(file) ?? throw new InvalidDataException($"{file} is not in the zip.")).Open(), Encoding.UTF8);
        using var names = Open("Name.tsv");
        using var taxa = Open("Taxon.tsv");
        using var distribution = Open("Distribution.tsv");
        using var synonyms = Open("Synonym.tsv");
        // metadata.yaml has the dataset's version ("version: 2026-06").
        string? release = null;
        if (zip.GetEntry("metadata.yaml") is { } metadata) {
            using var yaml = new StreamReader(metadata.Open(), Encoding.UTF8);
            for (var line = yaml.ReadLine(); line is not null && release is null; line = yaml.ReadLine()) {
                if (line.StartsWith("version:", StringComparison.Ordinal)) {
                    release = line["version:".Length..].Trim().Trim('"', '\'') is { Length: > 0 } v ? v : null;
                }
            }
        }
        return ParseReptileDatabase(names, taxa, distribution, synonyms, release is null ? "ChecklistBank 1008" : $"ChecklistBank 1008 ({release})");
    }

    internal static ChecklistParse ParseReptileDatabase(TextReader names, TextReader taxa, TextReader distribution, TextReader synonyms, string? version) {
        var nameText = new Dictionary<string, (string Name, string Rank, string? Authorship)>(StringComparer.Ordinal);
        var h = Header(names.ReadLine(), '\t');
        var authorshipColumn = h.GetValueOrDefault("authorship", -1);
        for (var line = names.ReadLine(); line is not null; line = names.ReadLine()) {
            var f = line.Split('\t');
            if (f.Length > Math.Max(h["scientific_name"], h["rank"])) {
                var authorship = authorshipColumn >= 0 && authorshipColumn < f.Length && f[authorshipColumn].Trim() is { Length: > 0 } a ? a : null;
                nameText[f[h["id"]]] = (f[h["scientific_name"]], f[h["rank"]], authorship);
            }
        }
        var taxonName = new Dictionary<string, string>(StringComparer.Ordinal);
        var species = new List<ChecklistSpecies>();
        // Accepted subspecies: Taxon.tsv rows of rank subspecies (taxon id, parent taxon id, name).
        // The names of rank subspecies in Synonym.tsv are synonyms and are left out.
        var subspeciesTaxa = new List<(string Parent, string Name, string? Authorship)>();
        h = Header(taxa.ReadLine(), '\t');
        var parentColumn = h.GetValueOrDefault("parent_id", -1);
        var linkColumn = h.GetValueOrDefault("link", -1);
        for (var line = taxa.ReadLine(); line is not null; line = taxa.ReadLine()) {
            var f = line.Split('\t');
            if (f.Length <= h["name_id"] || !nameText.TryGetValue(f[h["name_id"]], out var n)) {
                continue;
            }
            if (n.Rank == "species") {
                taxonName[f[h["id"]]] = n.Name;
                var link = linkColumn >= 0 && linkColumn < f.Length ? f[linkColumn] : null;
                species.Add(new ChecklistSpecies(n.Name, ReptileDatabaseRecordId(link, n.Name)));
            } else if (n.Rank is "subspecies" or "subsp." && parentColumn >= 0 && parentColumn < f.Length) {
                subspeciesTaxa.Add((f[parentColumn], n.Name.Trim(), n.Authorship));
            }
        }
        var subspecies = new List<ChecklistInfraspecific>();
        var subspeciesNotRead = 0;
        foreach (var (parent, name, authorship) in subspeciesTaxa) {
            if (taxonName.TryGetValue(parent, out var speciesName) && name.Length > 0) {
                subspecies.Add(new ChecklistInfraspecific(speciesName, name, InfraspecificNames.Subspecies, authorship));
            } else {
                subspeciesNotRead++;
            }
        }
        var rows = new List<ChecklistArea>();
        h = Header(distribution.ReadLine(), '\t');
        for (var line = distribution.ReadLine(); line is not null; line = distribution.ReadLine()) {
            var f = line.Split('\t');
            if (f.Length <= h["area"] || !taxonName.TryGetValue(f[h["taxon_id"]], out var taxon)) {
                continue;
            }
            foreach (var (place, introduced) in PlacesInText(f[h["area"]])) {
                rows.Add(new ChecklistArea(taxon, place, ChecklistSchemes.Name, introduced ? ChecklistOrigins.Introduced : ChecklistOrigins.Native));
            }
        }
        var synonymList = new List<(string, string)>();
        h = Header(synonyms.ReadLine(), '\t');
        for (var line = synonyms.ReadLine(); line is not null; line = synonyms.ReadLine()) {
            var f = line.Split('\t');
            if (f.Length > h["name_id"] && taxonName.TryGetValue(f[h["taxon_id"]], out var acceptedName)
                && nameText.TryGetValue(f[h["name_id"]], out var syn) && syn.Rank == "species") {
                synonymList.Add((syn.Name, acceptedName));
            }
        }
        return new ChecklistParse(version, rows, synonymList, []) {
            Species = species, Infraspecific = subspecies, InfraspecificNotRead = subspeciesNotRead,
        };
    }

    public const string ReptileDatabaseSpeciesUrl = "https://reptile-database.reptarium.cz/species?";

    /// The query of a species' page on the Reptile Database ("genus=Ablepharus&species=alaicus"), the
    /// id that Wikidata's Reptile Database ID (P5473) holds: from the taxon's link, else from its name.
    internal static string? ReptileDatabaseRecordId(string? link, string speciesName) {
        if (link?.Trim() is { Length: > 0 } url && url.StartsWith(ReptileDatabaseSpeciesUrl, StringComparison.OrdinalIgnoreCase)
            && url[ReptileDatabaseSpeciesUrl.Length..] is { Length: > 0 } query) {
            return query;
        }
        var words = speciesName.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return words.Length == 2 ? $"genus={words[0]}&species={words[1]}" : null;
    }

    /// The places a free-text distribution names: "N China (W Xinjiang), Kyrgyzstan, NE Uzbekistan"
    /// gives "N China", "Kyrgyzstan", "NE Uzbekistan". Bracketed text and "introduced to" are taken
    /// out; a part that mentions introduction is marked introduced. Compass words stay, because they
    /// are part of some names ("South Africa"): ChecklistCrosscheck.Countries takes them off only when
    /// the whole place is not a country name.
    internal static IEnumerable<(string Place, bool Introduced)> PlacesInText(string text) {
        var flat = Brackets().Replace(text, " ");
        foreach (var raw in flat.Split([',', ';', '/'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
            var introduced = raw.Contains("introduc", StringComparison.OrdinalIgnoreCase);
            var place = Introduction().Replace(raw, " ");
            place = Regex.Replace(place, @"\s+", " ").Trim(' ', '.', ':', '?', '"');
            if (place.Length >= 3) {
                yield return (place, introduced);
            }
        }
    }

    [GeneratedRegex(@"\([^()]*\)|\[[^\[\]]*\]")]
    private static partial Regex Brackets();

    [GeneratedRegex(@"(?i)\b(?:introduced|introduction)\b(?:\s+(?:to|in|into|on))?")]
    private static partial Regex Introduction();

    // ---------------------------------------------------------------- AmphibiaWeb

    internal static ChecklistParse ParseAmphibiaWeb(string path) {
        using var reader = new StreamReader(path, Encoding.UTF8);
        var date = Regex.Match(Path.GetFileName(path), @"(\d{4})(\d{2})(\d{2})") is { Success: true } m ? $"{m.Groups[1]}-{m.Groups[2]}-{m.Groups[3]}" : null;
        return ParseAmphibiaWeb(reader, date);
    }

    internal static ChecklistParse ParseAmphibiaWeb(TextReader text, string? version) {
        var h = Header(text.ReadLine(), '\t');
        var rows = new List<ChecklistArea>();
        var synonyms = new List<(string, string)>();
        var names = new List<ChecklistName>();
        for (var line = text.ReadLine(); line is not null; line = text.ReadLine()) {
            var f = line.Split('\t');
            if (f.Length <= h["intro_isocc"]) {
                continue;
            }
            var name = $"{f[h["genus"]].Trim()} {f[h["species"]].Trim()}";
            foreach (var code in f[h["isocc"]].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
                rows.Add(new ChecklistArea(name, code.ToUpperInvariant(), ChecklistSchemes.Iso2, ChecklistOrigins.Native));
            }
            foreach (var code in f[h["intro_isocc"]].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)) {
                rows.Add(new ChecklistArea(name, code.ToUpperInvariant(), ChecklistSchemes.Iso2, ChecklistOrigins.Introduced));
            }
            // gaa_name: the Global Amphibian Assessment's (IUCN's) name where it differs.
            if (h.TryGetValue("gaa_name", out var gaa) && f[gaa].Trim() is { Length: > 0 } iucnName && iucnName != name) {
                synonyms.Add((iucnName, name));
            }
            // common_name and synonymies: comma-separated.
            if (h.TryGetValue("common_name", out var commonColumn)) {
                foreach (var c in f[commonColumn].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Distinct()) {
                    names.Add(new ChecklistName(name, c, ChecklistNameTypes.Common));
                }
            }
            if (h.TryGetValue("synonymies", out var synonymColumn)) {
                foreach (var s in f[synonymColumn].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Distinct()) {
                    if (s != name) {
                        names.Add(new ChecklistName(name, s, ChecklistNameTypes.Synonym));
                    }
                }
            }
        }
        return new ChecklistParse(version, rows, synonyms, []) { Names = names };
    }

    private static Dictionary<string, int> Header(string? line, char separator) {
        var header = new Dictionary<string, int>(StringComparer.Ordinal);
        var fields = (line ?? string.Empty).TrimStart('﻿').Split(separator);
        for (var i = 0; i < fields.Length; i++) {
            header.TryAdd(fields[i].Trim(), i);
        }
        return header;
    }
}

internal static class ChecklistOrigins {
    public const string Native = "native";
    public const string Introduced = "introduced";
    public const string Extinct = "extinct";
    public const string Uncertain = "uncertain";
    /// Occurrence records in the area, whatever their origin (GBIF).
    public const string Recorded = "recorded";
}
