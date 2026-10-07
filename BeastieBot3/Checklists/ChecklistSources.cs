using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using CsvHelper;
using CsvHelper.Configuration;

// The country checklists `checklists import` reads, each with where to download it, its licence,
// and a parser that turns the file into ChecklistArea rows (and synonyms):
//   mdd          Mammal Diversity Database (Zenodo, CC BY 4.0): countryDistribution, country names
//                separated by "|", "?" for uncertain.
//   wcvp         World Checklist of Vascular Plants (Kew, CC BY): wcvp_distribution.csv, TDWG level-3
//                areas with introduced / extinct / location_doubtful flags; synonyms from wcvp_names.csv.
//   reptiledb    The Reptile Database on ChecklistBank (dataset 1008, ColDP, CC BY): Distribution.tsv,
//                free text ("N China (W Xinjiang), Kyrgyzstan") split into place names.
//   amphibiaweb  AmphibiaWeb's names file (CC BY-NC 4.0): isocc and intro_isocc, ISO alpha-2 codes.
//                AmphibiaWeb asks to be contacted before large downloads; the default URL is the
//                monthly snapshot they publish on GitHub.

namespace BeastieBot3.Checklists;

internal sealed record ChecklistParse(string? Version, IReadOnlyList<ChecklistArea> Rows, IReadOnlyList<(string Name, string Accepted)> Synonyms,
    IReadOnlyList<string> Notes);

internal sealed record ChecklistSource(string Key, string Title, string Url, string FileName, string Licence, Func<string, ChecklistParse> Parse);

internal static partial class ChecklistSources {
    public static readonly IReadOnlyList<ChecklistSource> All = [
        new("mdd", "Mammal Diversity Database",
            "https://zenodo.org/api/records/21654811/files/MDD_v2.5_6904species.csv/content", "MDD_v2.5_6904species.csv", "CC BY 4.0", ParseMdd),
        new("wcvp", "World Checklist of Vascular Plants (Kew)",
            "https://sftp.kew.org/pub/data-repositories/WCVP/wcvp.zip", "wcvp.zip", "CC BY", ParseWcvp),
        new("reptiledb", "The Reptile Database (ChecklistBank dataset 1008)",
            "https://api.checklistbank.org/dataset/1008/archive.zip", "reptiledb_clb1008.zip", "CC BY", ParseReptileDatabase),
        new("amphibiaweb", "AmphibiaWeb",
            "https://raw.githubusercontent.com/AmphibiaWeb/taxonomy-archive/master/amphib_names_20260401.txt", "amphib_names_20260401.txt",
            "CC BY-NC 4.0", ParseAmphibiaWeb),
    ];

    public static ChecklistSource? Find(string key) => All.FirstOrDefault(s => string.Equals(s.Key, key, StringComparison.OrdinalIgnoreCase));

    // ---------------------------------------------------------------- MDD

    internal static ChecklistParse ParseMdd(string path) {
        using var reader = new StreamReader(path, Encoding.UTF8);
        return ParseMdd(reader, Path.GetFileNameWithoutExtension(path));
    }

    internal static ChecklistParse ParseMdd(TextReader text, string? version) {
        using var csv = new CsvReader(text, new CsvConfiguration(CultureInfo.InvariantCulture) { BadDataFound = null, MissingFieldFound = null });
        csv.Read();
        csv.ReadHeader();
        var rows = new List<ChecklistArea>();
        while (csv.Read()) {
            var name = csv.GetField("sciName")?.Replace('_', ' ').Trim();
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
        return new ChecklistParse(version, rows, [], []);
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
        return ParseReptileDatabase(names, taxa, distribution, synonyms, "ChecklistBank 1008");
    }

    internal static ChecklistParse ParseReptileDatabase(TextReader names, TextReader taxa, TextReader distribution, TextReader synonyms, string? version) {
        var nameText = new Dictionary<string, (string Name, string Rank)>(StringComparer.Ordinal);
        var h = Header(names.ReadLine(), '\t');
        for (var line = names.ReadLine(); line is not null; line = names.ReadLine()) {
            var f = line.Split('\t');
            if (f.Length > Math.Max(h["scientific_name"], h["rank"])) {
                nameText[f[h["id"]]] = (f[h["scientific_name"]], f[h["rank"]]);
            }
        }
        var taxonName = new Dictionary<string, string>(StringComparer.Ordinal);
        h = Header(taxa.ReadLine(), '\t');
        for (var line = taxa.ReadLine(); line is not null; line = taxa.ReadLine()) {
            var f = line.Split('\t');
            if (f.Length > h["name_id"] && nameText.TryGetValue(f[h["name_id"]], out var n) && n.Rank == "species") {
                taxonName[f[h["id"]]] = n.Name;
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
        return new ChecklistParse(version, rows, synonymList, []);
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
        var date = Regex.Match(Path.GetFileName(path), @"\d{8}") is { Success: true } m ? m.Value : null;
        return ParseAmphibiaWeb(reader, date is null ? null : $"names file of {date}");
    }

    internal static ChecklistParse ParseAmphibiaWeb(TextReader text, string? version) {
        var h = Header(text.ReadLine(), '\t');
        var rows = new List<ChecklistArea>();
        var synonyms = new List<(string, string)>();
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
        }
        return new ChecklistParse(version, rows, synonyms, []);
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
}
