using BeastieBot3.Shared.SiteData;
using BeastieBot3.Shared.Wikitext;
using Microsoft.Data.Sqlite;

// Compares the countries IUCN's latest global assessments code (the site database's taxon_area)
// with a country checklist from another source (ChecklistStore), species by species.
//
// Names: an IUCN species of the source's group matches the source's species of the same name, else
// the one the source lists the IUCN name as a synonym of, else the one an IUCN synonym of the taxon
// names; a name that leads to two or more source species is not matched.
// Places: ISO codes as they are; country names through AreaNames (a part of a country counts as the
// country); TDWG level-3 regions through rules/tdwg-level4-iso.csv (a region of several countries
// covers each of them).
// Records: IUCN native = native or reintroduced, presence not uncertain; uncertain records (either
// side) and introduced or vagrant records never count as a disagreement, only native ones do.

namespace BeastieBot3.Checklists;

/// A source's group in the IUCN data: the column and value its species are in.
internal sealed record ChecklistGroup(string Column, string Value);

/// One IUCN species compared with a source. IucnOnly: countries IUCN records as native that the
/// source does not list at all. SourceOnly: places the source lists as native that IUCN does not
/// record in any way. SingleCountry: the source's only native country, when it lists one.
internal sealed record ChecklistComparison(long TaxonId, string ScientificName, string SourceName, string ClassName,
    IReadOnlyList<string> IucnOnly, IReadOnlyList<string> SourceOnly, string? IucnEndemic, string? SingleCountry, int IucnNative, int SourceNative,
    bool BySynonym = false);

/// Lumped: IUCN species matched by synonym to a checklist species that another IUCN species also
/// matches (the checklist treats them as one species), which are not compared.
internal sealed record ChecklistCrosscheckResult(string Source, int IucnSpecies, int Matched, int MatchedBySynonym, int Compared,
    IReadOnlyList<ChecklistComparison> Comparisons, IReadOnlyDictionary<string, int> UnreadPlaces, int Lumped = 0);

internal static class ChecklistCrosscheck {
    public static readonly IReadOnlyDictionary<string, ChecklistGroup> Groups = new Dictionary<string, ChecklistGroup>(StringComparer.Ordinal) {
        ["mdd"] = new("class_name", "MAMMALIA"),
        ["wcvp"] = new("phylum", "TRACHEOPHYTA"),
        ["reptiledb"] = new("class_name", "REPTILIA"),
        ["amphibiaweb"] = new("class_name", "AMPHIBIA"),
    };

    private sealed record IucnTaxon(long TaxonId, string Name, string ClassName, List<string> Synonyms,
        HashSet<string> Native, HashSet<string> AnyRecord, string? Endemic);

    public static ChecklistCrosscheckResult Run(string source, ChecklistStore store, SqliteConnection site, IReadOnlyDictionary<string, string[]> tdwg) {
        var group = Groups[source];
        var areas = new AreaNames(ReadAreas(site));
        var taxa = ReadIucn(site, group);

        // The source: its species, each with native places and every place. A place is a set of
        // country codes (a TDWG region or "Hispaniola" covers several), with a label.
        var unread = new Dictionary<string, int>(StringComparer.Ordinal);
        var native = new Dictionary<string, List<Place>>(StringComparer.Ordinal);
        var listed = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (var row in store.Rows(source)) {
            var countries = Countries(row, areas, tdwg);
            if (countries.Count == 0) {
                if (row.Scheme == ChecklistSchemes.Name) {
                    unread[row.Area] = unread.GetValueOrDefault(row.Area) + 1;
                }
                continue;
            }
            var key = SiteNameKey.Fold(row.ScientificName);
            (listed.TryGetValue(key, out var all) ? all : listed[key] = new HashSet<string>(StringComparer.Ordinal)).UnionWith(countries);
            if (row.Origin is ChecklistOrigins.Native or ChecklistOrigins.Extinct) {
                var label = countries.Count == 1 ? countries.First() : row.Area;
                (native.TryGetValue(key, out var n) ? n : native[key] = []).Add(new Place(label, countries));
            }
        }
        var sourceNames = store.Rows(source).Select(r => r.ScientificName).Distinct(StringComparer.Ordinal)
            .GroupBy(SiteNameKey.Fold).ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
        var synonyms = store.Synonyms(source).ToDictionary(p => SiteNameKey.Fold(p.Key), p => p.Value.Select(SiteNameKey.Fold).Distinct().ToList(),
            StringComparer.Ordinal);

        string? Match(IucnTaxon taxon, out bool bySynonym) {
            bySynonym = false;
            var key = SiteNameKey.Fold(taxon.Name);
            if (sourceNames.ContainsKey(key)) {
                return key;
            }
            bySynonym = true;
            if (synonyms.TryGetValue(key, out var accepted) && accepted.Count == 1 && sourceNames.ContainsKey(accepted[0])) {
                return accepted[0];
            }
            var viaIucn = taxon.Synonyms.Select(SiteNameKey.Fold).Where(sourceNames.ContainsKey).Distinct().ToList();
            return viaIucn.Count == 1 ? viaIucn[0] : null;
        }

        int matched = 0, bySynonymCount = 0, lumped = 0;
        var matches = taxa.Select(t => (Taxon: t, Key: Match(t, out var s), BySynonym: s)).Where(m => m.Key is not null).ToList();
        var taxaPerKey = matches.GroupBy(m => m.Key!).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        var comparisons = new List<ChecklistComparison>();
        foreach (var (taxon, matchKey, bySynonym) in matches) {
            var key = matchKey!;
            matched++;
            if (bySynonym) {
                bySynonymCount++;
                if (taxaPerKey[key] > 1) {
                    lumped++;
                    continue;
                }
            }
            var sourceNative = native.GetValueOrDefault(key) ?? [];
            var sourceAll = listed.GetValueOrDefault(key) ?? [];
            if (taxon.Native.Count == 0 || sourceNative.Count == 0) {
                continue;
            }
            // One native place of one country: the checklist's only native country.
            var nativeCountries = sourceNative.Where(p => p.Countries.Count == 1).Select(p => p.Countries.First()).Distinct().ToList();
            var single = sourceNative.All(p => p.Countries.Count == 1) && nativeCountries.Count == 1 ? nativeCountries[0] : null;
            comparisons.Add(new ChecklistComparison(taxon.TaxonId, taxon.Name, sourceNames[key], taxon.ClassName,
                [.. taxon.Native.Where(c => !sourceAll.Contains(c)).Order(StringComparer.Ordinal)],
                [.. sourceNative.Where(p => !p.Countries.Overlaps(taxon.AnyRecord)).Select(p => p.Label).Distinct().Order(StringComparer.Ordinal)],
                taxon.Endemic, single, taxon.Native.Count, sourceNative.Select(p => p.Label).Distinct().Count(), bySynonym));
        }
        return new ChecklistCrosscheckResult(source, taxa.Count, matched, bySynonymCount, comparisons.Count, comparisons, unread, lumped);
    }

    private sealed record Place(string Label, HashSet<string> Countries);

    // Places that are not one country: islands shared by two, old names.
    private static readonly Dictionary<string, string[]> PlaceAliases = new(StringComparer.Ordinal) {
        ["hispaniola"] = ["HT", "DO"], ["new guinea"] = ["ID", "PG"], ["borneo"] = ["BN", "ID", "MY"], ["timor"] = ["ID", "TL"],
        ["zaire"] = ["CD"], ["trinidad"] = ["TT"], ["tobago"] = ["TT"], ["sint maarten"] = ["SX"], ["saint martin"] = ["MF", "SX"],
        ["northern marianas"] = ["MP"], ["northern mariana islands"] = ["MP"], ["cocos islands"] = ["CC"], ["faroe"] = ["FO"], ["faroe islands"] = ["FO"],
        ["andaman and nicobar islands"] = ["IN"], ["andaman islands"] = ["IN"], ["nicobar islands"] = ["IN"], ["galapagos islands"] = ["EC"],
        ["french southern and antarctic lands"] = ["TF"], ["prince edward islands"] = ["ZA"], ["antarctica"] = ["AQ"],
        ["democratic republic congo"] = ["CD"], ["republic of congo"] = ["CG"], ["ussr"] = [], ["yugoslavia"] = [],
        ["sumatra"] = ["ID"], ["java"] = ["ID"], ["sulawesi"] = ["ID"], ["sri lanka"] = ["LK"], ["tasmania"] = ["AU"],
        ["sardinia"] = ["IT"], ["sicily"] = ["IT"], ["corsica"] = ["FR"], ["crete"] = ["GR"], ["hainan"] = ["CN"], ["okinawa"] = ["JP"],
        ["luzon"] = ["PH"], ["mindanao"] = ["PH"], ["palawan"] = ["PH"], ["sabah"] = ["MY"], ["sarawak"] = ["MY"], ["kalimantan"] = ["ID"],
        ["zanzibar"] = ["TZ"], ["socotra"] = ["YE"], ["canary islands"] = ["ES"], ["balearic islands"] = ["ES"], ["azores"] = ["PT"], ["madeira"] = ["PT"],
        ["bioko"] = ["GQ"], ["pemba"] = ["TZ"], ["nosy be"] = ["MG"], ["isla de la juventud"] = ["CU"], ["new britain"] = ["PG"], ["new ireland"] = ["PG"],
        ["bougainville"] = ["PG"], ["halmahera"] = ["ID"], ["sumba"] = ["ID"], ["flores"] = ["ID"], ["lombok"] = ["ID"], ["bali"] = ["ID"],
        ["republic of south africa"] = ["ZA"], ["republic of south sudan"] = ["SS"], ["republic of georgia"] = ["GE"],
        ["democratic republic of congo"] = ["CD"], ["comoro islands"] = ["KM"], ["irian jaya"] = ["ID"], ["soviet union"] = [],
        ["natal"] = ["ZA"], ["saint vincent"] = ["VC"], ["isla margarita"] = ["VE"], ["admiralty islands"] = ["PG"], ["admirality islands"] = ["PG"],
        ["nosy komba"] = ["MG"], ["nosy sakatia"] = ["MG"], ["pulau tioman"] = ["MY"], ["ile de la tortue"] = ["HT"], ["bismarck archipelago"] = ["PG"],
    };

    // Words that say where in a place, before or after its name ("N China", "Kenya coast").
    private static readonly HashSet<string> Qualifiers = new(StringComparer.Ordinal) {
        "n", "s", "e", "w", "ne", "nw", "se", "sw", "c", "ene", "ese", "nne", "nnw", "sse", "ssw", "wnw", "wsw", "extreme", "central",
        "northern", "southern", "eastern", "western", "north", "south", "east", "west", "northeastern", "northwestern", "southeastern",
        "southwestern", "coastal", "coast", "lowland", "lowlands", "highlands", "probably", "possibly", "likely", "also", "isolated",
        "record", "records", "from", "in", "the", "of", "and", "or", "mainland", "region", "province", "state", "islands", "island",
        "westernmost", "easternmost", "northernmost", "southernmost", "upper", "lower", "parts", "part", "areas", "area", "all",
        "ce", "nc", "sc", "ec", "wc", "cn", "cs", "ne.", "extreme", "far", "southernmost", "interior", "inland",
    };

    /// The country codes of a place written as text: the whole text as an area name, else a place
    /// alias, else the same with words of position taken off the ends ("N China" is China; "South
    /// Africa" stays South Africa because it is a country name). Empty when no reading finds a country.
    internal static HashSet<string> PlaceCountries(string place, AreaNames areas) {
        // "Nossi Be = Nosy Bé", "Hispaniola: Dominican Republic": the part after the sign, else before it.
        foreach (var sign in new[] { '=', ':' }) {
            var at = place.IndexOf(sign);
            if (at > 0) {
                var after = PlaceCountries(place[(at + 1)..], areas);
                return after.Count > 0 ? after : PlaceCountries(place[..at], areas);
            }
        }
        var words = AreaNames.Key(place).Split(' ', StringSplitOptions.RemoveEmptyEntries).ToList();
        while (words.Count > 0) {
            var text = string.Join(' ', words);
            if (PlaceAliases.TryGetValue(text, out var codes)) {
                return new HashSet<string>(codes, StringComparer.Ordinal);
            }
            if (areas.Find(text) is { } area) {
                return new HashSet<string>([area.Country ?? area.Code], StringComparer.Ordinal);
            }
            if (Qualifiers.Contains(words[0])) {
                words.RemoveAt(0);
            } else if (Qualifiers.Contains(words[^1])) {
                words.RemoveAt(words.Count - 1);
            } else {
                break;
            }
        }
        return new HashSet<string>(StringComparer.Ordinal);
    }

    // The ISO alpha-2 codes a row's place stands for: none when it cannot be read.
    internal static HashSet<string> Countries(ChecklistArea row, AreaNames areas, IReadOnlyDictionary<string, string[]> tdwg) {
        var set = new HashSet<string>(StringComparer.Ordinal);
        switch (row.Scheme) {
            case ChecklistSchemes.Iso2:
                if (row.Area.Length == 2) {
                    set.Add(row.Area);
                }
                break;
            case ChecklistSchemes.Tdwg3:
                if (tdwg.TryGetValue(row.Area, out var codes)) {
                    set.UnionWith(codes);
                }
                break;
            default:
                set.UnionWith(PlaceCountries(row.Area, areas));
                break;
        }
        return set;
    }

    /// rules/tdwg-level4-iso.csv: each TDWG level-3 region and the ISO codes of its level-4 parts.
    public static IReadOnlyDictionary<string, string[]> ReadTdwg(string path) {
        var map = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (var line in File.ReadLines(path).Skip(1)) {
            var f = line.Split(',');
            if (f.Length >= 3 && f[2].Length == 2) {
                // Codes retired since the table was made: East Timor, the Netherlands Antilles.
                string[] codes = f[2] switch { "TP" => ["TL"], "AN" => ["CW", "SX", "BQ"], var c => [c] };
                // Region SUD is Sudan as it was before 2011, South Sudan included.
                if (f[1] == "SUD") {
                    codes = [.. codes, "SS"];
                }
                (map.TryGetValue(f[1], out var set) ? set : map[f[1]] = []).UnionWith(codes);
            }
        }
        return map.ToDictionary(p => p.Key, p => p.Value.Order(StringComparer.Ordinal).ToArray(), StringComparer.Ordinal);
    }

    private static List<AreaName> ReadAreas(SqliteConnection site) {
        using var command = site.CreateCommand();
        command.CommandText = "SELECT code, name, country FROM area";
        using var reader = command.ExecuteReader();
        var list = new List<AreaName>();
        while (reader.Read()) {
            list.Add(new AreaName(reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }
        return list;
    }

    // The group's species in the release with a latest global assessment, their IUCN synonyms, and
    // their countries (parts of countries are left out: each comes with its country).
    private static List<IucnTaxon> ReadIucn(SqliteConnection site, ChecklistGroup group) {
        var taxa = new Dictionary<long, IucnTaxon>();
        using (var command = site.CreateCommand()) {
            command.CommandText = $"""
                SELECT taxon_id, scientific_name, class_name FROM taxon
                WHERE in_release = 1 AND kind = 'species' AND latest_global_assessment_id IS NOT NULL AND {group.Column} = @value
                """;
            command.Parameters.AddWithValue("@value", group.Value);
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                taxa[reader.GetInt64(0)] = new IucnTaxon(reader.GetInt64(0), reader.GetString(1), reader.IsDBNull(2) ? "" : reader.GetString(2), [],
                    new HashSet<string>(StringComparer.Ordinal), new HashSet<string>(StringComparer.Ordinal), null);
            }
        }
        using (var command = site.CreateCommand()) {
            command.CommandText = "SELECT taxon_id, name FROM name WHERE name_type = 'synonym' AND source = 'iucn'";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (taxa.TryGetValue(reader.GetInt64(0), out var t)) {
                    t.Synonyms.Add(reader.GetString(1));
                }
            }
        }
        var endemic = new Dictionary<long, string>();
        using (var command = site.CreateCommand()) {
            command.CommandText = "SELECT taxon_id, area, origin, presence, endemic FROM taxon_area WHERE length(area) = 2";
            using var reader = command.ExecuteReader();
            while (reader.Read()) {
                if (!taxa.TryGetValue(reader.GetInt64(0), out var t)) {
                    continue;
                }
                var area = reader.GetString(1);
                var origin = (AreaOrigin)reader.GetInt32(2);
                var presence = (AreaPresence)reader.GetInt32(3);
                t.AnyRecord.Add(area);
                if (origin is AreaOrigin.Native or AreaOrigin.Reintroduced && presence != AreaPresence.PresenceUncertain) {
                    t.Native.Add(area);
                }
                if (reader.GetInt64(4) != 0) {
                    endemic[t.TaxonId] = area;
                }
            }
        }
        return [.. taxa.Values.Select(t => endemic.TryGetValue(t.TaxonId, out var e) ? t with { Endemic = e } : t).OrderBy(t => t.Name, StringComparer.Ordinal)];
    }
}
