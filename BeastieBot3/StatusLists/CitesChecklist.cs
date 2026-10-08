using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

// The Checklist of CITES Species (checklist.cites.org), compiled by UNEP-WCMC for the CITES
// Secretariat: every taxon in the CITES Appendices, with its current listing. The Checklist's web app
// (checklist.cites.org/js/app.js) reads one JSON endpoint of the Species+ checklist API, which
// answers without a token. It is not documented; the parameters are the ones the app sends:
//   GET https://www.speciesplus.net/checklist/taxon_concepts?output_layout=alphabetical
//       &level_of_listing=0&show_synonyms=1&show_author=1&show_english=0&show_spanish=0
//       &show_french=0&locale=en&page=<from 1>&per_page=1000
// A page is [{"result_cnt", "total_cnt", "animalia": [...], "plantae": [...]}]: animals first, then
// plants, each in name order. Each row is one accepted taxon concept (43,310 on 2026-10-08:
// species, subspecies, varieties, and the genera, families and orders that are listed or hold
// listed taxa), with its synonyms and author ("synonyms_with_authors") and "current_additions", the
// current listings that apply to it: its own, and those it inherits from a higher taxon (auto_note
// "FAMILY listing Trochilidae spp."). A species gets its family's listing even when only the family
// is named in the Appendices.
//
// Terms (checklist.cites.org and speciesplus.net terms of use): no commercial use; the data may be
// published online when it cannot be downloaded, with the citation below, the date of download and
// a link to the source.

namespace BeastieBot3.StatusLists;

/// One accepted taxon concept of the CITES Checklist. CurrentListing: Species+'s summary of the
/// appendices of the taxon and its descendants ("I", "I/II", "II/NC", "NC"). Listings: the current
/// listings that apply to the taxon itself.
internal sealed record CitesTaxon(
    long TaxonConceptId,
    string FullName,
    string? AuthorYear,
    string Rank,
    bool CitesAccepted,
    string? Kingdom,
    string? Phylum,
    string? TaxClass,
    string? TaxOrder,
    string? Family,
    string? Genus,
    string? CurrentListing,
    IReadOnlyList<CitesListing> Listings,
    IReadOnlyList<CitesSynonym> Synonyms);

/// One current listing of a taxon. Notes are HTML as Species+ gives them. Inherited*: set for a
/// listing the taxon inherits from a higher taxon (InheritedName is never null then); InheritedFromId
/// is that taxon's id when the download has exactly one taxon of that name and rank.
internal sealed record CitesListing(
    long ListingChangeId,
    string Appendix,
    string? PartyIsoCode,
    string? PartyName,
    string? EffectiveOn,
    string? ShortNote,
    string? FullNote,
    string? AnnotationSymbol,
    string? AnnotationNote,
    string? InheritedRank,
    string? InheritedName,
    long? InheritedFromId,
    string? InheritedShortNote,
    string? InheritedFullNote,
    string? NomenclatureNote);

/// A synonym as Species+ gives it ("Ornismya abeillei Lesson & DeLattre, 1839"), and the name and
/// author that CitesChecklist.SplitSynonym reads from it.
internal sealed record CitesSynonym(string NameWithAuthor, string Name, string? Author);

internal static partial class CitesChecklist {
    public const string SiteUrl = "https://checklist.cites.org/";
    public const string TaxonConceptsUrl = "https://www.speciesplus.net/checklist/taxon_concepts";
    public const int PageSize = 1000;

    public const string Title = "Checklist of CITES Species, CITES Secretariat and UNEP-WCMC";
    public const string Licence =
        "No commercial use; may be published online when it cannot be downloaded, with the citation, the date of download and a link to the source (CITES Checklist and Species+ terms of use)";

    /// The citation the Checklist's terms of use ask for, with the date of download.
    public static string Citation(DateTime accessedUtc) =>
        "UNEP-WCMC (Comps.) " + accessedUtc.Year.ToString(CultureInfo.InvariantCulture)
        + ". The Checklist of CITES Species Website. CITES Secretariat, Geneva, Switzerland. Compiled by UNEP-WCMC, Cambridge, UK. Available at: http://checklist.cites.org. [Accessed "
        + accessedUtc.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "].";

    public static string PageUrl(int page, int perPage = PageSize) =>
        TaxonConceptsUrl
        + "?output_layout=alphabetical&level_of_listing=0&show_synonyms=1&show_author=1&show_english=0&show_spanish=0&show_french=0&locale=en"
        + $"&page={page.ToString(CultureInfo.InvariantCulture)}&per_page={perPage.ToString(CultureInfo.InvariantCulture)}";

    /// The taxon's page on Species+, with its CITES listings. Species+ and the Checklist share the
    /// taxon concept ids.
    public static string SpeciesPlusUrl(long taxonConceptId) =>
        "https://speciesplus.net/species#/taxon_concepts/" + taxonConceptId.ToString(CultureInfo.InvariantCulture) + "/legal";

    /// The total the page reports and its rows (animals, then plants), as JSON elements.
    public static (long Total, IReadOnlyList<JsonElement> Rows) ReadPage(string json) {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var index = root.ValueKind == JsonValueKind.Array && root.GetArrayLength() > 0 ? root[0] : root;
        var total = index.TryGetProperty("total_cnt", out var t) && t.ValueKind == JsonValueKind.Number ? t.GetInt64() : 0;
        var rows = new List<JsonElement>();
        foreach (var kingdom in new[] { "animalia", "plantae" }) {
            if (index.TryGetProperty(kingdom, out var list) && list.ValueKind == JsonValueKind.Array) {
                rows.AddRange(list.EnumerateArray().Select(e => e.Clone()));
            }
        }
        return (total, rows);
    }

    /// The taxa of every row, with each inherited listing's InheritedFromId found among them. A row
    /// with no id or name is left out.
    public static IReadOnlyList<CitesTaxon> ReadTaxa(IEnumerable<JsonElement> rows) {
        var taxa = rows.Select(ReadTaxon).OfType<CitesTaxon>().ToList();
        var byNameAndRank = taxa.GroupBy(t => (t.FullName, t.Rank))
            .Where(g => g.Count() == 1)
            .ToDictionary(g => g.Key, g => g.First().TaxonConceptId);
        return taxa.Select(taxon => taxon.Listings.Any(l => l.InheritedRank is not null)
                ? taxon with {
                    Listings = taxon.Listings.Select(l => l.InheritedRank is { } rank && l.InheritedName is { } name
                        && byNameAndRank.TryGetValue((name, rank), out var id) ? l with { InheritedFromId = id } : l).ToList(),
                }
                : taxon)
            .ToList();
    }

    /// One row as a taxon, InheritedFromId not yet set; null for a row with no id or name.
    public static CitesTaxon? ReadTaxon(JsonElement row) {
        if (Long(row, "id") is not { } id || Text(row, "full_name") is not { } name) {
            return null;
        }
        // The paged endpoint calls the listings current_additions; the Checklist's full JSON
        // download calls them current_listing_changes.
        var listingRows = row.TryGetProperty("current_additions", out var additions) && additions.ValueKind == JsonValueKind.Array
            ? additions
            : row.TryGetProperty("current_listing_changes", out var changes) && changes.ValueKind == JsonValueKind.Array ? changes : default;
        var listings = listingRows.ValueKind == JsonValueKind.Array
            ? listingRows.EnumerateArray().Select(ReadListing).OfType<CitesListing>().ToList()
            : new List<CitesListing>();
        var synonyms = row.TryGetProperty("synonyms_with_authors", out var synonymRows) && synonymRows.ValueKind == JsonValueKind.Array
            ? synonymRows.EnumerateArray()
                .Where(s => s.ValueKind == JsonValueKind.String)
                .Select(s => s.GetString()!.Trim())
                .Where(s => s.Length > 0)
                .Distinct(StringComparer.Ordinal)
                .Select(s => SplitSynonym(s) is { } split ? new CitesSynonym(s, split.Name, split.Author) : null)
                .OfType<CitesSynonym>()
                .ToList()
            : new List<CitesSynonym>();
        return new CitesTaxon(id, name, Text(row, "author_year"), Text(row, "rank_name") ?? string.Empty,
            row.TryGetProperty("cites_accepted", out var accepted) && accepted.ValueKind == JsonValueKind.True,
            Text(row, "kingdom_name"), Text(row, "phylum_name"), Text(row, "class_name"), Text(row, "order_name"), Text(row, "family_name"),
            Text(row, "genus_name"), Text(row, "current_listing"), listings, synonyms);
    }

    /// One listing; null for one with no id or appendix.
    public static CitesListing? ReadListing(JsonElement row) {
        if (Long(row, "id") is not { } id || Text(row, "species_listing_name") is not { } appendix) {
            return null;
        }
        string? inheritedRank = null, inheritedName = null;
        if (Text(row, "auto_note") is { } autoNote) {
            // "FAMILY listing Trochilidae spp."; a note in any other form is kept whole as the name,
            // so the listing still reads as inherited.
            var match = AutoNote().Match(autoNote);
            (inheritedRank, inheritedName) = match.Success ? (match.Groups[1].Value, match.Groups[2].Value.Trim()) : (null, autoNote);
        }
        return new CitesListing(id, appendix, Text(row, "party_iso_code"), Text(row, "party_full_name"),
            IsoDate(Text(row, "effective_at_formatted")), Text(row, "short_note"), Text(row, "full_note"), Text(row, "hash_ann_symbol"),
            Text(row, "hash_full_note"), inheritedRank, inheritedName, null, Text(row, "inherited_short_note"), Text(row, "inherited_full_note"),
            Text(row, "nomenclature_note"));
    }

    /// "22/10/1987" as "1987-10-22"; null for anything else.
    public static string? IsoDate(string? text) =>
        DateTime.TryParseExact(text, "dd/MM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date)
            ? date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : null;

    /// A synonym as Species+ writes it, the name followed by its author ("Abronia aurita Köhler,
    /// 2008", "Trochilus tzacatl de la Llave, 1833"), split into the two. The name is the genus, then
    /// a subgenus in brackets, lower-case epithets ("d'albertisii", "nonchinensis?"), rank words and
    /// the qualifiers "aff." and "cf."; the author starts at the first other word, at a particle ("de",
    /// "van") followed by a capitalised word, or at "sensu", "auct.", "hort." and the like. Checked
    /// against the names the endpoint gives with show_author=0: all 2,272 synonyms on two pages of
    /// 1,000 taxa split the same way. Null for an empty string.
    public static (string Name, string? Author)? SplitSynonym(string text) {
        var words = text.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (words.Length == 0) {
            return null;
        }
        var at = 1;
        while (at < words.Length) {
            var word = words[at];
            var isNamePart = RankWords.Contains(word)
                || (at == 1 && Subgenus().IsMatch(word))
                || (Epithet().IsMatch(word) && !AuthorWords.Contains(word) && !StartsAuthor(words, at));
            if (!isNamePart) {
                break;
            }
            at++;
        }
        var author = string.Join(' ', words.Skip(at));
        return (string.Join(' ', words.Take(at)), author.Length == 0 ? null : author);
    }

    // A run of particles from words[at] that ends at a capitalised word or a bracket: "de la Llave".
    private static bool StartsAuthor(string[] words, int at) {
        var next = at;
        while (next < words.Length && Particles.Contains(words[next])) {
            next++;
        }
        return next > at && next < words.Length && (char.IsUpper(words[next][0]) || words[next][0] == '(');
    }

    // Rank words, and the qualifiers of informal names ("Mantella aff. baroni", "Siredon spec.? var. alba").
    private static readonly HashSet<string> RankWords = new(StringComparer.Ordinal) {
        "ssp.", "subsp.", "var.", "f.", "forma", "subvar.", "nothosp.", "nothovar.", "x", "×", "aff.", "cf.", "sp.", "spec.", "spec.?",
    };

    // Lower-case words that start an author's name, not an epithet.
    private static readonly HashSet<string> Particles = new(StringComparer.Ordinal) {
        "de", "van", "von", "der", "den", "du", "la", "le", "da", "dos", "das", "del", "des", "ter", "zu", "y",
    };

    // Lower-case words that start an author part that is not a name: "sensu Smith", "auct.", "hort. ex".
    private static readonly HashSet<string> AuthorWords = new(StringComparer.Ordinal) { "sensu", "non", "nec", "auct", "hort", "emend" };

    private static string? Text(JsonElement row, string property) =>
        row.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(value.GetString())
            ? value.GetString()!.Trim()
            : null;

    private static long? Long(JsonElement row, string property) =>
        row.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var n) ? n : null;

    [GeneratedRegex(@"^(KINGDOM|PHYLUM|CLASS|ORDER|SUBORDER|FAMILY|SUBFAMILY|TRIBE|GENUS|SPECIES|SUBSPECIES|VARIETY) listing (.+?)(?: spp\.)?$")]
    private static partial Regex AutoNote();

    // A lower-case epithet: "aurita", "novae-zelandiae", "grosmorneënsis", "d'albertisii", and one
    // with a question mark, "nonchinensis?". An author such as "d'Orbigny" has a capital letter.
    [GeneratedRegex(@"^\p{Ll}[\p{Ll}'’-]*\??$")]
    private static partial Regex Epithet();

    // A subgenus after the genus, in brackets, which Species+ sometimes writes in lower case: "(agalychnis)".
    [GeneratedRegex(@"^\([A-Za-z]+\)$")]
    private static partial Regex Subgenus();
}
