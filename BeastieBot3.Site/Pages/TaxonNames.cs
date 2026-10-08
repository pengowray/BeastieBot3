using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Display;

namespace BeastieBot3.Site.Pages;

public sealed record EnglishCommonName(string Name, IReadOnlyList<string> Sources, bool IsIucnMain);

/// A synonym with the authority shown beside it (the first source's that gives one) and its sources.
/// A source whose authority differs from the one shown has it in OtherAuthority.
public sealed record SynonymSource(string Label, string? OtherAuthority);

public sealed record SynonymRow(string Name, string? Authority, IReadOnlyList<SynonymSource> Sources);

/// A common name in a language other than English, in the form most of its sources give, with the
/// labels of its sources in OtherLanguageSourceOrder.
public sealed record OtherLanguageName(string Name, IReadOnlyList<string> Sources);

/// Common names in one language, the names with most sources first. Lang: the code for the lang
/// attribute, or null.
public sealed record LanguageGroup(string Language, string? Lang, IReadOnlyList<OtherLanguageName> Names, bool NotGiven = false);

/// The names section of a taxon page: English common names with their sources, the other
/// languages' names, and synonyms with their authorities and sources.
public sealed record TaxonNames(
    IReadOnlyList<EnglishCommonName> English,
    IReadOnlyList<LanguageGroup> OtherLanguages,
    IReadOnlyList<SynonymRow> Synonyms) {
    /// commonNameEn: the taxon's English name for display, listed second after IUCN's main name.
    public static TaxonNames Build(IReadOnlyList<NameRow> names, string? commonNameEn) {
        var english = names
            .Where(n => n.NameType == NameTypes.Common && LanguageNames.IsEnglish(n.Language))
            .GroupBy(n => SiteNameKey.Fold(n.Name))
            .Select(g => {
                var rows = g.ToList();
                var shown = rows.FirstOrDefault(r => r.Source == "iucn" && r.IsPreferred) ?? rows[0];
                var sources = rows.Select(r => r.Source).Distinct().OrderBy(SourceOrder).Select(SiteText.SourceLabel).ToList();
                return new EnglishCommonName(shown.Name, sources, rows.Any(r => r.Source == "iucn" && r.IsPreferred));
            })
            .OrderByDescending(n => n.IsIucnMain)
            .ThenByDescending(n => commonNameEn is not null && SiteNameKey.Fold(n.Name) == SiteNameKey.Fold(commonNameEn))
            .ThenBy(n => n.Name, StringComparer.CurrentCultureIgnoreCase)
            .ToList();

        // Grouped by code, not by display name, so each group can carry its lang attribute.
        // Names with no language ("und" included) come last.
        var otherLanguages = names
            .Where(n => n.NameType == NameTypes.Common && !LanguageNames.IsEnglish(n.Language))
            .GroupBy(n => LanguageNames.Key(n.Language))
            .Select(g => new LanguageGroup(LanguageNames.Name(g.Key), LanguageNames.LangAttribute(g.Key), OtherLanguageNames(g),
                NotGiven: g.Key.Length == 0))
            .OrderBy(g => g.NotGiven)
            .ThenBy(g => g.Language, StringComparer.CurrentCultureIgnoreCase)
            .ToList();

        return new TaxonNames(english, otherLanguages, SynonymRows(names));
    }

    /// The names of one language, one per name with case and Unicode normalisation ignored
    /// (SiteNameKey.CaseFold; accents count), each with all the sources that give it: the names
    /// with most sources first, then by name.
    public static IReadOnlyList<OtherLanguageName> OtherLanguageNames(IEnumerable<NameRow> rows) => rows
        .GroupBy(r => SiteNameKey.CaseFold(r.Name))
        .Select(g => {
            var group = g.OrderBy(r => OtherLanguageSourceOrder(r.Source)).ThenBy(r => r.NameId).ToList();
            var sources = group.Select(r => r.Source).Distinct().ToList();
            return new OtherLanguageName(ShownForm(group), sources.Select(SiteText.SourceLabel).ToList());
        })
        .OrderByDescending(n => n.Sources.Count)
        .ThenBy(n => n.Name, StringComparer.CurrentCultureIgnoreCase)
        .ToList();

    // The spelling most of the sources give; on a tie IUCN's, else the first source's in
    // OtherLanguageSourceOrder. rows are in that order.
    private static string ShownForm(IReadOnlyList<NameRow> rows) {
        var forms = rows
            .GroupBy(r => r.Name, StringComparer.Ordinal)
            .Select(f => (Form: f.Key, Sources: f.Select(r => r.Source).Distinct().Count(), First: f.Min(r => OtherLanguageSourceOrder(r.Source))))
            .ToList();
        var most = forms.Max(f => f.Sources);
        return forms.Where(f => f.Sources == most).OrderBy(f => f.First).First().Form;
    }

    /// The order of the sources of a name in a language other than English: the English names' order
    /// (SourceOrder), so both tables list the sources alike.
    public static int OtherLanguageSourceOrder(string source) => SourceOrder(source);

    /// The synonyms, one row per name (folded), sources in SourceOrder, by name.
    public static IReadOnlyList<SynonymRow> SynonymRows(IEnumerable<NameRow> names) => names
        .Where(n => n.NameType == NameTypes.Synonym)
        .GroupBy(n => SiteNameKey.Fold(n.Name))
        .Select(g => {
            var rows = g.OrderBy(r => SourceOrder(r.Source)).ThenBy(r => r.NameId).ToList();
            var authority = rows.Select(r => r.Authority).FirstOrDefault(a => a is not null);
            var sources = rows
                .GroupBy(r => r.Source)
                .Select(s => {
                    var own = s.Select(r => r.Authority).FirstOrDefault(a => a is not null);
                    var other = own is not null && authority is not null && !SameAuthority(own, authority) ? own : null;
                    return new SynonymSource(SiteText.SourceLabel(s.Key), other);
                })
                .ToList();
            return new SynonymRow(rows[0].Name, authority, sources);
        })
        .OrderBy(s => s.Name, StringComparer.Ordinal)
        .ToList();

    // Authorities that differ only in spacing or case are the same.
    private static bool SameAuthority(string a, string b) => SiteNameKey.Fold(a) == SiteNameKey.Fold(b);

    private static int SourceOrder(string source) => source switch {
        "iucn" => 0,
        "wikidata" => 1,
        "col" => 2,
        "wikipedia" => 3,
        "wikipedia-taxobox" => 4,
        _ => 5,
    };
}
