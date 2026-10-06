using System.Globalization;
using System.Text;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Pages;
using BeastieBot3.Site.Update;

// The group page's second list type: the {{Species table}} tables of English Wikipedia's featured
// species lists ("List of vespertilionines", "List of felids"), one table per genus:
//
//   {{Species table |no-note=y |genus=[[Afronycteris]] |authority-name= |authority-year= |species-count=two}}
//   {{Species table/row
//   |name=[[Banana serotine]] |binomial=A. nanus
//   |image= |image-alt=
//   |authority-name=W.C.H. Peters |authority-year=1851 |authority-not-original=yes
//   |range= |range-image=
//   |size= |habitat= |diet=
//   |iucn-status=LC |population=Unknown
//   |direction={{population change unknown}}<ref name="IUCNBananaserotine"/>
//   }}
//   {{Species table/end}}
//
// The site database holds no range, size, habitat, diet or image (the IUCN terms keep habitat and
// range data out of it), so those parameters are written blank for the editor to fill in, or the
// "Size and ecology" column is switched off (no-ecology, no-diet). Template:Species table/row
// writes {{{name}}}, {{{binomial}}}, {{{range}}}, {{{size}}}, {{{habitat}}}, {{{diet}}} and the
// authority without a default, so a parameter left out shows as "{{{range}}}"; each one is always
// written, blank if need be. no-ecology must be on the table and on every row.
//
// The genus has no authority in the site database, so the table's authority is blank (the template
// then leaves it out of the caption). species-count is the number of species in the genus in the
// Red List version, not the number of rows: the caption says how many species the genus has.
//
// The rows' {{IUCN status|option=23|CODE}} has no taxon id, so the reference after the population
// trend is the row's only link to the assessment.

namespace BeastieBot3.Site.Lists;

public enum ListType { Bullets, Tables }

public enum TableReferences {
    None,
    /// <ref name="..."/> in the row, the citations in a {{reflist|refs=}} at the end.
    ListDefined,
    /// <ref name="...">citation</ref> in the row.
    Inline,
}

public enum TableRefNames {
    /// "IUCN" and the English name without spaces or punctuation ("IUCNHellersserotine"), as the
    /// featured lists name them; the scientific name when there is no English name.
    CommonName,
    /// "iucn-44923".
    TaxonId,
    /// "Afronycteris nanus".
    ScientificName,
}

public enum TableColumns {
    /// Blank image, range, size, habitat and diet parameters.
    All,
    /// No diet: no-diet=yes.
    NoDiet,
    /// No "Size and ecology" column: no-ecology=yes.
    NoEcology,
}

public sealed record SpeciesTableOptions {
    public ListType Type { get; init; } = ListType.Bullets;
    public TableReferences References { get; init; } = TableReferences.ListDefined;
    public TableRefNames RefNames { get; init; } = TableRefNames.CommonName;
    public ReferenceTemplate Template { get; init; } = ReferenceTemplate.CiteIucn;
    public TableColumns Columns { get; init; } = TableColumns.All;
    /// The {{IUCN statuses}} box of category counts at the top.
    public bool Summary { get; init; } = true;

    public bool IsTable => Type == ListType.Tables;
}

/// What a table row needs beyond a list line, from the taxon and its latest global assessment.
public sealed record TableTaxonExtra(
    long TaxonId,
    string? Authority,
    string? PopulationTrend,
    string? PopulationSize,
    string? CitationJson,
    string? WikidataItemQid,
    string? WikidataItemProperties);

public abstract record TableListItem;

public sealed record TableHeadingItem(HeadingBlock Heading) : TableListItem;

public sealed record TableGroupNameItem(GroupRow Group) : TableListItem;

/// One {{Species table}}: a genus and its rows. GenusLink is the wikitext of |genus=.
public sealed record GenusTable(string GenusName, string GenusLink, int SpeciesCount, IReadOnlyList<SpeciesTableRow> Rows) : TableListItem;

/// One {{Species table/row}}, with every value already as wikitext. Reference is the citation
/// template (no <ref>); null when the row has none (NE, no citation, or references off).
public sealed record SpeciesTableRow(
    ListTaxonRow Taxon,
    string Name,
    string Binomial,
    string AuthorityName,
    string AuthorityYear,
    bool AuthorityNotOriginal,
    string StatusCode,
    string Population,
    string Direction,
    string? RefName,
    string? Reference);

public sealed record SpeciesTableResult(IReadOnlyList<TableListItem> Items, IReadOnlyList<string> SkippedRanks) {
    public IEnumerable<SpeciesTableRow> Rows => Items.OfType<GenusTable>().SelectMany(t => t.Rows);
    public int RowCount => Items.OfType<GenusTable>().Sum(t => t.Rows.Count);
}

public static class SpeciesTable {
    /// The most rows a table list may have. Wikipedia stops expanding templates on a page past 2 MB
    /// (2,097,152 bytes) of post-expand include size. Measured with action=parse in October 2026 on
    /// subfamily Vespertilioninae (281 rows): 3.8 KB a row with list-defined {{cite iucn}}
    /// references (1,078,884 bytes), 3.8 KB with {{cite Q}} references in the rows, and 1.3 KB a
    /// row with no references and no "Size and ecology" column (360,827 bytes); about 1.9 KB with
    /// that column (family Felidae). The caps leave about a quarter of the page for the article's
    /// own text and references: 546 and about 1,100 rows would fill it. Lua time was 0.9 s for 281
    /// references, far from its 10 s limit.
    public const int MaxRowsWithReferences = 400;
    public const int MaxRowsWithoutReferences = 1000;

    /// The mark after an extinct species' name.
    public const string Dagger = "{{dagger|alt=Extinct}}";

    public static int MaxRows(SpeciesTableOptions options) =>
        options.References == TableReferences.None ? MaxRowsWithoutReferences : MaxRowsWithReferences;

    /// The bullet-list options a table list is built with: species only, no status sections, and
    /// no genus heading (each genus is a table).
    public static GroupListOptions ListOptions(GroupListOptions options) => options with {
        HeadingRanks = options.HeadingRanks.Where(r => r != "genus").ToList(),
        ByStatus = false,
        Infra = InfraMode.None,
        Subpopulations = false,
        StatusTemplate = false,
        Style = SpeciesListStyle.CommonNameOnly,
        Sort = options.Sort == ListSort.FirstName ? ListSort.CommonName : options.Sort,
    };

    /// The tables of a list built by GroupList.Build with ListOptions: the headings and "Members
    /// of" lines as they are, and each run of lines split into one table per genus, genera in
    /// tree order, rows in the list's order.
    public static SpeciesTableResult Build(GroupListResult list, IReadOnlyDictionary<int, GroupRow> groups,
        IReadOnlyDictionary<long, TableTaxonExtra> extras, SpeciesTableOptions options) {
        var items = new List<TableListItem>();
        var run = new List<LineBlock>();
        var names = new RefNamer(options.RefNames);

        void FlushRun() {
            if (run.Count == 0) {
                return;
            }
            var byGenus = run
                .GroupBy(l => GenusOf(l.Taxon, groups)?.NodeId ?? -1)
                .Select(g => (Genus: GenusOf(g.First().Taxon, groups), Lines: g.ToList()))
                .OrderBy(g => g.Genus?.FirstPos ?? g.Lines[0].Taxon.TreePos);
            foreach (var (genus, lines) in byGenus) {
                var name = genus?.Name ?? lines[0].Taxon.Genus ?? string.Empty;
                var rows = lines.Select(l => Row(l.Taxon, extras.GetValueOrDefault(l.Taxon.TaxonId), options, names)).ToList();
                items.Add(new GenusTable(name, GenusLink(genus, name), genus?.SpeciesCount ?? rows.Count, rows));
            }
            run.Clear();
        }

        foreach (var block in list.Blocks) {
            switch (block) {
                case LineBlock line:
                    run.Add(line);
                    break;
                case HeadingBlock heading:
                    FlushRun();
                    items.Add(new TableHeadingItem(heading));
                    break;
                case GroupNameBlock name:
                    FlushRun();
                    items.Add(new TableGroupNameItem(name.Group));
                    break;
            }
        }
        FlushRun();
        return new SpeciesTableResult(items, list.SkippedRanks);
    }

    internal static SpeciesTableRow Row(ListTaxonRow taxon, TableTaxonExtra? extra, SpeciesTableOptions options, RefNamer names) {
        var entry = GroupList.Entry(taxon);
        var code = GroupList.StatusCode(taxon);
        var hasCommon = !string.IsNullOrWhiteSpace(taxon.CommonNameEn);
        var abbreviated = Abbreviated(taxon);
        string name, binomial;
        if (hasCommon) {
            name = SpeciesListLine.NameFragment(entry, new SpeciesListLineOptions { Style = SpeciesListStyle.CommonNameOnly });
            binomial = Value(abbreviated);
        } else {
            // No English name: the name cell stays blank and the scientific name is the link.
            var target = taxon.ListArticleTitle ?? taxon.ScientificName;
            name = string.Empty;
            binomial = $"[[{Value(target)}|{Value(abbreviated)}]]";
        }
        // The featured lists mark an extinct species with a dagger after its name.
        if (code == "EX" && hasCommon) {
            name += Dagger;
        } else if (code == "EX") {
            binomial += Dagger;
        }
        var (authorName, authorYear, notOriginal) = SplitAuthority(extra?.Authority);
        var assessed = taxon.Category is not null;
        string? reference = null;
        string? refName = null;
        if (assessed && options.References != TableReferences.None && extra is not null) {
            reference = Reference(taxon, extra, options.Template);
            if (reference is not null) {
                refName = names.Next(taxon);
            }
        }
        return new SpeciesTableRow(taxon, name, binomial, Value(authorName), authorYear, notOriginal, code,
            assessed ? PopulationValues.Display(extra?.PopulationSize) : "Unknown",
            PopulationTrendTemplate.For(assessed ? extra?.PopulationTrend : null),
            refName, reference);
    }

    // The citation of the taxon's latest global assessment, without <ref>.
    private static string? Reference(ListTaxonRow taxon, TableTaxonExtra extra, ReferenceTemplate template) {
        var parts = IucnReference.ReadParts(extra.CitationJson);
        DateOnly? downloaded = parts?.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null;
        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        return IucnReference.Render(template, parts, extra.WikidataItemQid, extra.WikidataItemProperties,
            WikitextOptions.Default.ToCiteIucnOptions(today, downloaded) with { WrapInRef = false },
            new CiteQOptions { AccessDate = downloaded, WrapInRef = false });
    }

    /// "A. nanus" for Afronycteris nanus: the table's caption names the genus.
    public static string Abbreviated(ListTaxonRow taxon) =>
        taxon.Genus is { Length: > 0 } genus && taxon.SpeciesEpithet is { Length: > 0 } epithet
            ? $"{genus[0]}. {epithet}"
            : taxon.ScientificName;

    /// IUCN's authority as the table writes it: "(W.C.H. Peters, 1851)" is name "W.C.H. Peters",
    /// year "1851", in brackets because the species was first described in another genus. An
    /// authority with no year after its last comma ("L.") is all name.
    public static (string Name, string Year, bool NotOriginal) SplitAuthority(string? authority) {
        var text = (authority ?? string.Empty).Trim();
        var notOriginal = false;
        if (text.Length > 1 && text[0] == '(' && text[^1] == ')') {
            text = text[1..^1].Trim();
            notOriginal = true;
        }
        var comma = text.LastIndexOf(',');
        if (comma > 0) {
            var year = text[(comma + 1)..].Trim();
            if (year.Length == 4 && year.All(char.IsAsciiDigit)) {
                return (text[..comma].Trim(), year, notOriginal);
            }
        }
        return (text, string.Empty, notOriginal);
    }

    private static string GenusLink(GroupRow? genus, string name) =>
        genus?.EnwikiTitle is { } title && !string.Equals(title, name, StringComparison.Ordinal)
            ? $"[[{title}|{name}]]"
            : $"[[{name}]]";

    private static GroupRow? GenusOf(ListTaxonRow taxon, IReadOnlyDictionary<int, GroupRow> groups) {
        for (var at = groups.GetValueOrDefault(taxon.NodeId); at is not null;
             at = at.ParentNodeId is { } parent ? groups.GetValueOrDefault(parent) : null) {
            if (at.IsGenus) {
                return at;
            }
        }
        return null;
    }

    // ------------------------------------------------------------ ref names

    internal sealed class RefNamer(TableRefNames style) {
        private readonly HashSet<string> _used = new(StringComparer.Ordinal);

        public string Next(ListTaxonRow taxon) {
            var name = RefName(taxon, style);
            if (!_used.Add(name)) {
                name = $"{name}-{taxon.TaxonId.ToString(CultureInfo.InvariantCulture)}";
                _used.Add(name);
            }
            return name;
        }
    }

    /// The ref name of a taxon's reference in this style, before two taxa with one name are told apart.
    public static string RefName(ListTaxonRow taxon, TableRefNames style) => style switch {
        TableRefNames.TaxonId => "iucn-" + taxon.TaxonId.ToString(CultureInfo.InvariantCulture),
        TableRefNames.ScientificName => SafeRefName(taxon.ScientificName),
        _ => "IUCN" + new string((taxon.CommonNameEn ?? taxon.ScientificName).Where(c => char.IsLetterOrDigit(c) || c == '-').ToArray()),
    };

    // Quotes and angle brackets would end the name attribute; "/" would make <ref name=x/>.
    private static string SafeRefName(string name) =>
        string.Join(' ', new string(name.Where(c => c is not ('"' or '\'' or '<' or '>' or '/')).ToArray())
            .Split(' ', StringSplitOptions.RemoveEmptyEntries));

    // A value inside a template parameter: no pipe, no braces, one line.
    private static string Value(string text) =>
        string.Join(' ', text.Replace("{", string.Empty, StringComparison.Ordinal).Replace("}", string.Empty, StringComparison.Ordinal)
            .Replace("|", "{{!}}", StringComparison.Ordinal)
            .Split((char[])['\n', '\r', '\t', ' '], StringSplitOptions.RemoveEmptyEntries));

    // ------------------------------------------------------------ wikitext

    /// "three" for 3, as the featured lists write species-count; numerals from 100.
    public static string CountWords(int n) {
        string[] ones = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven",
            "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"];
        string[] tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"];
        if (n is >= 0 and < 20) {
            return ones[n];
        }
        if (n is >= 20 and < 100) {
            return n % 10 == 0 ? tens[n / 10] : $"{tens[n / 10]}-{ones[n % 10]}";
        }
        return n.ToString("N0", CultureInfo.InvariantCulture);
    }

    /// The {{IUCN statuses}} counts of the rows: ex, ew, cr, en, vu, nt, lc, dd, ne. LR/nt and
    /// LR/cd count as nt and LR/lc as lc, as in the bullet lists' sections.
    public static IReadOnlyList<(string Key, int Count)> StatusCounts(SpeciesTableResult result) {
        var counts = StatusSection.All.ToDictionary(s => s.Key, _ => 0);
        foreach (var row in result.Rows) {
            if (StatusSection.For(row.StatusCode) is { } section) {
                counts[section.Key]++;
            }
        }
        return StatusSection.All.Select(s => (s.Key.ToLowerInvariant(), counts[s.Key])).ToList();
    }

    public static string ToWikitext(SpeciesTableResult result, SpeciesTableOptions options) {
        var sb = new StringBuilder();
        if (options.Summary) {
            sb.Append("{{IUCN statuses");
            foreach (var (key, count) in StatusCounts(result)) {
                sb.Append('|').Append(key).Append('=').Append(count.ToString(CultureInfo.InvariantCulture));
            }
            sb.Append("}}\n\n");
        }
        var noEcology = options.Columns == TableColumns.NoEcology;
        var previousWasTable = false;
        foreach (var item in result.Items) {
            switch (item) {
                case TableHeadingItem { Heading: var heading }:
                    if (previousWasTable) {
                        sb.Append('\n');
                    }
                    var marks = new string('=', heading.Level);
                    sb.Append(marks).Append(' ').Append(heading.Text).Append(' ').Append(marks).Append('\n');
                    previousWasTable = false;
                    break;
                case TableGroupNameItem { Group: var group }:
                    sb.Append(GroupList.GroupNameSentence(group)).Append('\n');
                    previousWasTable = false;
                    break;
                case GenusTable table:
                    sb.Append("{{Species table |no-note=y");
                    if (noEcology) {
                        sb.Append(" |no-ecology=yes");
                    }
                    sb.Append(" |genus=").Append(table.GenusLink)
                        .Append(" |authority-name= |authority-year= |species-count=").Append(CountWords(table.SpeciesCount)).Append("}}\n");
                    foreach (var row in table.Rows) {
                        AppendRow(sb, row, options);
                    }
                    sb.Append("{{Species table/end}}\n");
                    previousWasTable = true;
                    break;
            }
        }
        if (options.References == TableReferences.ListDefined) {
            var refs = result.Rows.Where(r => r.Reference is not null).ToList();
            if (refs.Count > 0) {
                sb.Append("\n{{reflist|refs=\n");
                foreach (var row in refs) {
                    sb.Append("<ref name=\"").Append(row.RefName).Append("\">").Append(row.Reference).Append("</ref>\n");
                }
                sb.Append("}}\n");
            }
        }
        return sb.ToString().TrimEnd('\n');
    }

    private static void AppendRow(StringBuilder sb, SpeciesTableRow row, SpeciesTableOptions options) {
        sb.Append("{{Species table/row");
        if (options.Columns == TableColumns.NoEcology) {
            sb.Append(" |no-ecology=yes");
        }
        sb.Append("\n|name=").Append(row.Name).Append(" |binomial=").Append(row.Binomial).Append('\n');
        sb.Append("|image= |image-alt=\n");
        sb.Append("|authority-name=").Append(row.AuthorityName).Append(" |authority-year=").Append(row.AuthorityYear);
        if (row.AuthorityNotOriginal) {
            sb.Append(" |authority-not-original=yes");
        }
        sb.Append('\n');
        sb.Append("|range= |range-image=\n");
        switch (options.Columns) {
            case TableColumns.All:
                sb.Append("|size= |habitat= |diet=\n");
                break;
            case TableColumns.NoDiet:
                sb.Append("|size= |habitat=\n|no-diet=yes\n");
                break;
        }
        sb.Append("|iucn-status=").Append(row.StatusCode).Append(" |population=").Append(row.Population).Append('\n');
        sb.Append("|direction=").Append(row.Direction);
        if (row.Reference is not null) {
            sb.Append(options.References == TableReferences.Inline
                ? $"<ref name=\"{row.RefName}\">{row.Reference}</ref>"
                : $"<ref name=\"{row.RefName}\"/>");
        }
        sb.Append("\n}}\n");
    }
}
