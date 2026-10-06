using System.Text;
using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Pages;

// docs/public-site.md, "List lines", has the measurements behind MaxLines.

// The references after the lines of a bullet list (the list option lrefs, ListLineOptions). Each line
// cites the source it is listed from (ListSourceMerge's most preferred ticked source):
//
//   IUCN       the taxon's latest global assessment, as the species tables cite it: {{cite iucn}}, or
//              {{cite Q}} of the assessment's Wikidata item (IucnReference). A taxon with no global
//              assessment (NE) gets no reference.
//   CoL        {{Catalogue of Life |id=... |title=''Name'' Authority}}, the template English Wikipedia
//              uses for the Catalogue of Life (Module:Cite taxon). The authority is CoL's, so only for
//              a species from CoL shown under CoL's name.
//   Wikidata   {{cite Q|Q...}} of the taxon item. It renders as "Name, Wikidata Q..."; English
//              Wikipedia does not accept Wikidata as a reliable source (WP:UGC), so the page asks
//              editors to replace these.
//
// Ref names: IUCN references as the species tables name them by default ("IUCN" + English name);
// CoL "col-<id>", Wikidata "wd-Q<n>".

namespace BeastieBot3.Site.Lists;

/// One reference after a line: its ref name and the citation template (no <ref>).
public sealed record LineReference(string Name, string Citation);

/// The citation columns of a taxon's latest global assessment.
public sealed record AssessmentCitation(string? CitationJson, string? WikidataItemQid, string? WikidataItemProperties);

public static class ListReferences {
    /// The most lines a list with references may have. Wikipedia stops expanding templates past 2 MB
    /// (2,097,152 bytes) of post-expand include size. Measured with action=parse in October 2026 on
    /// genus Pristimantis (531 lines, authorities in small text, {{IUCN status}}): 1,522,686 bytes with
    /// {{cite iucn}} references, list-defined or in the line (2.9 KB a line), and 299,422 bytes with
    /// no references (0.56 KB a line). 731 lines with references would fill the page; the cap leaves
    /// about a quarter for the article's own text and references, as SpeciesTable's caps do. Lua
    /// time was 1.8 s of the 10 s limit. A Catalogue of Life or Wikidata reference is smaller.
    public const int MaxLines = 550;

    /// The reference of each line, by row id (ListTaxonRow.TaxonId; negative for a species only in
    /// CoL or Wikidata). merge is null for a list of IUCN's taxa only.
    public static IReadOnlyDictionary<long, LineReference> Build(GroupListResult list,
        IReadOnlyDictionary<long, AssessmentCitation> citations, ListSourceMergeResult? merge, ReferenceTemplate template, DateOnly today) {
        var result = new Dictionary<long, LineReference>();
        var names = new SpeciesTable.RefNamer(TableRefNames.CommonName);
        foreach (var line in list.Blocks.OfType<LineBlock>()) {
            var taxon = line.Taxon;
            if (result.ContainsKey(taxon.TaxonId)) {
                continue;
            }
            var entry = merge?.Entries.GetValueOrDefault(taxon.TaxonId);
            var source = entry?.Source ?? ListSource.Iucn;
            LineReference? reference = source switch {
                ListSource.Col when entry?.ColId is { } colId =>
                    new LineReference("col-" + colId, CatalogueOfLife(colId, taxon.ScientificName, taxon.TaxonId < 0 ? taxon.Authority : null)),
                ListSource.Wikidata when entry?.WikidataQid is { } qid =>
                    new LineReference("wd-" + qid, WikidataCitation.CiteQ(qid)),
                ListSource.Iucn when taxon.Category is not null && citations.GetValueOrDefault(taxon.TaxonId) is { } citation
                    && Iucn(citation, template, today) is { } text =>
                    new LineReference(names.Next(taxon), text),
                _ => null,
            };
            if (reference is not null) {
                result[taxon.TaxonId] = reference;
            }
        }
        return result;
    }

    /// {{Catalogue of Life |id=... |title=''Name'' Authority}}.
    public static string CatalogueOfLife(string colId, string scientificName, string? authority) {
        var title = $"''{Value(scientificName)}''";
        if (!string.IsNullOrWhiteSpace(authority)) {
            title += " " + Value(authority);
        }
        return $"{{{{Catalogue of Life |id={Value(colId)} |title={title}}}}}";
    }

    private static string? Iucn(AssessmentCitation citation, ReferenceTemplate template, DateOnly today) {
        var parts = IucnReference.ReadParts(citation.CitationJson);
        DateOnly? downloaded = parts?.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null;
        return IucnReference.Render(template, parts, citation.WikidataItemQid, citation.WikidataItemProperties,
            WikitextOptions.Default.ToCiteIucnOptions(today, downloaded) with { WrapInRef = false },
            new CiteQOptions { AccessDate = downloaded, WrapInRef = false });
    }

    // A value inside a template parameter: no pipe, no braces, one line.
    private static string Value(string text) =>
        string.Join(' ', text.Replace("{", string.Empty, StringComparison.Ordinal).Replace("}", string.Empty, StringComparison.Ordinal)
            .Replace("|", "{{!}}", StringComparison.Ordinal)
            .Split((char[])['\n', '\r', '\t', ' '], StringSplitOptions.RemoveEmptyEntries));
}

/// Writes the references into the list's wikitext: after each line that has one, and for list-defined
/// references the {{reflist|refs=}} at the end. A name used again gets <ref name="..."/>.
internal sealed class ListReferenceWriter(ListReferenceMode mode, IReadOnlyDictionary<long, LineReference>? references) {
    private readonly List<LineReference> _defined = new();
    private readonly HashSet<string> _names = new(StringComparer.Ordinal);

    public void AppendRef(StringBuilder sb, long rowId) {
        if (mode == ListReferenceMode.None || references is null || !references.TryGetValue(rowId, out var reference)) {
            return;
        }
        var first = _names.Add(reference.Name);
        if (first) {
            _defined.Add(reference);
        }
        sb.Append(mode == ListReferenceMode.Inline && first
            ? $"<ref name=\"{reference.Name}\">{reference.Citation}</ref>"
            : $"<ref name=\"{reference.Name}\"/>");
    }

    public void AppendRefList(StringBuilder sb) {
        if (mode != ListReferenceMode.ListDefined || _defined.Count == 0) {
            return;
        }
        sb.Append("\n{{reflist|refs=\n");
        foreach (var reference in _defined) {
            sb.Append("<ref name=\"").Append(reference.Name).Append("\">").Append(reference.Citation).Append("</ref>\n");
        }
        sb.Append("}}\n");
    }
}
