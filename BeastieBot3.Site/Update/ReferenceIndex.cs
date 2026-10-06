using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Update;

/// The named references a text defines (<ref name="X">...</ref>) and the IUCN assessments their
/// citations name ("T44853A22072238" in an article number or DOI, or a Red List address), read once
/// per text: for finding an item's taxon by the reference on its status (StatusTaxonResolver), and for
/// using a reference again instead of adding another one for the same assessment.
public sealed partial class ReferenceIndex {
    private readonly Dictionary<string, HashSet<long>> _taxaByName = new(StringComparer.Ordinal);
    private readonly Dictionary<long, string> _nameByTaxon = [];
    private readonly Dictionary<long, string> _nameByAssessment = [];

    public static readonly ReferenceIndex Empty = new();

    private ReferenceIndex() { }

    public static ReferenceIndex Read(WikitextScanner s) {
        var index = new ReferenceIndex();
        foreach (Match m in Definition().Matches(s.Masked)) {
            var name = m.Groups["name"].Value.Trim();
            foreach (Match id in StatusTaxonResolver.AssessmentInText().Matches(m.Groups["body"].Value)) {
                if (long.TryParse(id.Groups["t"].Value, out var taxonId)) {
                    if (!index._taxaByName.TryGetValue(name, out var ids)) {
                        index._taxaByName[name] = ids = [];
                    }
                    ids.Add(taxonId);
                    index._nameByTaxon.TryAdd(taxonId, name);
                }
                if (long.TryParse(id.Groups["a"].Value, out var assessmentId)) {
                    index._nameByAssessment.TryAdd(assessmentId, name);
                }
            }
        }
        return index;
    }

    /// The taxon ids the citations of the reference with this name have; null when it has none.
    public IReadOnlySet<long>? TaxaOf(string name) => _taxaByName.GetValueOrDefault(name);

    /// The first reference whose citation names this taxon, or null.
    public string? NameForTaxon(long taxonId) => _nameByTaxon.GetValueOrDefault(taxonId);

    /// The first reference whose citation names this assessment, or null.
    public string? NameForAssessment(long assessmentId) => _nameByAssessment.GetValueOrDefault(assessmentId);

    [GeneratedRegex(@"<ref\s+name\s*=\s*[""']?(?<name>[^""'/>]+?)[""']?\s*>(?<body>.*?)</ref\s*>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Definition();
}
