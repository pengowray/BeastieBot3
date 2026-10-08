using System.Text.RegularExpressions;

namespace BeastieBot3.Site.Update;

/// What a text lists its taxa in, so that the update page explains only the ways of adding missing
/// taxa that apply to it: list lines, headings above them, {{Species table}}s, rows of wikitables,
/// and subspecies or varieties on list lines.
public sealed partial record ListForms(bool ListLines, bool Headings, bool SpeciesTables, bool TableRows, bool InfraOnLines = false) {
    public static ListForms Of(string text, IReadOnlyList<ListMember> members) {
        var listed = members.Where(m => m.Taxon.InRelease).ToList();
        var lines = listed.Any(m => m.Source == ListMemberSource.ListLine);
        return new ListForms(lines, lines && Heading().IsMatch(text),
            listed.Any(m => m.Source == ListMemberSource.SpeciesTableRow), listed.Any(m => m.Source == ListMemberSource.TableRow),
            listed.Any(m => m.Source == ListMemberSource.ListLine && m.Taxon.Kind != Data.TaxonKinds.Species));
    }

    [GeneratedRegex(@"^={2,6}[^=\n]+={2,6}[ \t\r]*$", RegexOptions.Multiline)]
    private static partial Regex Heading();
}
