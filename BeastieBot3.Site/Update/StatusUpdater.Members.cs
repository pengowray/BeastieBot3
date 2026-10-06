
namespace BeastieBot3.Site.Update;

// The taxa a text lists (StatusUpdateResult.Members), for the comparison with their group (ListScope).
public sealed partial class StatusUpdater {
    // ---------------------------------------------------------------- members

    // The taxa named on list lines and in table rows that have no status, found whether or not the
    // options add one. Filled by FindMissing.
    private readonly List<ListMember> _bareMembers = [];

    private static char FirstNonSpace(string text, int from) {
        for (var i = from; i < text.Length && text[i] != '\n'; i++) {
            if (!char.IsWhiteSpace(text[i])) {
                return text[i];
            }
        }
        return '\0';
    }

    private static ListMember Member(StatusTaxon taxon, int line, StatusNote? howFound, string? code) =>
        new(taxon, line, howFound is { Kind: StatusNoteKind.MatchedBySynonym or StatusNoteKind.MatchedByCommonName or StatusNoteKind.MatchedByArticle, Detail: { } name }
            ? name : taxon.ScientificName, code);

    private static readonly HashSet<StatusItemKind> MemberKinds = [
        StatusItemKind.StatusTemplate, StatusItemKind.TableCell, StatusItemKind.ListLine, StatusItemKind.SpeciesTableRow,
        StatusItemKind.ListLineAdded, StatusItemKind.TableRowAdded,
    ];

    // The taxa the text lists: the items' taxa and the lines and rows with no status, one per taxon
    // and line.
    private List<ListMember> Members(WikitextScanner s, IReadOnlyList<StatusFinding> findings) {
        var members = new List<ListMember>();
        var seen = new HashSet<(long, int)>();
        foreach (var finding in findings.Where(f => MemberKinds.Contains(f.Kind) && f.Taxon is not null)) {
            if (seen.Add((finding.Taxon!.TaxonId, finding.Line))) {
                var howFound = finding.Notes.FirstOrDefault(n => n.Kind is StatusNoteKind.MatchedBySynonym or StatusNoteKind.MatchedByCommonName
                    or StatusNoteKind.MatchedByArticle);
                var code = finding.Kind is StatusItemKind.ListLineAdded or StatusItemKind.TableRowAdded ? null : EditSummary.CodeIn(finding.Before);
                // An {{IUCN status}} with ids on a "*" line (the generated lists) is a list line too;
                // a template in a table cell is on a line that starts with "|".
                var lineStart = s.LineStart(finding.Line);
                var source = finding.Kind switch {
                    StatusItemKind.ListLine or StatusItemKind.ListLineAdded => ListMemberSource.ListLine,
                    StatusItemKind.SpeciesTableRow => ListMemberSource.SpeciesTableRow,
                    StatusItemKind.TableCell or StatusItemKind.TableRowAdded => ListMemberSource.TableRow,
                    // "*" and "#" lines only: the lines new taxa can be put next to.
                    _ when lineStart < s.Text.Length && s.Text[lineStart] is '*' or '#' => ListMemberSource.ListLine,
                    _ when FirstNonSpace(s.Text, lineStart) is '|' or '!' => ListMemberSource.TableRow,
                    _ => ListMemberSource.Other,
                };
                var hasStatus = source == ListMemberSource.ListLine
                    && (finding.Kind != StatusItemKind.ListLineAdded || finding.Outcome == StatusOutcome.Updated);
                members.Add(Member(finding.Taxon, finding.Line, howFound, code) with { Source = source, HasStatusTemplate = hasStatus });
            }
        }
        foreach (var member in _bareMembers) {
            if (seen.Add((member.Taxon.TaxonId, member.Line))) {
                members.Add(member);
            }
        }
        members.Sort((a, b) => a.Line.CompareTo(b.Line));
        return members;
    }
}
