using System.Text;
using System.Text.RegularExpressions;

// The parts `wikipedia report-species-lists` writes its report with (headings, lists, tables, links),
// as Markdown or as wikitext for a user page on English Wikipedia.

namespace BeastieBot3.Wikipedia;

internal abstract class ReportDocument {
    protected readonly StringBuilder Sb = new();

    public abstract void Heading(int level, string text);
    public abstract void Bullets(IReadOnlyList<string> items);

    /// numeric: per column, whether its cells are numbers (right-aligned).
    public abstract void Table(IReadOnlyList<string> headers, IReadOnlyList<bool> numeric, IEnumerable<IReadOnlyList<string>> rows);

    /// A link to the English Wikipedia article with this title.
    public abstract string Link(string title);

    /// Text that may name a template ("{{IUCN status}}").
    public virtual string Text(string text) => text;

    public void Paragraph(string text) {
        Sb.AppendLine(Text(text));
        Sb.AppendLine();
    }

    public override string ToString() => Sb.ToString();
}

internal sealed class MarkdownReportDocument : ReportDocument {
    public override void Heading(int level, string text) {
        Sb.AppendLine($"{new string('#', level)} {text}");
        Sb.AppendLine();
    }

    public override void Bullets(IReadOnlyList<string> items) {
        foreach (var item in items) {
            Sb.AppendLine("- " + Text(item));
        }
        Sb.AppendLine();
    }

    public override void Table(IReadOnlyList<string> headers, IReadOnlyList<bool> numeric, IEnumerable<IReadOnlyList<string>> rows) {
        Sb.AppendLine("| " + string.Join(" | ", headers) + " |");
        Sb.AppendLine("|" + string.Join("|", numeric.Select(n => n ? "---:" : "---")) + "|");
        foreach (var row in rows) {
            Sb.AppendLine("| " + string.Join(" | ", row) + " |");
        }
        Sb.AppendLine();
    }

    public override string Link(string title) =>
        $"[{title.Replace("|", "\\|").Replace("[", "\\[").Replace("]", "\\]")}]({SpeciesListReport.Url(title)})";
}

/// Wikitext for a user page on English Wikipedia: links are [[Title]], template names are
/// {{tl|Name}} (so they show as names, not as templates), and the tables are sortable wikitables.
internal sealed partial class WikitextReportDocument : ReportDocument {
    public override void Heading(int level, string text) {
        // The page's own title is its top heading, so level 1 is a bold line and sections are "==".
        Sb.AppendLine(level == 1 ? $"'''{Text(text)}'''" : $"{new string('=', level)} {Text(text)} {new string('=', level)}");
        Sb.AppendLine();
    }

    public override void Bullets(IReadOnlyList<string> items) {
        foreach (var item in items) {
            Sb.AppendLine("* " + Text(item));
        }
        Sb.AppendLine();
    }

    public override void Table(IReadOnlyList<string> headers, IReadOnlyList<bool> numeric, IEnumerable<IReadOnlyList<string>> rows) {
        Sb.AppendLine("{| class=\"wikitable sortable\"");
        Sb.AppendLine("! " + string.Join(" !! ", headers));
        foreach (var row in rows) {
            Sb.AppendLine("|-");
            Sb.AppendLine("| " + string.Join(" || ", row.Select((cell, i) => numeric[i] && cell.Length > 0 ? $"style=\"text-align:right\" | {cell}" : cell)));
        }
        Sb.AppendLine("|}");
        Sb.AppendLine();
    }

    public override string Link(string title) => $"[[{title}]]";

    public override string Text(string text) => TemplateName().Replace(text, m => $"{{{{tl|{m.Groups[1].Value}}}}}");

    [GeneratedRegex(@"\{\{([^{}|]+)\}\}")]
    private static partial Regex TemplateName();
}
