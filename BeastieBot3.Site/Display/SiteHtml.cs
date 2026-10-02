using System.Text;
using System.Text.Encodings.Web;
using System.Text.RegularExpressions;
using System.Text.Unicode;

namespace BeastieBot3.Site.Display;

/// HTML encoding for strings built outside Razor. Letters outside ASCII (Ø, é, 中) stay as they are;
/// Razor's own encoder is set up the same way in Program.cs.
public static partial class SiteHtml {
    public static readonly HtmlEncoder Encoder = HtmlEncoder.Create(UnicodeRanges.All);

    public static string Encode(string? text) => text is null ? string.Empty : Encoder.Encode(text);

    /// The text HTML-encoded, with every http or https URL in it made a link. A full stop, comma,
    /// semicolon, colon or closing bracket at the end of a URL is left outside the link, as it
    /// usually ends the sentence. Used for citations taken from source metadata.
    public static string Linkify(string? text) {
        if (string.IsNullOrEmpty(text)) {
            return string.Empty;
        }
        var sb = new StringBuilder(text.Length + 64);
        var at = 0;
        foreach (Match match in Url().Matches(text)) {
            var url = match.Value.TrimEnd('.', ',', ';', ':', ')', ']');
            sb.Append(Encode(text[at..match.Index]));
            sb.Append("<a href=\"").Append(Encode(url)).Append("\">").Append(Encode(url)).Append("</a>");
            at = match.Index + url.Length;
        }
        sb.Append(Encode(text[at..]));
        return sb.ToString();
    }

    [GeneratedRegex(@"https?://[^\s<>""]+")]
    private static partial Regex Url();
}
