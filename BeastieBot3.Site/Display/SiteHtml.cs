using System.Text.Encodings.Web;
using System.Text.Unicode;

namespace BeastieBot3.Site.Display;

/// HTML encoding for strings built outside Razor. Letters outside ASCII (Ø, é, 中) stay as they are;
/// Razor's own encoder is set up the same way in Program.cs.
public static class SiteHtml {
    public static readonly HtmlEncoder Encoder = HtmlEncoder.Create(UnicodeRanges.All);

    public static string Encode(string? text) => text is null ? string.Empty : Encoder.Encode(text);
}
