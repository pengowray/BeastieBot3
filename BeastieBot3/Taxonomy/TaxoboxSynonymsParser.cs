using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

// Reads the synonyms listed in the "synonyms" parameter of an English Wikipedia taxobox.
//
// Editors write the list in many ways: italic names with the authority in <small> or {{small}},
// {{au}}, or plain text after the name; bullets, <br /> breaks, or nothing between entries;
// {{Species list}} / {{Taxon list}} pairs of name and authority; and any of these inside
// {{collapsible list}}, {{Plainlist}}, {{hidden begin}} and similar wrappers.
//
// The parser first rewrites the wikitext into a simple form (references, comments and links
// removed, list templates turned into bulleted lines, <small> and <br> turned into marker
// characters), then splits each line into pieces at line breaks and at italic text that starts
// with a capital letter. A piece that starts with a scientific name begins a new synonym; any
// other piece is more of the previous synonym's authority. Only names with a genus and at least
// one epithet are returned, so a genus alone, a family name or "Around 80, including:" is dropped.

namespace BeastieBot3.Taxonomy;

internal sealed record TaxoboxSynonym(string Name, string? Authority);

internal static partial class TaxoboxSynonymsParser {
    private const char SmallOpen = '\u0002';
    private const char SmallClose = '\u0003';
    private const char LineBreak = '\u0001';

    public static IReadOnlyList<TaxoboxSynonym> Parse(string? wikitext) {
        if (string.IsNullOrWhiteSpace(wikitext)) {
            return Array.Empty<TaxoboxSynonym>();
        }
        try {
            return ParseCore(wikitext);
        } catch (Exception) {
            return Array.Empty<TaxoboxSynonym>();
        }
    }

    // Cleans an authority written as wikitext, such as a taxobox's binomial_authority:
    // "([[Oldfield Thomas|Thomas]], 1904)" gives "(Thomas, 1904)". Null when nothing is left.
    public static string? CleanAuthority(string? wikitext) {
        if (string.IsNullOrWhiteSpace(wikitext)) {
            return null;
        }
        try {
            return CleanAuthorityText(Simplify(wikitext).Replace("''", ""));
        } catch (Exception) {
            return null;
        }
    }

    private static IReadOnlyList<TaxoboxSynonym> ParseCore(string wikitext) {
        var text = Simplify(wikitext);
        var results = new List<TaxoboxSynonym>();
        var indexByName = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var line in Bullet().Split(text)) {
            foreach (var (name, tail) in ParseLine(line)) {
                var authority = AuthorityFrom(tail);
                if (indexByName.TryGetValue(name, out var index)) {
                    if (results[index].Authority is null && authority is not null) {
                        results[index] = results[index] with { Authority = authority };
                    }
                    continue;
                }
                indexByName[name] = results.Count;
                results.Add(new TaxoboxSynonym(name, authority));
            }
        }
        return results;
    }

    // Rewrites wikitext into plain text with '' italics, bullets on new lines, and the marker
    // characters for <small>, </small> and <br>.
    private static string Simplify(string wikitext) {
        var text = ControlChars().Replace(wikitext, " ");
        text = Comment().Replace(text, " ");
        text = RefPair().Replace(text, " ");
        text = RefSingle().Replace(text, " ");
        text = text.Replace("'''''", "''").Replace("'''", "");
        text = ItalicTag().Replace(text, "''");
        for (var i = 0; i < 2; i++) {
            text = FileLink().Replace(text, " ");
            text = PipedLink().Replace(text, "$2");
            text = PlainLink().Replace(text, m => InterwikiPrefix().Replace(m.Groups[1].Value, ""));
        }
        text = ExternalLink().Replace(text, "$1");
        text = ExpandTemplates(text, 0);
        text = SmallOpenTag().Replace(text, SmallOpen.ToString());
        text = SmallCloseTag().Replace(text, SmallClose.ToString());
        text = BreakTag().Replace(text, LineBreak.ToString());
        text = OtherTag().Replace(text, " ");
        return WebUtility.HtmlDecode(text);
    }

    // Splits one line into (name, authority text) pairs.
    private static List<(string Name, string Tail)> ParseLine(string line) {
        var entries = new List<(string Name, string Tail)>();
        foreach (var piece in SplitPieces(line)) {
            var plain = piece.Replace("''", "").Replace(LineBreak, ' ');
            var lead = LeadingJunk().Match(plain).Length;
            // "<small>''Orania nicobarica'' Kurz</small>": the whole entry in small type.
            if (lead < plain.Length && plain[lead] == SmallOpen) {
                lead += LeadingJunk().Match(plain[(lead + 1)..]).Length + 1;
            }
            var match = ScientificName().Match(plain, lead);
            if (match.Success && match.Index == lead && !IsStopWord(match.Groups["genus"].Value)) {
                var name = Whitespace().Replace(match.Value, " ").Trim();
                entries.Add((name, plain[(match.Index + match.Length)..]));
            } else if (entries.Count > 0) {
                var last = entries[^1];
                entries[^1] = (last.Name, last.Tail + " " + plain);
            }
        }
        return entries;
    }

    // Cuts a line at <br> and before italic text that starts with a capital letter, except
    // inside <small>, where both belong to an authority.
    private static List<string> SplitPieces(string line) {
        var pieces = new List<string>();
        var start = 0;
        var smallDepth = 0;
        var inItalic = false;
        for (var i = 0; i < line.Length; i++) {
            var c = line[i];
            if (c == SmallOpen) {
                smallDepth++;
            } else if (c == SmallClose) {
                smallDepth = Math.Max(0, smallDepth - 1);
            } else if (c == LineBreak && smallDepth == 0) {
                pieces.Add(line[start..i]);
                start = i + 1;
            } else if (c == '\'' && i + 1 < line.Length && line[i + 1] == '\'') {
                if (!inItalic && smallDepth == 0 && i > start && StartsWithCapital(line, i + 2) && !AfterOpenBracket(line, i)) {
                    pieces.Add(line[start..i]);
                    start = i;
                }
                inItalic = !inItalic;
                i++;
            }
        }
        pieces.Add(line[start..]);
        return pieces;
    }

    // "''Iolaus'' (''Iolaphilus'') ''carolinae''": a subgenus in its own italics.
    private static bool AfterOpenBracket(string text, int index) {
        var j = index - 1;
        while (j >= 0 && char.IsWhiteSpace(text[j])) {
            j--;
        }
        return j >= 0 && text[j] == '(';
    }

    private static bool StartsWithCapital(string text, int index) {
        while (index < text.Length && (char.IsWhiteSpace(text[index]) || text[index] is '?' or '†' or '"' or '[')) {
            index++;
        }
        return index < text.Length && (char.IsUpper(text[index]) || text[index] == '×');
    }

    private static string? AuthorityFrom(string tail) {
        var open = tail.IndexOf(SmallOpen);
        if (open >= 0) {
            var close = tail.IndexOf(SmallClose, open + 1);
            var inner = close < 0 ? tail[(open + 1)..] : tail[(open + 1)..close];
            var fromSmall = CleanAuthorityText(inner);
            if (fromSmall is not null) {
                return fromSmall;
            }
        }
        return CleanAuthorityText(tail);
    }

    private static string? CleanAuthorityText(string text) {
        text = text.Replace(SmallOpen, ' ').Replace(SmallClose, ' ').Replace(LineBreak, ' ')
            .Replace('*', ' ').Replace('†', ' ');
        var cut = text.IndexOfAny(new[] { '·', '•' });
        if (cut >= 0) {
            text = text[..cut];
        }
        text = NomenclaturalNote().Replace(text, " ");
        text = SquareBracketNote().Replace(text, " ");
        text = LowerCaseParenthesisNote().Replace(text, " ");
        text = BrokenMarkup().Replace(text, " ");
        text = Whitespace().Replace(text, " ");
        text = AuthorityEdgeStart().Replace(text, "");
        text = AuthorityEdgeEnd().Replace(text, "");
        return text.Length > 0 && Letter().IsMatch(text) ? text : null;
    }

    // Replaces each template with its text: list templates become bulleted lines, {{small}} and
    // {{au}} become <small>, and templates with nothing to show (citations, {{hidden begin}})
    // are removed.
    private static string ExpandTemplates(string text, int depth) {
        if (depth > 20 || !text.Contains("{{", StringComparison.Ordinal)) {
            return text;
        }
        var sb = new StringBuilder(text.Length);
        var i = 0;
        while (i < text.Length) {
            var open = text.IndexOf("{{", i, StringComparison.Ordinal);
            if (open < 0) {
                sb.Append(text, i, text.Length - i);
                break;
            }
            sb.Append(text, i, open - i);
            var close = FindTemplateEnd(text, open);
            var body = close < 0 ? text[(open + 2)..] : text[(open + 2)..close];
            sb.Append(RenderTemplate(SplitArgs(body), depth));
            i = close < 0 ? text.Length : close + 2;
        }
        return sb.ToString();
    }

    private static int FindTemplateEnd(string text, int open) {
        var depth = 0;
        for (var i = open; i + 1 < text.Length; i++) {
            if (text[i] == '{' && text[i + 1] == '{') {
                depth++;
                i++;
            } else if (text[i] == '}' && text[i + 1] == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
                i++;
            }
        }
        return -1;
    }

    private static List<string> SplitArgs(string body) {
        var args = new List<string>();
        var depth = 0;
        var start = 0;
        for (var i = 0; i < body.Length; i++) {
            if (i + 1 < body.Length && (body[i] == '{' && body[i + 1] == '{' || body[i] == '[' && body[i + 1] == '[')) {
                depth++;
                i++;
            } else if (i + 1 < body.Length && (body[i] == '}' && body[i + 1] == '}' || body[i] == ']' && body[i + 1] == ']')) {
                depth = Math.Max(0, depth - 1);
                i++;
            } else if (body[i] == '|' && depth == 0) {
                args.Add(body[start..i]);
                start = i + 1;
            }
        }
        args.Add(body[start..]);
        return args;
    }

    private static string RenderTemplate(List<string> parts, int depth) {
        var name = parts[0].Trim().Replace('_', ' ').ToLowerInvariant();
        var positional = new List<string>();
        for (var i = 1; i < parts.Count; i++) {
            if (!NamedArgument().IsMatch(parts[i])) {
                positional.Add(ExpandTemplates(parts[i], depth + 1).Trim());
            }
        }
        string First() => positional.Count > 0 ? positional[0] : "";

        switch (name) {
            case "small": case "smaller": case "sm": case "smalldiv": case "au": case "aut":
                return "<small>" + First() + "</small>";
            case "nowrap": case "nobr": case "center": case "smallcaps": case "noitalic": case "nobold":
            case "ill": case "interlanguage link": case "interlanguage link multi": case "abbr": case "tooltip":
            case "auto link":
                return First();
            case "sic": case "not a typo":
                return string.Concat(positional);
            case "species list": case "specieslist": case "taxon list":
                return RenderNameAuthorityPairs(positional);
            case "collapsible list": case "collapsable list": case "collapsed list": case "plainlist":
            case "plain list": case "flatlist": case "flat list": case "unbulleted list": case "ubl": case "ubil":
            case "hlist": case "bulleted list": case "bulletedlist": case "blist": case "bullet list":
                return string.Concat(positional.ConvertAll(p => "\n* " + p)) + "\n";
            case "hidden":
                return string.Concat(positional.GetRange(Math.Min(1, positional.Count), Math.Max(0, positional.Count - 1)).ConvertAll(p => "\n* " + p)) + "\n";
            case "*": case "bull":
                return "\n* ";
            case "br":
                return "<br>";
            case "!":
                return "|";
            case "nbsp":
                return " ";
            case "ndash":
                return "–";
            case "mdash":
                return "—";
            default:
                return " ";
        }
    }

    // {{Species list|name|authority|name|authority...}} as bulleted lines.
    private static string RenderNameAuthorityPairs(List<string> positional) {
        var sb = new StringBuilder();
        for (var i = 0; i < positional.Count; i += 2) {
            var taxon = SmallTag().Replace(positional[i], " ").Replace("''", "").Trim();
            if (taxon.Length == 0) {
                continue;
            }
            var authority = i + 1 < positional.Count ? SmallTag().Replace(positional[i + 1], " ").Trim() : "";
            sb.Append("\n* ''").Append(taxon).Append("''");
            if (authority.Length > 0) {
                sb.Append(" <small>").Append(authority).Append("</small>");
            }
        }
        return sb.Append('\n').ToString();
    }

    // Words that start free text ("See text", "Many, see text", "Family level"), never a genus.
    private static readonly HashSet<string> GenusStopWords = new(StringComparer.OrdinalIgnoreCase) {
        "about", "above", "al", "also", "an", "and", "approximately", "are", "around", "as", "auct", "below",
        "by", "cf", "aff", "da", "de", "del", "den", "der", "di", "du", "emend", "et", "ex", "excluding", "family", "fide",
        "for", "formerly", "from", "genus", "in", "including", "includes", "is", "la", "le", "list", "many",
        "more", "most", "nec", "nom", "nomen", "non", "none", "not", "numerous", "of", "or", "other", "others",
        "over", "part", "partim", "pro", "possibly", "probably", "sec", "see", "sensu", "several", "some",
        "sp", "species", "spp", "subspecies", "synonym", "synonyms", "text", "the", "these", "this", "to",
        "van", "von", "was", "were", "with",
    };

    private static bool IsStopWord(string word) => GenusStopWords.Contains(word);

    // A genus, an optional (Subgenus), an epithet and up to two infraspecific epithets, each
    // optionally after a rank marker such as "subsp." or "var.". No word may be a stop word or an
    // abbreviation ending in a full stop, except that a species epithet of four letters or more
    // may end a sentence ("''Callichthys paleatus''. Jenyns, 1842.").
    private const string NotStopWord = @"(?!(?i:about|above|al|also|an|and|are|as|auct|below|by|cf|aff|da|de|del|den|der|di|du|emend|et|ex|excluding|fide|for|from|in|including|includes|is|la|le|list|nec|nom|nomen|non|not|of|or|other|others|part|partim|pro|sec|sect|see|sens|sensu|ser|sp|spec|spp|subg|subgen|subsect|subser|subtrib|trib|species|synonyms?|text|the|to|van|von|was|were|with)(?![\p{L}-]))";
    private const string Epithet = NotStopWord + @"(?:\p{Ll}[\p{Ll}-]{2,}\p{Ll}(?![\p{L}'’-])|\p{Ll}[\p{Ll}-]*\p{Ll}(?![\p{L}.'’-]))";
    private const string InfraEpithet = NotStopWord + @"\p{Ll}[\p{Ll}-]*\p{Ll}(?![\p{L}.'’-])";

    [GeneratedRegex(@"(?<genus>(?:×\s?)?\p{Lu}\p{Ll}+)(?:\s+\(\p{Lu}\p{Ll}+\))?\s+(?:(?:×\s?|x\s+))?" + Epithet
        + @"(?:\s+(?:(?:(?i:subsp|ssp|var|f|subvar|subf|nothosubsp|nothovar)\.|forma|morph)\s*)?" + InfraEpithet + @"){0,2}")]
    private static partial Regex ScientificName();

    [GeneratedRegex(@"^[\s*?†""“”„'‘’=:;,.·•\-–—]*")]
    private static partial Regex LeadingJunk();

    [GeneratedRegex(@"\n\s*\*+|\n|(?<=^|\s)\*+(?=\s*(?:''|\[|\p{Lu}|×|\?|†|\u0002))")]
    private static partial Regex Bullet();

    [GeneratedRegex(@"[\u0000-\u0008]")]
    private static partial Regex ControlChars();

    [GeneratedRegex(@"<!--.*?(?:-->|$)", RegexOptions.Singleline)]
    private static partial Regex Comment();

    [GeneratedRegex(@"<ref\b[^>/]*(?:/(?!>)[^>/]*)*>.*?(?:</ref\s*>|$)", RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex RefPair();

    [GeneratedRegex(@"<ref\b[^>]*/>", RegexOptions.IgnoreCase)]
    private static partial Regex RefSingle();

    [GeneratedRegex(@"</?i\s*>", RegexOptions.IgnoreCase)]
    private static partial Regex ItalicTag();

    [GeneratedRegex(@"\[\[(?:File|Image):[^\[\]]*\]\]", RegexOptions.IgnoreCase)]
    private static partial Regex FileLink();

    [GeneratedRegex(@"\[\[([^\[\]|]*)\|([^\[\]]*)\]\]")]
    private static partial Regex PipedLink();

    [GeneratedRegex(@"\[\[([^\[\]|]*)\]\]")]
    private static partial Regex PlainLink();

    [GeneratedRegex(@"^:?(?:[a-z\-]{2,}:)?")]
    private static partial Regex InterwikiPrefix();

    [GeneratedRegex(@"\[(?:https?:)?//[^\s\]]+\s*([^\]]*)\]")]
    private static partial Regex ExternalLink();

    [GeneratedRegex(@"<small\b[^>]*>", RegexOptions.IgnoreCase)]
    private static partial Regex SmallOpenTag();

    [GeneratedRegex(@"</small\s*>", RegexOptions.IgnoreCase)]
    private static partial Regex SmallCloseTag();

    [GeneratedRegex(@"</?small\b[^>]*>", RegexOptions.IgnoreCase)]
    private static partial Regex SmallTag();

    [GeneratedRegex(@"</?br\s*/?>", RegexOptions.IgnoreCase)]
    private static partial Regex BreakTag();

    [GeneratedRegex(@"</?[a-zA-Z][^>]*>")]
    private static partial Regex OtherTag();

    [GeneratedRegex(@"^\s*[A-Za-z][\w \-]*=")]
    private static partial Regex NamedArgument();

    [GeneratedRegex(@"\b(?:nom(?:en)?\.?\s*(?:illeg|nud|superfl|inval|rejic|cons|dub|oblit|confus|ambig|nov|prov|subnud)[a-z]*\.?|nomen\s+\p{Ll}+|orth\.\s*(?:var|err(?:or)?)\.?|orthographic(?:al)?\s+(?:variant|error)|ambiguous\s+synonym|unjustified\s+emendation|incorrect\s+(?:original\s+|subsequent\s+)?spelling|lapsus(?:\s+calami)?|not\s+validly\s+publ(?:ished|\.)?|pro\s+syn\.?)", RegexOptions.IgnoreCase)]
    private static partial Regex NomenclaturalNote();

    // "[lapsus]", "[Invalid]", "[family AGAVACEAE]"; not a year in brackets ("Godart, [1824]")
    // or an author in brackets before the year ("[Denis & Schiffermüller], 1775").
    [GeneratedRegex(@"\[(?!\d{4}\])[^\[\]]*\](?!\s*,?\s*\d{4})")]
    private static partial Regex SquareBracketNote();

    [GeneratedRegex(@"\([^()\p{Lu}\d]*\)")]
    private static partial Regex LowerCaseParenthesisNote();

    [GeneratedRegex(@"^[\s,;:.=\-–—]+")]
    private static partial Regex AuthorityEdgeStart();

    [GeneratedRegex(@"[\s,;:=\-–—(]+$")]
    private static partial Regex AuthorityEdgeEnd();

    // Left over from unbalanced markup: "Thouars]]", "(Girard, 1856)>", "Heimerl/small>".
    [GeneratedRegex(@"[<\/]small>?|\[\[|\]\]|[{}<>|]")]
    private static partial Regex BrokenMarkup();

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();

    [GeneratedRegex(@"\p{L}")]
    private static partial Regex Letter();
}
