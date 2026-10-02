using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml.Linq;

// Reads the dataset description (EML, eml.xml) of GBIF's IUCN checklist archive: the title, the Red
// List version, the publication date, the licence and the citation IUCN asks users of the dataset
// to give.

namespace BeastieBot3.Iucn.Gbif;

/// <param name="RedListVersion">The release the checklist was built from, e.g. "2026-1", read from "Version 2026-1" in the citation or title.</param>
/// <param name="VersionText">The text that named the version, e.g. "Version 2026-1".</param>
/// <param name="PubDate">&lt;pubDate&gt; as written, e.g. "2026-07-28".</param>
/// <param name="Citation">The recommended citation (&lt;citation&gt; in GBIF's additional metadata).</param>
/// <param name="CitationIdentifier">The citation's identifier attribute, usually the dataset DOI.</param>
internal sealed record GbifIucnDatasetInfo(
    string? Title,
    string? RedListVersion,
    string? VersionText,
    string? PubDate,
    string? LicenceText,
    string? LicenceUrl,
    string? Citation,
    string? CitationIdentifier);

internal static class GbifIucnEml {
    private static readonly Regex VersionPattern = new(
        @"\bVersion\s+(?<version>(?:19|20)\d{2}(?:-\d)?)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static GbifIucnDatasetInfo Parse(Stream emlXml) {
        XDocument document;
        try {
            document = XDocument.Load(emlXml);
        } catch (System.Xml.XmlException ex) {
            throw new InvalidDataException($"eml.xml is not valid XML: {ex.Message}", ex);
        }
        var root = document.Root ?? throw new InvalidDataException("eml.xml is empty.");
        var dataset = Child(root, "dataset");

        var title = Text(Child(dataset, "title"));
        var pubDate = Text(Child(dataset, "pubDate"));

        var rights = Child(dataset, "intellectualRights");
        var licenceText = Text(rights);
        var licenceUrl = rights?.Descendants().FirstOrDefault(e => e.Name.LocalName == "ulink")?.Attribute("url")?.Value.Trim();

        var citationElement = root.Descendants().FirstOrDefault(e => e.Name.LocalName == "citation");
        var citation = Text(citationElement);
        var citationIdentifier = citationElement?.Attribute("identifier")?.Value.Trim();

        // The version is written in the citation ("... Version 2026-1. https://www.iucnredlist.org ...");
        // the title and the abstract are fallbacks.
        Match? version = null;
        foreach (var text in new[] { citation, title, Text(Child(dataset, "abstract")) }) {
            if (text is not null && VersionPattern.Match(text) is { Success: true } match) {
                version = match;
                break;
            }
        }

        return new GbifIucnDatasetInfo(
            title,
            version?.Groups["version"].Value,
            version?.Value,
            pubDate,
            licenceText,
            string.IsNullOrEmpty(licenceUrl) ? null : licenceUrl,
            citation,
            string.IsNullOrEmpty(citationIdentifier) ? null : citationIdentifier);
    }

    private static XElement? Child(XElement? parent, string localName) =>
        parent?.Elements().FirstOrDefault(e => e.Name.LocalName == localName);

    // The element's text with runs of whitespace (including newlines inside <para>) made single spaces.
    private static string? Text(XElement? element) {
        if (element is null) {
            return null;
        }
        var text = Regex.Replace(element.Value, @"\s+", " ").Trim();
        return text.Length == 0 ? null : text;
    }
}
