using System.Globalization;
using System.IO.Compression;
using System.Text;
using YamlDotNet.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// The citation and DOI of a Catalogue of Life release, for the site's attribution (CC BY 4.0), read
// from metadata.yaml in the release's ColDP zip (Datasets:COL_dir).
//
// metadata.yaml is large (120 MB for COL26.7 XR) because it lists every source dataset under
// `source:`; the fields used here all come before that key, so only the text before it is read.
//
// The citation is built the way ChecklistBank shows it for the dataset (APA style, checked against
// api.checklistbank.org/dataset/315834 for COL26.7 XR):
//
//   Bánki, O., Roskov, Y., ... Acero P, A., et al. (2026). Catalogue of Life (2026-07-17 XR).
//   Catalogue of Life Foundation, Amsterdam, Netherlands. https://doi.org/10.48580/dgykv
//
// Up to 20 creators are all listed, the last after "&"; with more, the first 19 and "et al.".
// When metadata.yaml has no creators but has a `citation`, that text is used instead.

namespace BeastieBot3.SiteBuild;

/// The release's alias ("COL26.7 XR"), DOI without a resolver prefix ("10.48580/dgykv") and citation.
internal sealed record ColReleaseCitationInfo(string? Alias, string? Doi, string? Citation);

internal static class ColReleaseCitation {
    private const int MaxListedCreators = 20;
    private const int CreatorsBeforeEtAl = 19;

    /// The citation of the release, from the ColDP zip in colDir whose alias is the release. When
    /// release is null, the newest zip. Null when no zip fits (warning says why).
    public static ColReleaseCitationInfo? Find(string colDir, string? release, out string? warning) {
        warning = null;
        var zips = Directory.EnumerateFiles(colDir, "*.zip")
            .Select(path => new FileInfo(path))
            .OrderByDescending(file => file.LastWriteTimeUtc)
            .ToList();
        var read = new List<ColReleaseCitationInfo>();
        foreach (var zip in zips) {
            ColReleaseCitationInfo? info;
            try {
                info = ReadFromZip(zip.FullName);
            } catch (Exception ex) when (ex is InvalidDataException or IOException) {
                // Not a readable zip: another file in the folder.
                continue;
            }
            if (info is null) {
                continue;
            }
            if (release is null || string.Equals(info.Alias, release, StringComparison.Ordinal)) {
                return info;
            }
            read.Add(info);
        }
        warning = read.Count == 0
            ? $"No ColDP zip with a metadata.yaml in {colDir}, so the site database has no Catalogue of Life citation."
            : $"No ColDP zip in {colDir} is release {release} (found {string.Join(", ", read.Select(i => i.Alias ?? "no alias"))}), so the site database has no Catalogue of Life citation.";
        return null;
    }

    /// Reads metadata.yaml from a ColDP zip, up to the `source:` list. Null when the zip has none.
    public static ColReleaseCitationInfo? ReadFromZip(string zipPath) {
        using var archive = ZipFile.OpenRead(zipPath);
        var entry = archive.Entries.FirstOrDefault(e => e.FullName.Equals("metadata.yaml", StringComparison.OrdinalIgnoreCase));
        if (entry is null) {
            return null;
        }
        using var reader = new StreamReader(entry.Open(), Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        return Parse(ReadBeforeSources(reader));
    }

    /// The text before the top-level `source:` key.
    internal static string ReadBeforeSources(TextReader reader) {
        var sb = new StringBuilder();
        while (reader.ReadLine() is { } line) {
            if (line.StartsWith("source:", StringComparison.Ordinal)) {
                break;
            }
            sb.Append(line).Append('\n');
        }
        return sb.ToString();
    }

    internal static ColReleaseCitationInfo? Parse(string yaml) {
        ColMetadata? metadata;
        try {
            metadata = new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .IgnoreUnmatchedProperties()
                .Build()
                .Deserialize<ColMetadata>(yaml);
        } catch (YamlException) {
            return null;
        }
        if (metadata is null) {
            return null;
        }
        var doi = Clean(metadata.Doi) is { } d ? Iucn.Gbif.IucnDoi.Extract(d) ?? d : null;
        return new ColReleaseCitationInfo(Clean(metadata.Alias), doi, BuildCitation(metadata, doi));
    }

    /// The citation as ChecklistBank writes it (see the file comment); metadata.yaml's own `citation`
    /// when it lists no creators.
    internal static string? BuildCitation(ColMetadata metadata, string? doi) {
        var creators = (metadata.Creator ?? new List<ColAgent>()).Select(AgentName).OfType<string>().ToList();
        if (creators.Count == 0 && Clean(metadata.Citation) is { } given) {
            return given;
        }
        var title = Clean(metadata.Title);
        if (title is null) {
            return null;
        }
        var titlePart = Clean(metadata.Version) is { } version ? $"{title} ({version})." : $"{title}.";
        var year = Clean(metadata.Issued) is { Length: >= 4 } issued && int.TryParse(issued[..4], NumberStyles.None, CultureInfo.InvariantCulture, out var y)
            ? y.ToString(CultureInfo.InvariantCulture)
            : "n.d.";

        var parts = new List<string>();
        if (creators.Count > 0) {
            parts.Add($"{CreatorList(creators)} ({year}).");
            parts.Add(titlePart);
        } else {
            parts.Add(titlePart);
            parts.Add($"({year}).");
        }
        if (Publisher(metadata.Publisher) is { } publisher) {
            parts.Add(publisher + ".");
        }
        if (doi is not null) {
            parts.Add($"https://doi.org/{doi}");
        } else if (Clean(metadata.Url) is { } url) {
            parts.Add(url);
        }
        return string.Join(" ", parts);
    }

    private static string CreatorList(IReadOnlyList<string> creators) {
        if (creators.Count > MaxListedCreators) {
            return string.Join(", ", creators.Take(CreatorsBeforeEtAl)) + ", et al.";
        }
        if (creators.Count == 1) {
            return creators[0];
        }
        return string.Join(", ", creators.Take(creators.Count - 1)) + ", & " + creators[^1];
    }

    // "Bánki, O.", "Hernández Robles, D. R."; an organisation credited without a person by its name.
    private static string? AgentName(ColAgent agent) {
        var family = Clean(agent.Family);
        var given = Clean(agent.Given);
        if (family is not null) {
            return given is null ? family : $"{family}, {Initials(given)}";
        }
        return Clean(agent.Organisation) ?? given;
    }

    // "Diana Raquel" -> "D. R.", "Jean-Pierre" -> "J.-P.", "R. Edward" -> "R. E."
    internal static string Initials(string given) =>
        string.Join(" ", given.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Select(word => string.Join("-", word.Split('-', StringSplitOptions.RemoveEmptyEntries)
                .Select(part => char.IsLetter(part[0]) ? $"{part[..CharLength(part)]}." : part))));

    // The first letter, with a following combining mark if there is one.
    private static int CharLength(string text) {
        var length = char.IsSurrogate(text[0]) && text.Length > 1 ? 2 : 1;
        while (length < text.Length && CharUnicodeInfo.GetUnicodeCategory(text[length]) == UnicodeCategory.NonSpacingMark) {
            length++;
        }
        return length;
    }

    // "Catalogue of Life Foundation, Amsterdam, Netherlands"
    private static string? Publisher(ColAgent? publisher) {
        if (publisher is null) {
            return null;
        }
        var place = Clean(publisher.Address);
        if (place is null) {
            var pieces = new[] { publisher.City, publisher.State, publisher.Country }.Select(Clean).OfType<string>().ToList();
            place = pieces.Count > 0 ? string.Join(", ", pieces) : null;
        }
        var name = Clean(publisher.Organisation) ?? AgentName(publisher);
        return (name, place) switch {
            (null, null) => null,
            (null, _) => place,
            (_, null) => name,
            _ => $"{name}, {place}",
        };
    }

    private static string? Clean(string? text) {
        if (string.IsNullOrWhiteSpace(text)) {
            return null;
        }
        return string.Join(" ", text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
    }
}

/// The fields of a ColDP metadata.yaml the citation needs.
internal sealed class ColMetadata {
    public string? Doi { get; set; }
    public string? Title { get; set; }
    public string? Alias { get; set; }
    public string? Issued { get; set; }
    public string? Version { get; set; }
    public string? Citation { get; set; }
    public string? Url { get; set; }
    public List<ColAgent>? Creator { get; set; }
    public ColAgent? Publisher { get; set; }
}

/// A person or organisation in ColDP metadata.
internal sealed class ColAgent {
    public string? Given { get; set; }
    public string? Family { get; set; }
    public string? Organisation { get; set; }
    public string? Address { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Country { get; set; }
}
