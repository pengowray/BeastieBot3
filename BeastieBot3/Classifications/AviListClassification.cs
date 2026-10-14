using System.Text;
using System.Text.RegularExpressions;
using ExcelDataReader;

// Reads AviList (the unified global checklist of birds; avilist.org, CC BY 4.0) from its workbook:
// the first sheet lists every order, family, genus, species and subspecies in sequence, one row each
// (Taxon_rank, Scientific_name), so each species is in the order, family and genus above it. A
// species' node id is "species:<name>". Its BirdLife DataZone address ends in the BirdLife id, which
// is the IUCN taxon id of the bird (link scheme "iucn").

namespace BeastieBot3.Classifications;

internal static partial class AviListClassification {
    public const string IucnScheme = "iucn";

    public static (ClassificationTree Tree, List<ClassificationLink> Links) Read(string path) {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        using var stream = File.OpenRead(path);
        using var reader = ExcelReaderFactory.CreateOpenXmlReader(stream);
        var rows = new List<IReadOnlyDictionary<string, string>>();
        Dictionary<string, int>? columns = null;
        while (reader.Read()) {
            var cells = Enumerable.Range(0, reader.FieldCount).Select(i => Convert.ToString(reader.GetValue(i))?.Trim() ?? "").ToList();
            if (columns is null) {
                if (cells.Contains("Taxon_rank") && cells.Contains("Scientific_name")) {
                    columns = cells.Select((c, i) => (c, i)).Where(p => p.c.Length > 0).GroupBy(p => p.c).ToDictionary(g => g.Key, g => g.First().i, StringComparer.Ordinal);
                }
                continue;
            }
            rows.Add(columns.Where(c => c.Value < cells.Count && cells[c.Value].Length > 0).ToDictionary(c => c.Key, c => cells[c.Value], StringComparer.Ordinal));
        }
        if (columns is null) {
            throw new InvalidDataException("The workbook's first sheet has no heading row with Taxon_rank and Scientific_name.");
        }
        return Read(rows);
    }

    /// rows: the sheet's rows by heading, in order.
    internal static (ClassificationTree Tree, List<ClassificationLink> Links) Read(IEnumerable<IReadOnlyDictionary<string, string>> rows) {
        var tree = new ClassificationTree();
        var links = new List<ClassificationLink>();
        string? order = null, family = null, genus = null;
        foreach (var row in rows) {
            if (row.GetValueOrDefault("Scientific_name") is not { } name || row.GetValueOrDefault("Taxon_rank")?.ToLowerInvariant() is not { } rank) {
                continue;
            }
            switch (rank) {
                case "order":
                    order = "order:" + name;
                    family = genus = null;
                    tree.AddTaxon(order, null, rank, name);
                    break;
                case "family":
                    family = "family:" + name;
                    genus = null;
                    tree.AddTaxon(family, order, rank, name);
                    break;
                case "genus":
                    genus = "genus:" + name;
                    tree.AddTaxon(genus, family, rank, name);
                    break;
                case "species":
                    var id = "species:" + name;
                    tree.AddTaxon(id, genus ?? family, rank, name);
                    if (row.GetValueOrDefault("BirdLife_DataZone_URL") is { } url && TrailingNumber().Match(url) is { Success: true } m) {
                        links.Add(new ClassificationLink(IucnScheme, m.Groups[1].Value, id));
                    }
                    break;
            }
        }
        return (tree, links);
    }

    [GeneratedRegex(@"/(\d+)/?$")]
    private static partial Regex TrailingNumber();
}
