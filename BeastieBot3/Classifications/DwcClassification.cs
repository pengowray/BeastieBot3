using System.IO.Compression;
using System.Text.Json;
using BeastieBot3.StatusLists;

// Reads the classification in a Darwin Core Archive's taxon core (DwcArchive reads meta.xml): each
// accepted row (taxonomicStatus accepted or provisionally accepted, or none, and no acceptedNameUsageID
// other than its own id) is a taxon with its parentNameUsageID; each synonym row (or WoRMS's
// "unaccepted") leads its name to its acceptedNameUsageID. Ids lose the
// prefix given (WoRMS "urn:lsid:marinespecies.org:taxname:", IPNI "urn:lsid:ipni.org:names:").
// scientificName loses a trailing scientificNameAuthorship. A species row's kingdom, phylum, class,
// order, family and genus columns (and the subclass in dynamicProperties, which Plants of the World
// Online fills) are the groups above the highest parent the archive links.

namespace BeastieBot3.Classifications;

internal static class DwcClassification {
    public static ClassificationTree Read(string path, string idPrefix) {
        using var zip = ZipFile.OpenRead(path);
        var meta = DwcArchive.ReadMeta(zip);
        return Read(DwcArchive.ReadRows(zip, meta.Core), idPrefix);
    }

    internal static ClassificationTree Read(IEnumerable<DwcRow> rows, string idPrefix) {
        var tree = new ClassificationTree();
        string? Id(string? value) => value is null ? null : value.StartsWith(idPrefix, StringComparison.Ordinal) ? value[idPrefix.Length..] : value;
        foreach (var row in rows) {
            if (Id(row["taxonID"] ?? row.Id) is not { } id || row["scientificName"] is not { } scientificName) {
                continue;
            }
            var name = WithoutAuthorship(scientificName, row["scientificNameAuthorship"]);
            var status = row["taxonomicStatus"];
            var accepted = Id(row["acceptedNameUsageID"]);
            if (!IsAccepted(status)) {
                // WoRMS's "unaccepted" and the synonyms lead to their accepted taxon; doubtful and
                // misapplied names lead nowhere.
                if (accepted is not null && accepted != id
                    && (status is null || status.Contains("synonym", StringComparison.OrdinalIgnoreCase) || status.Equals("unaccepted", StringComparison.OrdinalIgnoreCase))) {
                    tree.AddSynonym(name, accepted);
                }
                continue;
            }
            if (accepted is not null && accepted != id) {
                continue;
            }
            var rank = ClassificationTree.NormaliseRank(row["taxonRank"]);
            tree.AddTaxon(id, Id(row["parentNameUsageID"]), rank, name);
            if (rank == "species") {
                var subclass = Subclass(row["dynamicProperties"]);
                (string Rank, string? Name)[] columns = [("kingdom", row["kingdom"]), ("phylum", row["phylum"]), ("class", row["class"]),
                    ("subclass", subclass), ("order", row["order"]), ("family", row["family"]), ("genus", row["genus"])];
                tree.AddColumnGroups(id, [.. columns.Where(c => c.Name is { Length: > 0 }).Select(c => (c.Rank, c.Name!))]);
            }
        }
        return tree;
    }

    private static bool IsAccepted(string? status) =>
        status is null || status.Equals("accepted", StringComparison.OrdinalIgnoreCase) || status.Equals("provisionally accepted", StringComparison.OrdinalIgnoreCase);

    internal static string WithoutAuthorship(string name, string? authorship) {
        name = name.Trim();
        if (authorship is { Length: > 0 } a && name.Length > a.Length && name.EndsWith(a, StringComparison.Ordinal)) {
            name = name[..^a.Length].TrimEnd();
        }
        return name;
    }

    // {"subclass":"Magnoliidae", ...} in Plants of the World Online's archive.
    private static string? Subclass(string? dynamicProperties) {
        if (dynamicProperties is null || !dynamicProperties.Contains("subclass", StringComparison.Ordinal)) {
            return null;
        }
        try {
            using var json = JsonDocument.Parse(dynamicProperties);
            return json.RootElement.TryGetProperty("subclass", out var value) && value.ValueKind == JsonValueKind.String
                && value.GetString() is { Length: > 0 } subclass ? subclass : null;
        } catch (JsonException) {
            return null;
        }
    }
}
