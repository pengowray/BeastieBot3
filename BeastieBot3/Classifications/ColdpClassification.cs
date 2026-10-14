using System.IO.Compression;
using System.Text;

// Reads the classification in a Catalogue of Life Data Package (ColDP) as ChecklistBank exports it:
// either NameUsage.tsv (one row per name, with ID, parentID, status, rank and scientificName; a
// synonym's parentID is its accepted taxon), or Taxon.tsv (ID, parentID, nameID) with Name.tsv (ID,
// scientificName, rank) and Synonym.tsv (taxonID, nameID). Column names are matched without a "col:"
// prefix, underscores or case ("parent_id" is "parentID"), and the text NULL is an empty value.
//
// ChecklistBank's archive of ITIS (dataset 2144) starts with the header of a multipart form upload
// before the zip itself; Unwrap removes everything before the zip's first entry.

namespace BeastieBot3.Classifications;

internal static class ColdpClassification {
    public static ClassificationTree Read(string path) {
        Unwrap(path);
        using var zip = ZipFile.OpenRead(path);
        return Read(name => Entry(zip, name) is { } entry ? new StreamReader(entry.Open(), Encoding.UTF8) : null);
    }

    /// open: the reader of a file in the package, or null when the package has no such file.
    internal static ClassificationTree Read(Func<string, TextReader?> open) {
        var tree = new ClassificationTree();
        using (var usages = open("NameUsage.tsv")) {
            if (usages is not null) {
                foreach (var row in Rows(usages)) {
                    if (row.Get("id") is not { } id || row.Get("scientificname") is not { } name) {
                        continue;
                    }
                    if (IsSynonym(row.Get("status"))) {
                        if (row.Get("parentid") is { } accepted) {
                            tree.AddSynonym(name, accepted);
                        }
                    } else if (IsAccepted(row.Get("status"))) {
                        tree.AddTaxon(id, row.Get("parentid"), row.Get("rank"), name);
                    }
                }
                return tree;
            }
        }
        var names = new Dictionary<string, (string Name, string? Rank)>(StringComparer.Ordinal);
        using (var nameFile = open("Name.tsv") ?? throw new InvalidDataException("The package has neither NameUsage.tsv nor Name.tsv.")) {
            foreach (var row in Rows(nameFile)) {
                if (row.Get("id") is { } id && row.Get("scientificname") is { } name) {
                    names[id] = (name, row.Get("rank"));
                }
            }
        }
        using (var taxa = open("Taxon.tsv") ?? throw new InvalidDataException("The package has Name.tsv but no Taxon.tsv.")) {
            foreach (var row in Rows(taxa)) {
                if (row.Get("id") is { } id && row.Get("nameid") is { } nameId && names.TryGetValue(nameId, out var name)) {
                    tree.AddTaxon(id, row.Get("parentid"), name.Rank, name.Name);
                }
            }
        }
        using var synonyms = open("Synonym.tsv");
        if (synonyms is not null) {
            foreach (var row in Rows(synonyms)) {
                if (row.Get("taxonid") is { } taxonId && row.Get("nameid") is { } nameId && names.TryGetValue(nameId, out var name)) {
                    tree.AddSynonym(name.Name, taxonId);
                }
            }
        }
        return tree;
    }

    /// The package's version from metadata.yaml ("version: 2026-09-22"), or null.
    public static string? Version(string path) {
        using var zip = ZipFile.OpenRead(path);
        if (Entry(zip, "metadata.yaml") is not { } metadata) {
            return null;
        }
        using var reader = new StreamReader(metadata.Open(), Encoding.UTF8);
        for (var line = reader.ReadLine(); line is not null; line = reader.ReadLine()) {
            if (line.StartsWith("version:", StringComparison.Ordinal)) {
                return line["version:".Length..].Trim().Trim('"', '\'') is { Length: > 0 } v ? v : null;
            }
        }
        return null;
    }

    private static bool IsSynonym(string? status) => status?.Contains("synonym", StringComparison.OrdinalIgnoreCase) == true;

    // "accepted" and "provisionally accepted"; a row with no status is taken as accepted.
    private static bool IsAccepted(string? status) =>
        status is null || status.Equals("accepted", StringComparison.OrdinalIgnoreCase) || status.Equals("provisionally accepted", StringComparison.OrdinalIgnoreCase);

    private static ZipArchiveEntry? Entry(ZipArchive zip, string name) =>
        zip.Entries.FirstOrDefault(e => string.Equals(e.FullName, name, StringComparison.OrdinalIgnoreCase));

    /// Removes anything before the zip's first local file header, in place.
    internal static void Unwrap(string path) {
        var head = new byte[4096];
        int read;
        using (var file = File.OpenRead(path)) {
            read = file.Read(head, 0, head.Length);
        }
        if (read >= 2 && head[0] == (byte)'P' && head[1] == (byte)'K') {
            return;
        }
        var start = head.AsSpan(0, read).IndexOf("PK\u0003\u0004"u8);
        if (start < 0) {
            return;
        }
        var unwrapped = path + ".unwrapped";
        using (var input = File.OpenRead(path))
        using (var output = File.Create(unwrapped)) {
            input.Seek(start, SeekOrigin.Begin);
            input.CopyTo(output);
        }
        File.Move(unwrapped, path, overwrite: true);
    }

    internal sealed class Row(Dictionary<string, int> header, string[] fields) {
        public string? Get(string column) =>
            header.TryGetValue(column, out var i) && i < fields.Length && fields[i].Trim() is { Length: > 0 } value && value != "NULL" ? value : null;
    }

    internal static IEnumerable<Row> Rows(TextReader reader) {
        var header = new Dictionary<string, int>(StringComparer.Ordinal);
        var columns = (reader.ReadLine() ?? "").TrimStart('﻿').Split('\t');
        for (var i = 0; i < columns.Length; i++) {
            header.TryAdd(Key(columns[i]), i);
        }
        for (var line = reader.ReadLine(); line is not null; line = reader.ReadLine()) {
            yield return new Row(header, line.Split('\t'));
        }
    }

    // "col:parentID", "parent_id" and "parentID" are all "parentid".
    private static string Key(string column) {
        var name = column.Trim();
        var colon = name.LastIndexOf(':');
        if (colon >= 0) {
            name = name[(colon + 1)..];
        }
        return name.Replace("_", "", StringComparison.Ordinal).ToLowerInvariant();
    }
}
