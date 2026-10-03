using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using BeastieBot3.CommonNames;

// Compares a page's stored taxobox fields (wiki_taxobox_data, written by the parser of the day the
// page was downloaded) with what the current TaxoboxParser reads from the same wikitext. Used by
// `wikipedia reparse-taxoboxes`, which saves the new fields without downloading the page again.

namespace BeastieBot3.Wikipedia;

internal enum TaxoboxReparseOutcome {
    Unchanged,
    // The page has a stored taxobox and the parser reads different fields from it.
    Changed,
    // No taxobox was stored for the page, and the parser now finds one.
    Added,
    // A taxobox was stored for the page, and the parser now finds none.
    Removed,
}

/// <summary>
/// What re-parsing one page's wikitext changes. <see cref="Columns"/> are the wiki_taxobox_data
/// columns that differ (scientific_name, rank, kingdom, ...), and <see cref="Parameters"/> the
/// template parameters in data_json that were added, removed or changed ("name", "status_ref").
/// Both are empty unless the outcome is <see cref="TaxoboxReparseOutcome.Changed"/>.
/// </summary>
internal sealed record TaxoboxReparseResult(
    TaxoboxReparseOutcome Outcome,
    IReadOnlyList<string> Columns,
    IReadOnlyList<string> Parameters) {

    public bool ParameterChanged(string name) => Parameters.Contains(name, StringComparer.OrdinalIgnoreCase);
}

/// <summary>
/// Counts of what a re-parse changed, for the summary of `wikipedia reparse-taxoboxes`. The common
/// name that `common-names aggregate` reads from the name parameter
/// (<see cref="TaxoboxCommonName.FromNameField"/>) is counted on its own: most changes to the name
/// parameter are inside a citation the aggregator removes anyway.
/// </summary>
internal sealed class TaxoboxReparseTally {
    public const int ExampleCount = 15;

    public long PagesRead { get; private set; }
    public long Unchanged { get; private set; }
    public long Changed { get; private set; }
    public long Added { get; private set; }
    public long Removed { get; private set; }
    // Of Changed: the name parameter differs.
    public long NameChanged { get; private set; }
    // Of Changed: the scientific_name column differs.
    public long ScientificNameChanged { get; private set; }
    // Of Changed: a column other than scientific_name differs (rank, genus, family, ...).
    public long OtherColumnsChanged { get; private set; }
    // Of Changed, Added and Removed: the common name read from the name parameter differs.
    public long CommonNameChanged { get; private set; }

    /// <summary>Pages that will be saved: changed, added or removed.</summary>
    public long ToSave => Changed + Added + Removed;

    /// <summary>The first pages whose common name changed: title, name before, name after.</summary>
    public List<(string Title, string? Before, string? After)> CommonNameExamples { get; } = new();

    /// <summary>The common name `common-names aggregate` reads from a taxobox; null when it has none.</summary>
    public static string? CommonName(WikiTaxoboxData? taxobox, string pageTitle) =>
        TaxoboxCommonName.FromNameField(TaxoboxReparse.Parameter(taxobox?.DataJson, "name"), pageTitle);

    public void Add(string pageTitle, TaxoboxReparseResult result, WikiTaxoboxData? stored, WikiTaxoboxData? parsed) {
        PagesRead++;
        switch (result.Outcome) {
            case TaxoboxReparseOutcome.Unchanged:
                Unchanged++;
                return;
            case TaxoboxReparseOutcome.Added:
                Added++;
                break;
            case TaxoboxReparseOutcome.Removed:
                Removed++;
                break;
            default:
                Changed++;
                if (result.Columns.Contains("scientific_name")) {
                    ScientificNameChanged++;
                }
                if (result.Columns.Any(column => column != "scientific_name")) {
                    OtherColumnsChanged++;
                }
                if (result.ParameterChanged("name")) {
                    NameChanged++;
                }
                break;
        }

        var before = CommonName(stored, pageTitle);
        var after = CommonName(parsed, pageTitle);
        if (!string.Equals(before, after, StringComparison.Ordinal)) {
            CommonNameChanged++;
            if (CommonNameExamples.Count < ExampleCount) {
                CommonNameExamples.Add((pageTitle, before, after));
            }
        }
    }
}

internal static class TaxoboxReparse {
    private static readonly IReadOnlyList<string> None = Array.Empty<string>();

    public static TaxoboxReparseResult Compare(WikiTaxoboxData? stored, WikiTaxoboxData? parsed) {
        if (stored is null) {
            return new(parsed is null ? TaxoboxReparseOutcome.Unchanged : TaxoboxReparseOutcome.Added, None, None);
        }
        if (parsed is null) {
            return new(TaxoboxReparseOutcome.Removed, None, None);
        }

        var columns = new List<string>();
        void Column(string name, string? a, string? b) {
            if (!string.Equals(a, b, StringComparison.Ordinal)) {
                columns.Add(name);
            }
        }
        Column("scientific_name", stored.ScientificName, parsed.ScientificName);
        Column("rank", stored.Rank, parsed.Rank);
        Column("kingdom", stored.Kingdom, parsed.Kingdom);
        Column("phylum", stored.Phylum, parsed.Phylum);
        Column("class_name", stored.Class, parsed.Class);
        Column("order_name", stored.Order, parsed.Order);
        Column("family", stored.Family, parsed.Family);
        Column("subfamily", stored.Subfamily, parsed.Subfamily);
        Column("tribe", stored.Tribe, parsed.Tribe);
        Column("genus", stored.Genus, parsed.Genus);
        Column("species", stored.Species, parsed.Species);
        if (stored.IsMonotypic != parsed.IsMonotypic) {
            columns.Add("is_monotypic");
        }

        var parameters = ChangedParameters(stored.DataJson, parsed.DataJson);
        return columns.Count == 0 && parameters.Count == 0
            ? new(TaxoboxReparseOutcome.Unchanged, None, None)
            : new(TaxoboxReparseOutcome.Changed, columns, parameters);
    }

    /// <summary>
    /// The template parameters whose values differ between two data_json texts, in the order
    /// they first appear; parameter names compared ignoring case, as the parser reads them.
    /// The order of the keys in the JSON text is not a change.
    /// </summary>
    public static IReadOnlyList<string> ChangedParameters(string? storedJson, string? parsedJson) {
        var stored = Fields(storedJson);
        var parsed = Fields(parsedJson);
        var changed = new List<string>();
        foreach (var (key, value) in stored) {
            if (!parsed.TryGetValue(key, out var other) || !string.Equals(value, other, StringComparison.Ordinal)) {
                changed.Add(key);
            }
        }
        foreach (var key in parsed.Keys) {
            if (!stored.ContainsKey(key)) {
                changed.Add(key);
            }
        }
        return changed;
    }

    /// <summary>The value of one template parameter in a data_json text; null when it is absent.</summary>
    public static string? Parameter(string? json, string name) =>
        Fields(json).TryGetValue(name, out var value) ? value : null;

    private static Dictionary<string, string> Fields(string? json) {
        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (string.IsNullOrWhiteSpace(json)) {
            return fields;
        }
        try {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind != JsonValueKind.Object) {
                return fields;
            }
            foreach (var property in document.RootElement.EnumerateObject()) {
                fields[property.Name] = property.Value.ValueKind == JsonValueKind.String
                    ? property.Value.GetString() ?? string.Empty
                    : property.Value.GetRawText();
            }
        } catch (JsonException) {
            // Unreadable JSON has no parameters; every parameter of the other side is a change.
        }
        return fields;
    }
}
