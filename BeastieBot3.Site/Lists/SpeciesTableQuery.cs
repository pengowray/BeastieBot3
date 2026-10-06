using BeastieBot3.Site.Pages;

// The species table options of a group page, read from and written to its query string beside the
// list options of GroupListQuery:
//
//   type      table: {{Species table}} tables (bullets: the bullet list, the default)
//   refs      none | list | inline     references: none, list-defined (the default), or in the row
//   refnames  common | id | sci        ref names: "IUCN" + English name (the default), iucn-<taxon id>,
//                                      or the scientific name
//   cite      iucn | q                 {{cite iucn}} (the default), or {{cite Q}} when the assessment
//                                      has a Wikidata item (IucnReference)
//   cols      all | nodiet | noecology blank image, range, size, habitat and diet (the default);
//                                      no-diet=yes; no-ecology=yes
//   summary   0: no {{IUCN statuses}} box

namespace BeastieBot3.Site.Lists;

public static class SpeciesTableQuery {
    public static readonly string[] Keys = ["type", "refs", "refnames", IucnReference.QueryKey, "cols", "summary"];

    public static SpeciesTableOptions Read(IQueryCollection query) {
        var defaults = new SpeciesTableOptions();
        return new SpeciesTableOptions {
            Type = Last(query, "type") == "table" ? ListType.Tables : ListType.Bullets,
            References = Last(query, "refs") switch {
                "none" => TableReferences.None,
                "inline" => TableReferences.Inline,
                _ => defaults.References,
            },
            RefNames = Last(query, "refnames") switch {
                "id" => TableRefNames.TaxonId,
                "sci" => TableRefNames.ScientificName,
                _ => defaults.RefNames,
            },
            Template = IucnReference.FromQuery(Last(query, IucnReference.QueryKey)),
            Columns = Last(query, "cols") switch {
                "nodiet" => TableColumns.NoDiet,
                "noecology" => TableColumns.NoEcology,
                _ => defaults.Columns,
            },
            Summary = Last(query, "summary") != "0",
        };
    }

    /// The query parts for these options, only those that differ from the defaults. The table
    /// options are written only for a table list.
    public static IReadOnlyList<string> Parts(SpeciesTableOptions options) {
        var parts = new List<string>();
        if (!options.IsTable) {
            return parts;
        }
        parts.Add("type=table");
        if (options.References != TableReferences.ListDefined) {
            parts.Add("refs=" + ReferencesKey(options.References));
        }
        if (options.RefNames != TableRefNames.CommonName) {
            parts.Add("refnames=" + RefNamesKey(options.RefNames));
        }
        if (options.Template != ReferenceTemplate.CiteIucn) {
            parts.Add(IucnReference.QueryKey + "=" + IucnReference.QueryValue(options.Template));
        }
        if (options.Columns != TableColumns.All) {
            parts.Add("cols=" + ColumnsKey(options.Columns));
        }
        if (!options.Summary) {
            parts.Add("summary=0");
        }
        return parts;
    }

    /// The list options' query string (GroupListQuery.Write: empty or "?...") with these options added.
    public static string Append(string listQuery, SpeciesTableOptions options) {
        var parts = Parts(options);
        if (parts.Count == 0) {
            return listQuery;
        }
        return (listQuery.Length == 0 ? "?" : listQuery + "&") + string.Join("&", parts);
    }

    public static string ReferencesKey(TableReferences value) => value switch {
        TableReferences.None => "none",
        TableReferences.Inline => "inline",
        _ => "list",
    };

    public static string RefNamesKey(TableRefNames value) => value switch {
        TableRefNames.TaxonId => "id",
        TableRefNames.ScientificName => "sci",
        _ => "common",
    };

    public static string ColumnsKey(TableColumns value) => value switch {
        TableColumns.NoDiet => "nodiet",
        TableColumns.NoEcology => "noecology",
        _ => "all",
    };

    // The last value: a form sends a hidden "0" before a checkbox's "1".
    private static string? Last(IQueryCollection query, string key) =>
        query.TryGetValue(key, out var values) && values.Count > 0 ? values[^1]?.Trim() : null;
}
