using System.Text;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Lists;

namespace BeastieBot3.Site.Update;

// Missing species put into the {{Species table}}s of a list (List of felids, List of canids): a new
// {{Species table/row}} in the table of the species' genus, or a new {{Species table}} for a genus
// the list has no table for, next to the table of another genus of its family.
public static partial class ListPlacement {
    /// One {{Species table}} ... {{Species table/end}}: its header, rows and end, and the genus its
    /// header names.
    private sealed record GenusTableText(WikiTemplate Header, IReadOnlyList<WikiTemplate> Rows, WikiTemplate? End, string Genus);

    private sealed partial class Placer {
        private List<GenusTableText>? _genusTables;

        private List<GenusTableText> GenusTables() {
            if (_genusTables is not null) {
                return _genusTables;
            }
            _genusTables = [];
            WikiTemplate? header = null;
            var rows = new List<WikiTemplate>();
            void Close(WikiTemplate? end) {
                if (header is not null) {
                    var genus = header.Named("genus") is { } g ? CleanGenus(_scanner.CoreText(g.Value)) : string.Empty;
                    _genusTables.Add(new GenusTableText(header, [.. rows], end, genus));
                }
                header = null;
                rows.Clear();
            }
            foreach (var t in _scanner.Templates) {
                switch (t.Name) {
                    case "species table":
                        Close(null);
                        header = t;
                        break;
                    case "species table/row" when header is not null:
                        rows.Add(t);
                        break;
                    case "species table/end":
                        Close(t);
                        break;
                }
            }
            Close(null);
            return _genusTables;
        }

        // "[[Lycaon (genus)|Lycaon]]{{dagger|alt=Extinct}}" -> "Lycaon".
        private static string CleanGenus(string text) =>
            LinkLabel(DaggerTemplate().Replace(text, string.Empty)).Trim();

        private (GenusTableText Table, int Row)? RowOf(ListMember member) {
            var at = _lines.Start(member.Line);
            foreach (var table in GenusTables()) {
                for (var i = 0; i < table.Rows.Count; i++) {
                    if (table.Rows[i].Span.Start <= at && at < table.Rows[i].Span.End) {
                        return (table, i);
                    }
                }
            }
            return null;
        }

        // The keys of a row: the scientific name (the binomial with the table's genus for its
        // initial) and the common name (the text of name=).
        private (string? Scientific, string? Common) RowKeys(GenusTableText table, WikiTemplate row) {
            string? scientific = null;
            if (row.Named("binomial") is { } b) {
                var binomial = CleanGenus(_scanner.CoreText(b.Value));
                var parts = binomial.Split(' ', 2);
                scientific = parts.Length == 2 && parts[0].EndsWith('.') && table.Genus.Length > 0 ? $"{table.Genus} {parts[1]}" : binomial;
            }
            var common = row.Named("name") is { } n ? CleanGenus(_scanner.CoreText(n.Value)) : null;
            return (scientific, string.IsNullOrEmpty(common) ? null : common);
        }

        private PlacedTaxon? InSpeciesTable(ListTaxonRow taxon) {
            var mates = Listed(ListMemberSource.SpeciesTableRow).Where(m => m.Taxon.NodeId == taxon.NodeId).ToList();
            var found = mates.Select(RowOf).FirstOrDefault(r => r is not null);
            if (found is not { Table: var table } || table.Rows.Count == 0) {
                return null;
            }
            var keys = table.Rows.Select(r => RowKeys(table, r)).ToList();
            var (order, index) = Order([.. keys.Select(k => k.Scientific)], taxon.ScientificName, [.. keys.Select(k => k.Common)], taxon.CommonNameEn);
            var before = order != ListOrder.None && index < table.Rows.Count;
            var neighbourIndex = before ? index : table.Rows.Count - 1;
            if (order != ListOrder.None && index < table.Rows.Count && index > 0) {
                // After the row before it reads better in a report than before the next one.
                before = false;
                neighbourIndex = index - 1;
            }
            var neighbour = table.Rows[neighbourIndex];
            var text = NewRow(neighbour, taxon);
            if (text is null) {
                return null;
            }
            var position = before ? neighbour.Span.Start : neighbour.Span.End;
            var neighbourKeys = keys[neighbourIndex];
            var member = _members.FirstOrDefault(m => m.Source == ListMemberSource.SpeciesTableRow && neighbour.Span.Start <= _lines.Start(m.Line)
                && _lines.Start(m.Line) < neighbour.Span.End);
            return new PlacedTaxon(taxon, text, position, neighbourKeys.Common ?? neighbourKeys.Scientific ?? string.Empty, member?.Taxon,
                _scanner.LineOf(neighbour.Span.Start), before);
        }

        // A {{Species table/row}} for the taxon in the layout of the neighbour row: its parameters in
        // its order and spacing, with the values from the site database (SpeciesTable.Row) and the
        // others (image, range, size, habitat, diet, subspecies) left blank for the editor.
        private string? NewRow(WikiTemplate neighbour, ListTaxonRow taxon) {
            var extras = Extras(taxon);
            var options = new SpeciesTableOptions {
                Type = ListType.Tables,
                References = TableReferences.Inline,
                Template = _options.CiteQ ? Pages.ReferenceTemplate.CiteQ : Pages.ReferenceTemplate.CiteIucn,
            };
            // A species IUCN has not assessed has no extras: its authority comes from the Catalogue of Life.
            var extra = extras.GetValueOrDefault(taxon.TaxonId) ?? new TableTaxonExtra(taxon.TaxonId, taxon.Authority, null, null, null, null, null);
            var row = SpeciesTable.Row(taxon, extra, options, new SpeciesTable.RefNamer(TableRefNames.CommonName));
            // A reference the text already defines for the taxon's assessment is used again by name.
            var cited = CitedBy(taxon);
            var refName = row.RefName is { } name && _text.Contains(name, StringComparison.Ordinal) ? $"{name}-{taxon.TaxonId}" : row.RefName;
            var refText = cited is not null ? $"<ref name=\"{cited}\"/>"
                : row.Reference is { } reference ? $"<ref name=\"{refName}\">{reference}</ref>"
                : string.Empty;
            var values = new Dictionary<string, string>(StringComparer.Ordinal) {
                ["name"] = row.Name,
                ["binomial"] = row.Binomial,
                ["authority-name"] = row.AuthorityName,
                ["authority-year"] = row.AuthorityYear,
                ["authority-not-original"] = row.AuthorityNotOriginal ? "yes" : string.Empty,
                ["iucn-status"] = row.StatusCode,
                ["population"] = row.Population,
                ["direction"] = row.Direction + refText,
            };
            var edits = new List<(int Start, int End, string Text)>();
            foreach (var p in neighbour.Parameters) {
                if (p.Name is not { } raw) {
                    continue;
                }
                var key = WikitextScanner.NormalizeParameterName(raw);
                var core = _scanner.Core(p.Value);
                // Layout switches ("no-ecology=yes") stay as they are.
                if (key.StartsWith("no-", StringComparison.Ordinal)) {
                    continue;
                }
                var value = values.Remove(key, out var v) ? v : string.Empty;
                edits.Add(core.Length == 0 ? (p.Value.Start, p.Value.Start, value) : (core.Start, core.End, value));
            }
            // authority-not-original after authority-year, when the neighbour has no such parameter.
            if (values.Remove("authority-not-original", out var notOriginal) && notOriginal.Length > 0
                && neighbour.Named("authority-year") is { } year) {
                var end = _scanner.Core(year.Value).End;
                edits.Add((end, end, " |authority-not-original=yes"));
            }
            if (values.Keys.Any(k => k is "name" or "binomial" or "iucn-status")) {
                return null;
            }
            var sb = new StringBuilder();
            var at = neighbour.Span.Start;
            foreach (var (start, end, text) in edits.OrderBy(e => e.Start)) {
                sb.Append(_text, at, start - at).Append(text);
                at = end;
            }
            sb.Append(_text, at, neighbour.Span.End - at);
            return sb.ToString();
        }

        private ReferenceIndex? _references;

        // The name of a reference the text defines whose citation has the taxon's id
        // ("<ref name="IUCNBobrinskisserotine">{{cite iucn ... |article-number=e.T7914A22114842}}</ref>").
        private string? CitedBy(ListTaxonRow taxon) => (_references ??= ReferenceIndex.Read(_scanner)).NameForTaxon(taxon.TaxonId);

        private Dictionary<long, IReadOnlyDictionary<long, TableTaxonExtra>>? _extras;

        // The extras of the taxon's genus, read once per genus.
        private IReadOnlyDictionary<long, TableTaxonExtra> Extras(ListTaxonRow taxon) {
            _extras ??= [];
            if (!_extras.TryGetValue(taxon.NodeId, out var extras)) {
                var genus = _lookup.PathOf(taxon.NodeId)[^1];
                _extras[taxon.NodeId] = extras = _lookup.ExtrasOf(genus);
            }
            return extras;
        }

        // ------------------------------------------------------------ new genus tables

        /// The missing species whose genus has no table, in a list of genus tables: a new table for
        /// each genus, next to the tables of the other genera of its family that the list has, in
        /// order of genus name when the list keeps it.
        public List<PlacedTaxon> NewGenusTables(IReadOnlyList<ListTaxonRow> rest) {
            var placed = new List<PlacedTaxon>();
            var tables = GenusTables().Where(t => t.Rows.Count > 0 && t.End is not null).ToList();
            if (tables.Count == 0) {
                return placed;
            }
            // The family of each table, from a species the comparison found in it.
            var familyOf = new Dictionary<GenusTableText, int>();
            foreach (var m in Listed(ListMemberSource.SpeciesTableRow)) {
                if (RowOf(m) is { Table: var table } && !familyOf.ContainsKey(table) && Family(m.Taxon.NodeId!.Value) is { } family) {
                    familyOf[table] = family;
                }
            }
            foreach (var genus in rest.Where(t => t.Kind == TaxonKinds.Species).GroupBy(t => t.NodeId)) {
                var path = _lookup.PathOf(genus.Key);
                if (path[^1] is not { IsGenus: true } genusGroup || Family(genus.Key) is not { } family) {
                    continue;
                }
                var siblings = tables.Where(t => familyOf.TryGetValue(t, out var f) && f == family).ToList();
                if (siblings.Count == 0) {
                    continue;
                }
                var index = OrderedPlace([.. siblings.Select(t => (string?)t.Genus)], genusGroup.Name) ?? siblings.Count;
                var before = index < siblings.Count;
                var neighbour = before ? siblings[index] : siblings[^1];
                var species = genus.ToList();
                var rowKeys = neighbour.Rows.Select(r => RowKeys(neighbour, r)).ToList();
                var byCommon = OrderedPlace([.. rowKeys.Select(k => k.Scientific)], "") is null
                    && OrderedPlace([.. rowKeys.Select(k => k.Common)], "") is not null && species.All(s => s.CommonNameEn is not null);
                species = byCommon
                    ? [.. species.OrderBy(s => s.CommonNameEn, StringComparer.OrdinalIgnoreCase)]
                    : [.. species.OrderBy(s => s.ScientificName, StringComparer.OrdinalIgnoreCase)];
                var rows = species.Select(s => NewRow(neighbour.Rows[0], s)).ToList();
                if (rows.Any(r => r is null)) {
                    continue;
                }
                var text = new StringBuilder(NewHeader(neighbour.Header, genusGroup));
                foreach (var row in rows) {
                    text.Append('\n').Append(row);
                }
                text.Append('\n').Append(_text, neighbour.End!.Span.Start, neighbour.End.Span.Length);
                var position = before ? neighbour.Header.Span.Start : neighbour.End.Span.End;
                var line = _scanner.LineOf(before ? neighbour.Header.Span.Start : neighbour.End.Span.Start);
                for (var i = 0; i < species.Count; i++) {
                    placed.Add(new PlacedTaxon(species[i], i == 0 ? text.ToString() : string.Empty, position, neighbour.Genus, null, line, before));
                }
            }
            return placed;
        }

        private int? Family(int nodeId) => _lookup.PathOf(nodeId).LastOrDefault(g => g.Rank == "family")?.NodeId;

        // The {{Species table}} header of the neighbour table with this genus, its species count, and
        // no authority (the site database has none for genera).
        private string NewHeader(WikiTemplate neighbour, GroupRow genus) {
            var link = genus.EnwikiTitle is { } title && !string.Equals(title, genus.Name, StringComparison.Ordinal)
                ? $"[[{title}|{genus.Name}]]"
                : $"[[{genus.Name}]]";
            var values = new Dictionary<string, string>(StringComparer.Ordinal) {
                ["genus"] = link,
                ["species-count"] = SpeciesTable.CountWords(genus.SpeciesCount),
            };
            var sb = new StringBuilder();
            var at = neighbour.Span.Start;
            foreach (var p in neighbour.Parameters.Where(p => p.Name is not null)) {
                var key = WikitextScanner.NormalizeParameterName(p.Name!);
                if (key.StartsWith("no-", StringComparison.Ordinal)) {
                    continue;
                }
                var core = _scanner.Core(p.Value);
                var (start, end) = core.Length == 0 ? (p.Value.Start, p.Value.Start) : (core.Start, core.End);
                sb.Append(_text, at, start - at).Append(values.GetValueOrDefault(key, string.Empty));
                at = end;
            }
            sb.Append(_text, at, neighbour.Span.End - at);
            return sb.ToString();
        }
    }

    [System.Text.RegularExpressions.GeneratedRegex(@"\{\{\s*dagger[^{}]*\}\}|†", System.Text.RegularExpressions.RegexOptions.IgnoreCase)]
    private static partial System.Text.RegularExpressions.Regex DaggerTemplate();

    /// The text of a link: "[[Canis lupus|Wolf]]" is "Wolf"; a name with no link is itself.
    private static string LinkLabel(string text) => StatusUpdater.LinkText(text);
}
