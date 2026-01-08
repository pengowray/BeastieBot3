using System;
using System.Collections.Generic;
using System.IO;

namespace BeastieBot3.WikipediaLists.Legacy;

internal sealed class LegacyTaxaRuleList {
    private readonly Dictionary<string, LegacyTaxonRules> _records = new(StringComparer.OrdinalIgnoreCase);

    public LegacyTaxaRuleList(string rulesPath) {
        if (string.IsNullOrWhiteSpace(rulesPath)) {
            throw new ArgumentException("Rules path was not provided.", nameof(rulesPath));
        }

        var fullPath = Path.GetFullPath(rulesPath);
        if (!File.Exists(fullPath)) {
            throw new FileNotFoundException($"rules-list.txt not found at {fullPath}", fullPath);
        }

        Compile(File.ReadAllLines(fullPath));
    }

    public LegacyTaxonRules? Get(string taxon) {
        if (string.IsNullOrWhiteSpace(taxon)) {
            return null;
        }

        return _records.TryGetValue(taxon.Trim(), out var rules) ? rules : null;
    }

    private void Compile(IEnumerable<string> lines) {
        var lineNumber = 0;
        foreach (var raw in lines) {
            lineNumber++;
            var line = StripComments(raw);
            if (string.IsNullOrWhiteSpace(line)) {
                continue;
            }

            if (line.Contains(" = ", StringComparison.Ordinal)) {
                SplitAndAssign(lineNumber, line, " = ", LegacyTaxonField.CommonName);
            } else if (line.Contains(" plural ", StringComparison.Ordinal)) {
                SplitAndAssign(lineNumber, line, " plural ", LegacyTaxonField.CommonPlural);
            } else if (line.Contains(" adj ", StringComparison.Ordinal)) {
                SplitAndAssign(lineNumber, line, " adj ", LegacyTaxonField.Adjective);
            } else if (line.Contains(" wikilink ", StringComparison.Ordinal)) {
                SplitAndAssign(lineNumber, line, " wikilink ", LegacyTaxonField.Wikilink);
            }
        }
    }

    private static string StripComments(string input) {
        if (string.IsNullOrWhiteSpace(input)) {
            return string.Empty;
        }

        var index = input.IndexOf("//", StringComparison.Ordinal);
        var withoutComment = index >= 0 ? input[..index] : input;
        return withoutComment.Trim();
    }

    private void SplitAndAssign(int lineNumber, string line, string separator, LegacyTaxonField field) {
        var parts = line.Split(new[] { separator }, 2, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2) {
            throw new InvalidDataException($"Malformed rules line {lineNumber}: {line}");
        }

        var taxon = parts[0].Trim();
        var value = parts[1].Trim();
        if (string.IsNullOrEmpty(taxon) || string.IsNullOrEmpty(value)) {
            return;
        }

        var record = _records.TryGetValue(taxon, out var existing) ? existing : (_records[taxon] = new LegacyTaxonRules());
        record[field] = value;
    }
}

internal enum LegacyTaxonField {
    None,
    CommonName,
    CommonPlural,
    Adjective,
    Wikilink
}

internal sealed class LegacyTaxonRules {
    private readonly Dictionary<LegacyTaxonField, string> _values = new();

    public string? this[LegacyTaxonField field] {
        get => _values.TryGetValue(field, out var value) ? value : null;
        set {
            if (string.IsNullOrWhiteSpace(value)) {
                _values.Remove(field);
            } else {
                _values[field] = value.Trim();
            }
        }
    }

    public string? CommonName => this[LegacyTaxonField.CommonName];
    public string? CommonPlural => this[LegacyTaxonField.CommonPlural];
    public string? Adjective => this[LegacyTaxonField.Adjective];
    public string? Wikilink => this[LegacyTaxonField.Wikilink];
}
