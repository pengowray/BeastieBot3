using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using YamlDotNet.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// The rules in rules/iucn-not-assigned.yml: an order for a family whose IUCN order is "NOT ASSIGNED",
// and a family for a genus whose IUCN family is "NOT ASSIGNED". They are applied on a read-only IUCN
// connection by a temporary view with the same name as the IUCN view, so every query the list
// generator, the charts, the grouping page and the CoL placement send through that connection sees
// the assigned values: headings, list membership and counts all agree. The IUCN database itself is
// never written, and the audit-site and report commands, which open their own connections, keep
// seeing IUCN's values.

namespace BeastieBot3.Iucn;

internal sealed class IucnNotAssignedRules {
    public const string FileName = "iucn-not-assigned.yml";

    /// <summary>The value IUCN stores for a rank it does not assign.</summary>
    public const string NotAssigned = "NOT ASSIGNED";

    private const string ViewName = "view_assessments_html_taxonomy_html";

    public static readonly IucnNotAssignedRules None = new(Array.Empty<OrderRule>(), Array.Empty<FamilyRule>(), null);

    private IucnNotAssignedRules(IReadOnlyList<OrderRule> orders, IReadOnlyList<FamilyRule> families, string? sourcePath) {
        Orders = orders;
        Families = families;
        SourcePath = sourcePath;
    }

    /// <summary>An order for every species of a family whose IUCN order is "NOT ASSIGNED".</summary>
    public sealed record OrderRule(string Class, string Family, string Order);

    /// <summary>A family for every species of a genus whose IUCN family is "NOT ASSIGNED".</summary>
    public sealed record FamilyRule(string Class, string Order, string Genus, string Family);

    public IReadOnlyList<OrderRule> Orders { get; }
    public IReadOnlyList<FamilyRule> Families { get; }

    /// <summary>The file the rules came from, or null when there was none.</summary>
    public string? SourcePath { get; }

    public bool IsEmpty => Orders.Count == 0 && Families.Count == 0;

    /// <summary>
    /// A short hash of the rules (not of the file's comments or layout). Stored with the CoL
    /// placement, which is out of date when the rules change. Empty when there are no rules.
    /// </summary>
    public string Fingerprint {
        get {
            if (IsEmpty) {
                return string.Empty;
            }
            var text = string.Join("\n",
                Orders.Select(r => $"o|{r.Class}|{r.Family}|{r.Order}").OrderBy(s => s, StringComparer.Ordinal)
                    .Concat(Families.Select(r => $"f|{r.Class}|{r.Order}|{r.Genus.ToUpperInvariant()}|{r.Family}").OrderBy(s => s, StringComparer.Ordinal)));
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(text));
            return Convert.ToHexString(hash, 0, 6).ToLowerInvariant();
        }
    }

    /// <summary>The rules file in a rules folder; no rules when the folder or file is missing.</summary>
    public static IucnNotAssignedRules LoadFromRulesDir(string? rulesDir) =>
        string.IsNullOrWhiteSpace(rulesDir) ? None : Load(Path.Combine(rulesDir, FileName));

    /// <summary>
    /// Reads the rules. A missing file means no rules. A file that cannot be read, or a rule with a
    /// blank value, throws <see cref="InvalidOperationException"/> naming the file.
    /// </summary>
    public static IucnNotAssignedRules Load(string path) {
        if (!File.Exists(path)) {
            return None;
        }

        RulesFile? file;
        try {
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .Build();
            file = deserializer.Deserialize<RulesFile>(File.ReadAllText(path));
        } catch (YamlException ex) {
            throw new InvalidOperationException($"{path}: {ex.Message}", ex);
        }

        return Parse(file, path);
    }

    /// <summary>Rules from YAML text; for tests.</summary>
    public static IucnNotAssignedRules FromYaml(string yaml) {
        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();
        return Parse(deserializer.Deserialize<RulesFile>(yaml), sourcePath: null);
    }

    private static IucnNotAssignedRules Parse(RulesFile? file, string? sourcePath) {
        string Required(string? value, string field, int index, string list) =>
            string.IsNullOrWhiteSpace(value)
                ? throw new InvalidOperationException($"{sourcePath ?? FileName}: rule {index + 1} under '{list}' has no {field}.")
                : value.Trim();

        var orders = (file?.Orders ?? new List<RawRule>())
            .Select((r, i) => new OrderRule(
                Required(r.Class, "class", i, "orders").ToUpperInvariant(),
                Required(r.Family, "family", i, "orders").ToUpperInvariant(),
                Required(r.Order, "order", i, "orders").ToUpperInvariant()))
            .ToList();
        var families = (file?.Families ?? new List<RawRule>())
            .Select((r, i) => new FamilyRule(
                Required(r.Class, "class", i, "families").ToUpperInvariant(),
                Required(r.Order, "order", i, "families").ToUpperInvariant(),
                Required(r.Genus, "genus", i, "families"),
                Required(r.Family, "family", i, "families").ToUpperInvariant()))
            .ToList();
        return new IucnNotAssignedRules(orders, families, sourcePath);
    }

    /// <summary>
    /// The order and family the lists use for a taxon: IUCN's values, with "NOT ASSIGNED" replaced
    /// where a rule covers it. Values are compared without regard to case.
    /// </summary>
    public (string? Order, string? Family) Resolve(string? className, string? orderName, string? familyName, string? genusName) {
        var order = orderName;
        if (IsNotAssigned(orderName)) {
            var rule = Orders.FirstOrDefault(r => Same(r.Class, className) && Same(r.Family, familyName));
            if (rule is not null) {
                order = rule.Order;
            }
        }

        var family = familyName;
        if (IsNotAssigned(familyName)) {
            var rule = Families.FirstOrDefault(r => Same(r.Class, className) && Same(r.Order, orderName) && Same(r.Genus, genusName));
            if (rule is not null) {
                family = rule.Family;
            }
        }

        return (order, family);
    }

    /// <summary>A read-only connection string for the IUCN database with pooling off, for <see cref="ApplyTo"/>.</summary>
    public static string ConnectionString(string databasePath) => new SqliteConnectionStringBuilder {
        DataSource = databasePath,
        Mode = SqliteOpenMode.ReadOnly,
        Pooling = false,
    }.ToString();

    public static bool IsNotAssigned(string? value) =>
        value is not null && value.Trim().Equals(NotAssigned, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Makes every later query on <paramref name="connection"/> that names the IUCN view see the
    /// assigned order and family, through a temporary view of the same name (SQLite looks in the
    /// temporary schema first). Works on a read-only connection. Does nothing when there are no
    /// rules or the database has no such view.
    /// The connection must be opened with pooling off (<see cref="ConnectionString"/>): a pooled
    /// connection keeps its temporary view when it goes back to the pool, and the next caller that
    /// opens the same database, such as an audit report, would read the assigned values as IUCN's.
    /// </summary>
    public void ApplyTo(SqliteConnection connection) {
        if (IsEmpty) {
            return;
        }
        if (new SqliteConnectionStringBuilder(connection.ConnectionString).Pooling) {
            throw new InvalidOperationException(
                "IucnNotAssignedRules.ApplyTo needs a connection opened with Pooling=false; use IucnNotAssignedRules.ConnectionString.");
        }

        var columns = new List<string>();
        using (var info = connection.CreateCommand()) {
            info.CommandText = $"PRAGMA main.table_info({ViewName})";
            using var reader = info.ExecuteReader();
            while (reader.Read()) {
                columns.Add(reader.GetString(1));
            }
        }
        if (!columns.Contains("orderName") || !columns.Contains("familyName")) {
            return;
        }

        var select = string.Join(",\n  ", columns.Select(column => column switch {
            "orderName" when Orders.Count > 0 => $"{OrderCase()} AS orderName",
            "familyName" when Families.Count > 0 => $"{FamilyCase()} AS familyName",
            _ => Quote(column),
        }));

        using var create = connection.CreateCommand();
        create.CommandText =
            $"DROP VIEW IF EXISTS temp.{ViewName};\n" +
            $"CREATE TEMP VIEW {ViewName} AS\nSELECT\n  {select}\nFROM main.{ViewName}";
        create.ExecuteNonQuery();
    }

    // The values come from the rules file, so they are written as escaped SQL string literals.
    private string OrderCase() {
        var sb = new StringBuilder("CASE");
        foreach (var rule in Orders) {
            sb.Append(CultureInfo.InvariantCulture,
                $" WHEN orderName = {Literal(NotAssigned)} AND className = {Literal(rule.Class)} AND familyName = {Literal(rule.Family)} THEN {Literal(rule.Order)}");
        }
        return sb.Append(" ELSE orderName END").ToString();
    }

    private string FamilyCase() {
        var sb = new StringBuilder("CASE");
        foreach (var rule in Families) {
            sb.Append(CultureInfo.InvariantCulture,
                $" WHEN familyName = {Literal(NotAssigned)} AND className = {Literal(rule.Class)} AND orderName = {Literal(rule.Order)} AND genusName = {Literal(rule.Genus)} COLLATE NOCASE THEN {Literal(rule.Family)}");
        }
        return sb.Append(" ELSE familyName END").ToString();
    }

    private static string Literal(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static bool Same(string ruleValue, string? value) =>
        value is not null && ruleValue.Equals(value.Trim(), StringComparison.OrdinalIgnoreCase);

    private sealed class RulesFile {
        public List<RawRule>? Orders { get; set; }
        public List<RawRule>? Families { get; set; }
    }

    private sealed class RawRule {
        public string? Class { get; set; }
        public string? Order { get; set; }
        public string? Family { get; set; }
        public string? Genus { get; set; }
    }
}
