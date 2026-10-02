using System;
using System.IO;
using BeastieBot3.Col;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;
using Xunit;

namespace BeastieBot3.Tests;

public sealed class IucnNotAssignedRulesTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-not-assigned-" + Guid.NewGuid().ToString("N"));

    public IucnNotAssignedRulesTests() => Directory.CreateDirectory(_dir);

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        Directory.Delete(_dir, recursive: true);
    }

    private const string Yaml = """
        orders:
          - { class: Actinopterygii, family: pomacentridae, order: Perciformes }
        families:
          - { class: ANTHOZOA, order: SCLERACTINIA, genus: Pachyseris, family: PACHYSERIDAE }
        """;

    [Fact]
    public void Resolve_FillsOnlyNotAssignedValues_IgnoringCase() {
        var rules = IucnNotAssignedRules.FromYaml(Yaml);

        Assert.Equal(("PERCIFORMES", "POMACENTRIDAE"), rules.Resolve("ACTINOPTERYGII", "NOT ASSIGNED", "POMACENTRIDAE", "Amphiprion"));
        Assert.Equal(("SCLERACTINIA", "PACHYSERIDAE"), rules.Resolve("ANTHOZOA", "SCLERACTINIA", "NOT ASSIGNED", "pachyseris"));
        // An order IUCN does assign is left alone, and so is a NOT ASSIGNED family with no rule.
        Assert.Equal(("CYPRINIFORMES", "POMACENTRIDAE"), rules.Resolve("ACTINOPTERYGII", "CYPRINIFORMES", "POMACENTRIDAE", "X"));
        Assert.Equal(("NOT ASSIGNED", "AMBASSIDAE"), rules.Resolve("ACTINOPTERYGII", "NOT ASSIGNED", "AMBASSIDAE", "Ambassis"));
    }

    [Fact]
    public void MissingFile_MeansNoRules_AndABlankValueNamesTheRule() {
        Assert.True(IucnNotAssignedRules.LoadFromRulesDir(_dir).IsEmpty);

        var path = Path.Combine(_dir, IucnNotAssignedRules.FileName);
        File.WriteAllText(path, "orders:\n  - { class: ACTINOPTERYGII, family: POMACENTRIDAE }\n");
        var ex = Assert.Throws<InvalidOperationException>(() => IucnNotAssignedRules.Load(path));
        Assert.Contains("rule 1 under 'orders' has no order", ex.Message);
    }

    [Fact]
    public void Fingerprint_FollowsTheRules_NotTheLayout() {
        var a = IucnNotAssignedRules.FromYaml(Yaml);
        var b = IucnNotAssignedRules.FromYaml("# comment\n" + Yaml.Replace("pomacentridae", "POMACENTRIDAE"));
        var c = IucnNotAssignedRules.FromYaml(Yaml.Replace("Perciformes", "Ovalentaria"));

        Assert.Equal(a.Fingerprint, b.Fingerprint);
        Assert.NotEqual(a.Fingerprint, c.Fingerprint);
        Assert.Equal(string.Empty, IucnNotAssignedRules.None.Fingerprint);
    }

    [Fact]
    public void PlacementStamp_KeepsTheFileStampSeparate() {
        var db = Path.Combine(_dir, "iucn.sqlite");
        File.WriteAllText(db, "x");
        var stamp = TaxonPlacementStore.IucnStamp(db, IucnNotAssignedRules.FromYaml(Yaml));

        Assert.NotEqual(TaxonPlacementStore.IucnStamp(db), stamp);
        Assert.Equal(TaxonPlacementStore.IucnStamp(db), TaxonPlacementStore.IucnFileStamp(stamp));
        Assert.Equal(TaxonPlacementStore.IucnStamp(db), TaxonPlacementStore.IucnStamp(db, IucnNotAssignedRules.None));
    }

    [Fact]
    public void ApplyTo_ShadowsTheIucnView_OnThisConnectionOnly() {
        var db = CreateIucnDatabase();
        var rules = IucnNotAssignedRules.FromYaml(Yaml);

        using (var withRules = new SqliteConnection(IucnNotAssignedRules.ConnectionString(db))) {
            withRules.Open();
            rules.ApplyTo(withRules);
            Assert.Equal("PERCIFORMES", Scalar(withRules, "SELECT orderName FROM view_assessments_html_taxonomy_html WHERE familyName = 'POMACENTRIDAE'"));
            Assert.Equal("PACHYSERIDAE", Scalar(withRules, "SELECT familyName FROM view_assessments_html_taxonomy_html WHERE genusName = 'Pachyseris'"));
            Assert.Equal("NOT ASSIGNED", Scalar(withRules, "SELECT orderName FROM main.view_assessments_html_taxonomy_html WHERE familyName = 'POMACENTRIDAE'"));
            Assert.Equal("3", Scalar(withRules, "SELECT COUNT(*) FROM view_assessments_html_taxonomy_html"));
        }

        // A later connection to the same file reads IUCN's own values.
        using var plain = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = db, Mode = SqliteOpenMode.ReadOnly }.ToString());
        plain.Open();
        Assert.Equal("NOT ASSIGNED", Scalar(plain, "SELECT orderName FROM view_assessments_html_taxonomy_html WHERE familyName = 'POMACENTRIDAE'"));
    }

    [Fact]
    public void ApplyTo_RefusesAPooledConnection() {
        var db = CreateIucnDatabase();
        using var pooled = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = db, Mode = SqliteOpenMode.ReadOnly }.ToString());
        pooled.Open();

        Assert.Throws<InvalidOperationException>(() => IucnNotAssignedRules.FromYaml(Yaml).ApplyTo(pooled));
    }

    [Fact]
    public void TwoRulesForOneTaxon_AreRejected() {
        var ex = Assert.Throws<InvalidOperationException>(() => IucnNotAssignedRules.FromYaml("""
            orders:
              - { class: ACTINOPTERYGII, family: POMACENTRIDAE, order: PERCIFORMES }
              - { class: actinopterygii, family: Pomacentridae, order: OVALENTARIA }
            """));
        Assert.Contains("rules 1 and 2 under 'orders'", ex.Message);
    }

    [Fact]
    public void SplitKey_RoundTrips_AndPlainValuesAreNotSplitKeys() {
        Assert.True(IucnNotAssignedRules.TryReadSplitKey(IucnNotAssignedRules.SplitKey("POMACENTRIDAE"), out var family));
        Assert.Equal("POMACENTRIDAE", family);
        Assert.False(IucnNotAssignedRules.TryReadSplitKey("PERCIFORMES", out _));
        Assert.Equal("family", IucnNotAssignedRules.NextRank("Order"));
        Assert.Null(IucnNotAssignedRules.NextRank("genus"));
    }

    private string CreateIucnDatabase() {
        var db = Path.Combine(_dir, "iucn.sqlite");
        using var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = db, Pooling = false }.ToString());
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE taxonomy (taxonId INTEGER, kingdomName TEXT, className TEXT, orderName TEXT, familyName TEXT, genusName TEXT);
            INSERT INTO taxonomy VALUES
              (1, 'ANIMALIA', 'ACTINOPTERYGII', 'NOT ASSIGNED', 'POMACENTRIDAE', 'Amphiprion'),
              (2, 'ANIMALIA', 'ACTINOPTERYGII', 'CYPRINIFORMES', 'CYPRINIDAE', 'Cyprinus'),
              (3, 'ANIMALIA', 'ANTHOZOA', 'SCLERACTINIA', 'NOT ASSIGNED', 'Pachyseris');
            CREATE VIEW view_assessments_html_taxonomy_html AS SELECT * FROM taxonomy;
            """;
        command.ExecuteNonQuery();
        return db;
    }

    private static string? Scalar(SqliteConnection connection, string sql) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToString(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
    }
}
