using System.Collections.Generic;
using System.Text;
using BeastieBot3.WikipediaLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// Pins the count-scope SQL — the single most load-bearing logic in the audit (the three count
// scopes that used to diverge). TaxonFilterSql is the one place lists and charts agree, so its
// emitted SQL must stay byte-stable.
public class CountScopeTests {
    [Fact]
    public void GlobalSpeciesPredicate_DefaultAlias_IsExact() {
        var sql = TaxonFilterSql.GlobalSpeciesPredicate();
        Assert.Equal(
            "(v.infraType IS NULL OR v.infraType = '') " +
            "AND (v.subpopulationName IS NULL OR TRIM(v.subpopulationName) = '') " +
            "AND (v.scopes IS NULL OR v.scopes = '' OR v.scopes LIKE '%Global%')",
            sql);
    }

    [Fact]
    public void GlobalSpeciesPredicate_HonoursAlias() {
        var sql = TaxonFilterSql.GlobalSpeciesPredicate("a");
        Assert.Contains("a.infraType IS NULL", sql);
        Assert.Contains("a.subpopulationName", sql);
        Assert.Contains("a.scopes LIKE '%Global%'", sql);
        Assert.DoesNotContain("v.", sql);
    }

    [Theory]
    [InlineData("kingdom", "kingdomName")]
    [InlineData("Phylum", "phylumName")]
    [InlineData("CLASS", "className")]
    [InlineData("order", "orderName")]
    [InlineData("family", "familyName")]
    [InlineData("genus", "genusName")]
    public void ResolveColumn_MapsRanks(string rank, string expected) {
        Assert.Equal(expected, TaxonFilterSql.ResolveColumn(rank));
    }

    [Theory]
    [InlineData("species")]
    [InlineData("subspecies")]
    [InlineData(null)]
    public void ResolveColumn_ReturnsNullForUnmapped(string? rank) {
        Assert.Null(TaxonFilterSql.ResolveColumn(rank));
    }

    [Fact]
    public void NormalizeValue_UppercasesHighRanks_TrimsGenus() {
        Assert.Equal("FELIDAE", TaxonFilterSql.NormalizeValue("family", "  felidae "));
        Assert.Equal("ANIMALIA", TaxonFilterSql.NormalizeValue("kingdom", "Animalia"));
        Assert.Equal("Panthera", TaxonFilterSql.NormalizeValue("genus", " Panthera "));
        Assert.Null(TaxonFilterSql.NormalizeValue("family", "   "));
    }

    [Fact]
    public void AppendFilter_System_EmitsLikeWithWildcards() {
        var sql = new StringBuilder();
        var ps = new List<SqliteParameter>();
        TaxonFilterSql.AppendFilter(sql, ps, new TaxonFilterDefinition { System = "Terrestrial" }, 0);

        Assert.Contains("AND v.systems LIKE @sys_0", sql.ToString());
        Assert.Equal("@sys_0", ps[0].ParameterName);
        Assert.Equal("%Terrestrial%", ps[0].Value);
    }

    [Fact]
    public void AppendFilter_SingleValueInclude_NormalizesAndBinds() {
        var sql = new StringBuilder();
        var ps = new List<SqliteParameter>();
        TaxonFilterSql.AppendFilter(sql, ps, new TaxonFilterDefinition { Rank = "family", Value = "Felidae" }, 0);

        Assert.Contains("AND v.familyName = @f_familyName_0", sql.ToString());
        Assert.Equal("FELIDAE", ps[0].Value);
    }

    [Fact]
    public void AppendFilter_MultipleValues_EmitsOrGroup() {
        var sql = new StringBuilder();
        var ps = new List<SqliteParameter>();
        TaxonFilterSql.AppendFilter(sql, ps,
            new TaxonFilterDefinition { Rank = "class", Values = new List<string> { "Aves", "Mammalia" } }, 0);

        Assert.Contains("AND (v.className = @f_className_0_0 OR v.className = @f_className_0_1)", sql.ToString());
        Assert.Equal("AVES", ps[0].Value);
        Assert.Equal("MAMMALIA", ps[1].Value);
    }

    [Fact]
    public void AppendFilter_Exclude_IsNullSafeNotIn() {
        var sql = new StringBuilder();
        var ps = new List<SqliteParameter>();
        TaxonFilterSql.AppendFilter(sql, ps,
            new TaxonFilterDefinition { Rank = "order", Exclude = new List<string> { "Rodentia" } }, 0);

        Assert.Contains("AND (v.orderName IS NULL OR v.orderName NOT IN (@x_orderName_0_0))", sql.ToString());
        Assert.Equal("RODENTIA", ps[0].Value);
    }
}
