using System;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// An in-memory Wikipedia cache holding the pages and titles of the October 2026 cache for two
// IUCN plants that were matched to the article about an animal with the same name:
// - the fig Ficus variegata (IUCN 147494668), matched to "Ficus variegata (gastropod)". "Ficus
//   variegata" is a disambiguation page, and the plant's article is "Ficus variegata (plant)";
// - the palm Gaussia princeps (IUCN 201634), matched to "Gaussia princeps (crustacean)". The cache
//   has not downloaded "Gaussia princeps".
// Also the plant Beilschmiedia madagascariensis (69222293), matched through its IUCN synonym
// Bernieria madagascariensis to the bird article "Long-billed bernieria", whose only kingdom
// evidence is its "Birds described in 1789" category, and the fish Orestias elegans (176675428),
// whose own name is a disambiguation page.
internal sealed class WikiKingdomFixture : IDisposable {
    public const string FicusVariegataId = "147494668";
    public const string GaussiaPrincepsId = "201634";
    public const string BeilschmiediaId = "69222293";

    private static readonly DateTime Now = new(2026, 10, 3, 0, 0, 0, DateTimeKind.Utc);

    public WikipediaCacheStore Cache { get; }

    public long FicusGastropodPage { get; }
    public long GaussiaCrustaceanPage { get; }
    public long BernieriaPage { get; }

    public WikiKingdomFixture() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        Cache = WikipediaCacheStore.OpenFromConnection(connection);

        Cache.AddDumpTitles([
            "Ficus variegata", "Ficus variegata (Tree)", "Ficus variegata (disambiguation)",
            "Ficus variegata (gastropod)", "Ficus variegata (plant)", "Ficus variegata (tree)",
            "Gaussia princeps", "Gaussia princeps (copepod)", "Gaussia princeps (crustacean)",
            "Gaussia princeps (disambiguation)", "Gaussia princeps (palm)", "Gaussia princeps (plant)",
            "Gaussia princeps (tree)",
            "Orestias elegans", "Orestias elegans (fish)", "Orestias elegans (plant)",
            "Long-billed bernieria",
        ]);

        AddPage("Ficus variegata", disambiguation: true,
            "'''''Ficus variegata''''' may refer to:\n* [[Ficus variegata (plant)]], a species of tropical fig tree\n"
            + "* [[Ficus variegata (gastropod)]], a species of sea snail");
        FicusGastropodPage = AddPage("Ficus variegata (gastropod)", disambiguation: false,
            "{{Speciesbox\n| genus = Ficus (gastropod)\n| species = variegata\n| authority = [[Röding]], 1798\n}}");
        AddTaxobox(FicusGastropodPage, "Ficus (gastropod)", "variegata",
            """{"genus":"Ficus (gastropod)","species":"variegata","authority":"[[Röding]], 1798"}""");
        Cache.ReplaceCategories(FicusGastropodPage, ["Ficidae", "Gastropods described in 1798", "Littorinimorpha stubs"]);

        GaussiaCrustaceanPage = AddPage("Gaussia princeps (crustacean)", disambiguation: false,
            "{{Speciesbox\n| genus = Gaussia (crustacean)\n| species = princeps\n| authority = (T. Scott, 1894)\n}}");
        AddTaxobox(GaussiaCrustaceanPage, "Gaussia (crustacean)", "princeps",
            """{"genus":"Gaussia (crustacean)","species":"princeps","authority":"(T. Scott, 1894)"}""");
        Cache.ReplaceCategories(GaussiaCrustaceanPage, ["Calanoida", "Copepod stubs", "Crustaceans described in 1894"]);

        BernieriaPage = AddPage("Long-billed bernieria", disambiguation: false,
            "{{speciesbox\n| name = Long-billed bernieria\n| genus = Bernieria\n| species = madagascariensis\n}}");
        AddTaxobox(BernieriaPage, "Bernieria", "madagascariensis",
            """{"name":"Long-billed bernieria","genus":"Bernieria","species":"madagascariensis"}""");
        Cache.ReplaceCategories(BernieriaPage, ["Birds described in 1789", "Malagasy warblers", "Taxa named by Johann Friedrich Gmelin"]);

        AddPage("Orestias elegans", disambiguation: true,
            "'''''Orestias elegans''''' may refer to:\n* [[Orestias elegans (fish)]]\n* [[Orestias elegans (plant)]]");
    }

    /// <summary>A downloaded page with <paramref name="wikitext"/>; returns its row id.</summary>
    public long AddPage(string title, bool disambiguation, string wikitext) {
        var row = Cache.UpsertPageCandidate(new WikiPageCandidate(title, title, null, Now, Now)).PageRowId;
        Cache.SavePageContent(new WikiPageContent(row, null, title, title, null, false, null, disambiguation, false,
            wikitext.Contains("box", StringComparison.OrdinalIgnoreCase), null, null, wikitext, Cache.BeginImport("test"), Now));
        return row;
    }

    /// <summary>A downloaded redirect from <paramref name="title"/> to <paramref name="target"/>.</summary>
    public long AddRedirect(string title, string target) {
        var row = Cache.UpsertPageCandidate(new WikiPageCandidate(title, title, null, Now, Now)).PageRowId;
        Cache.MarkRedirectStub(row, title, title, target, Now);
        return row;
    }

    /// <summary>Records <paramref name="taxonId"/> as matched to <paramref name="pageTitle"/> (row <paramref name="pageRowId"/>).</summary>
    public void Match(string taxonId, long pageRowId, string pageTitle, string method) =>
        Cache.UpsertTaxonMatch(new TaxonWikiMatch(TaxonSources.Iucn, taxonId, TaxonWikiMatchStatus.Matched, pageRowId,
            pageTitle, pageTitle, null, pageTitle, method, null, Now));

    private void AddTaxobox(long page, string genus, string species, string json) =>
        Cache.UpsertTaxoboxData(new WikiTaxoboxData(page, null, "species", null, null, null, null, null, null, null,
            genus, species, null, json));

    public void Dispose() => Cache.Dispose();
}
