using System;
using System.IO;
using System.Linq;
using BeastieBot3.SiteBuild;
using BeastieBot3.Taxonomy;
using BeastieBot3.Wikipedia;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

public class WikispeciesTaxonavigationTests {
    // From species.wikimedia.org, October 2026.
    private const string PantheraLeo = "{{Image|Lion.jpg|''Panthera leo''}}\n\n=={{int:Taxonavigation}}==\n{{Panthera (Leo)}}\nSpecies: ''[[Panthera leo]]''<br />\nSubspecies: \n{{ssp|P|anthera|l|eo|azandica}}, \n{{ssplast|P|anthera|l|eo|somaliensis}}<br>\n\n=={{int:Name}}==\n''Panthera leo'' (Linnaeus, 1758)\n";
    private const string PantheraLeoSubgenus = "{{Panthera}}\nSubgenus: ''[[Panthera (Leo)]]''<br/>\n<noinclude>[[Category:Taxonavigation templates]]</noinclude>\n<includeonly>[[Category:Pages with taxonavigation templates]]</includeonly>";
    private const string Panthera = "{{Pantherinae}}\nGenus: ''[[Panthera]]'' <br>\n<noinclude>[[Category:Taxonavigation templates]]</noinclude>";
    private const string Pantherinae = "{{Felidae}}\nSubfamilia: {{fbr|Pantherinae}}\n<noinclude>[[Category:Taxonavigation templates]]</noinclude>";
    private const string Felidae = "{{Taxonav|Feloidea}}\nFamilia: {{fbr|Felidae}}\n<noinclude>[[Category:Taxonavigation templates]]</noinclude>";
    private const string Mammalia = "{{Mammaliaformes}}\nClassis: {{cbr|Mammalia}}<!-- for {{Taxonav2}} --><section>{{#ifeq:{{{1|keyword}}}|summary|<section begin=summarytaxa/>‎{{TaxonavAmniota}}{{TaxonavSynapsida}}\nCladus: {{cbr|Mammaliaformes}}\nClassis: {{cbr|Mammalia}}<section end=summarytaxa/>}}‎</section>\n<noinclude>[[Category:Taxonavigation templates]]</noinclude>";
    private const string Quercus = "{{DISPLAYTITLE:{{Taxit|{{BASEPAGENAME}}}}|noreplace}}\n{{Fagaceae}}\nGenus: {{gbr|Quercus}}\n<noinclude>[[Category:Taxonavigation templates]]</noinclude>";
    private const string Biota = "__NOTOC__\nImperium: [[Biota]]<br />\n<noinclude>[[Category:Taxonavigation templates]]</noinclude>";

    [Fact]
    public void APageGivesItsTemplateAndStepsDownToItsOwnTaxon() {
        var t = WikispeciesTaxonavigation.ParsePage(PantheraLeo, "Panthera leo")!;
        Assert.Equal("Panthera (Leo)", t.Parent);
        Assert.Equal(new[] { new WikispeciesStep("species", "Panthera leo") }, t.Steps);
    }

    [Fact]
    public void APageWithNoTaxonavigationSectionGivesNothing() =>
        Assert.Null(WikispeciesTaxonavigation.ParsePage("Some text about [[Panthera]].", "Panthera leo"));

    [Theory]
    [InlineData(PantheraLeoSubgenus, "Panthera", "subgenus", "Panthera (Leo)")]
    [InlineData(Panthera, "Pantherinae", "genus", "Panthera")]
    [InlineData(Pantherinae, "Felidae", "subfamily", "Pantherinae")]
    [InlineData(Felidae, "Feloidea", "family", "Felidae")]
    [InlineData(Mammalia, "Mammaliaformes", "class", "Mammalia")]
    [InlineData(Quercus, "Fagaceae", "genus", "Quercus")]
    [InlineData(Biota, null, "empire", "Biota")]
    public void ATemplateGivesItsParentAndItsTaxon(string text, string? parent, string rank, string name) {
        var t = WikispeciesTaxonavigation.ParseTemplate(text)!;
        Assert.Equal(parent, t.Parent);
        Assert.Equal(new[] { new WikispeciesStep(rank, name) }, t.Steps);
    }

    [Fact]
    public void ALinkWithALabelGivesTheLinkedPage() {
        var t = WikispeciesTaxonavigation.ParseTemplate("{{Quercus}}\nSubgenus: [[Quercus subg. Quercus|''Q.'' subg. ''Quercus'']] <br/>\n")!;
        Assert.Equal(new[] { new WikispeciesStep("subgenus", "Quercus subg. Quercus") }, t.Steps);
    }

    [Fact]
    public void ATemplateCanNameSeveralTaxa() {
        var t = WikispeciesTaxonavigation.ParseTemplate("{{Aves}}\nOrdo: [[Passeriformes]]\nSubordo: [[Passeri]]\n")!;
        Assert.Equal(new[] { new WikispeciesStep("order", "Passeriformes"), new WikispeciesStep("suborder", "Passeri") }, t.Steps);
    }

    [Theory]
    [InlineData("Panthera leo ssp. persica", "Panthera leo persica")]
    [InlineData("Impatiens engleri subsp. pubescens", "Impatiens engleri subsp. pubescens")]
    [InlineData("Cassia afrofistula var. afrofistula", "Cassia afrofistula var. afrofistula")]
    public void TheTitleOfAnIucnName(string iucn, string title) => Assert.Equal(title, WikispeciesTaxonavigation.TitleFor(iucn));

    [Fact]
    public void TheLadderClimbsTheTemplatesFromTheTaxonPage() {
        var path = Path.Combine(Path.GetTempPath(), $"wikispecies-{Guid.NewGuid():N}.sqlite");
        try {
            using (var store = WikipediaCacheStore.Open(path)) {
                Add(store, "Panthera leo", PantheraLeo);
                Add(store, "Template:Panthera (Leo)", PantheraLeoSubgenus);
                Add(store, "Template:Panthera", Panthera);
                Add(store, "Template:Pantherinae", Pantherinae);
                // A template that names no taxon is passed through.
                Add(store, "Template:Felidae", "{{Biota}}\n");
                Add(store, "Template:Biota", Biota);
            }
            SqliteConnection.ClearAllPools();

            var nodes = SiteLadders.ReadWikispecies(path, ["Panthera leo", "Panthera onca"], default);

            var byId = nodes.ToDictionary(n => n.Id);
            var chain = new System.Collections.Generic.List<string>();
            for (var id = "page:Panthera leo"; id is not null; id = byId[id].ParentId) {
                chain.Add($"{byId[id].Rank} {byId[id].Name}");
            }
            Assert.Equal(new[] { "species Panthera leo", "subgenus Panthera (Leo)", "genus Panthera", "subfamily Pantherinae", "empire Biota" }, chain);
        } finally {
            SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-wal", "-shm" }) {
                try { File.Delete(path + suffix); } catch (IOException) { }
            }
        }
    }

    private static void Add(WikipediaCacheStore store, string title, string wikitext) {
        var now = new DateTime(2026, 10, 7, 0, 0, 0, DateTimeKind.Utc);
        var normalized = WikipediaTitleHelper.Normalize(title);
        var row = store.UpsertPageCandidate(new WikiPageCandidate(title, normalized, null, now, now)).PageRowId;
        store.SavePageContent(new WikiPageContent(row, null, title, normalized, null, false, null, false, false,
            false, null, null, wikitext, store.BeginImport("test"), now));
    }
}
