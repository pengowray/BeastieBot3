using System.IO.Compression;
using System.Text;
using BeastieBot3.StatusLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// `statuses france-import`: reading real rows of PatriNat's BDC Statuts (version 18), the parts of a
// red list remark, which red list row is current, the documents' years and titles, the TAXREF names
// and links, the citation forms, and the run with --file and --taxref-file over small zips built here.
public sealed class FranceStatusesTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-france-" + Guid.NewGuid().ToString("N"));

    public FranceStatusesTests() {
        Directory.CreateDirectory(_dir);
        File.WriteAllText(Path.Combine(_dir, "paths.ini"), "[Datastore]\n");
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try {
            Directory.Delete(_dir, recursive: true);
        } catch (IOException) {
        }
    }

    // ---- the BDC file ----

    private const string Header =
        "\"CD_NOM\",\"CD_REF\",\"CD_SUP\",\"CD_TYPE_STATUT\",\"LB_TYPE_STATUT\",\"REGROUPEMENT_TYPE\",\"CODE_STATUT\",\"LABEL_STATUT\",\"RQ_STATUT\",\"CD_SIG\",\"CD_DOC\",\"LB_NOM\",\"LB_AUTEUR\",\"NOM_COMPLET_HTML\",\"NOM_VALIDE_HTML\",\"REGNE\",\"PHYLUM\",\"CLASSE\",\"ORDRE\",\"FAMILLE\",\"GROUP1_INPN\",\"GROUP2_INPN\",\"LB_ADM_TR\",\"NIVEAU_ADMIN\",\"CD_ISO3166_1\",\"CD_ISO3166_2\",\"FULL_CITATION\",\"DOC_URL\",\"THEMATIQUE\",\"TYPE_VALUE\"";

    // Rows of bdc_18_01.csv (BDC version 18), in this order: IUCN's global red list (a type not
    // stored), the corncrake in the 2011 bird list (wintering and passage rows) and in the 2016 list
    // (breeding rows), the Sandwich tern in Guadeloupe (a category moved down one step for the region),
    // a Guadeloupe plant that is CR and possibly extinct (CR*), a pine and its nominate subspecies in
    // the vascular plant list (both under the accepted name Pinus mugo in TAXREF), the corncrake's
    // national protection, and the pine's national protection with a remark that is a note.
    private static readonly string[] Rows = [
        "838271,838271,838270,\"LRM\",\"Liste rouge mondiale\",\"Liste rouge\",\"DD\",\"Données insuffisantes\",\"\",\"WORLD\",,\"Mastigoteuthis psychrophila\",\"Nesis, 1977\",\"<i>Mastigoteuthis psychrophila</i> Nesis, 1977\",\"<i>Mastigoteuthis psychrophila</i> Nesis, 1977\",\"Animalia\",\"Mollusca\",\"Cephalopoda\",\"Oegopsida\",\"Mastigoteuthidae\",\"Mollusques\",\"Céphalopodes\",\"Monde\",\"\",\"\",\"\",\"\",\"\",\"STATUTS\",\"VALUE\"",
        "3053,3053,191254,\"LRN\",\"Liste rouge nationale\",\"Liste rouge\",\"NA\",\"Non applicable\",\"d - Visiteur\",\"TERFXFR\",31343,\"Crex crex\",\"(Linnaeus, 1758)\",\"<i>Crex crex</i> (Linnaeus, 1758)\",\"<i>Crex crex</i> (Linnaeus, 1758)\",\"Animalia\",\"Chordata\",\"Aves\",\"Gruiformes\",\"Rallidae\",\"Chordés\",\"Oiseaux\",\"France métropolitaine\",\"Territoire\",\"FXX\",\"\",\"UICN Comité français, MNHN, LPO, SEOF &amp; ONCFS. 2011. <em>La Liste rouge des espèces menacées en France - Chapitre Oiseaux de France métropolitaine.</em> Paris, France. 27 pp.\",\"http://inpn.mnhn.fr/docs/LR_FCE/Liste_rouge_France_Oiseaux_de_metropole.pdf\",\"STATUTS\",\"VALUE\"",
        "3053,3053,191254,\"LRN\",\"Liste rouge nationale\",\"Liste rouge\",\"EN\",\"En danger\",\"A2a C1 - Nicheur\",\"TERFXFR\",165208,\"Crex crex\",\"(Linnaeus, 1758)\",\"<i>Crex crex</i> (Linnaeus, 1758)\",\"<i>Crex crex</i> (Linnaeus, 1758)\",\"Animalia\",\"Chordata\",\"Aves\",\"Gruiformes\",\"Rallidae\",\"Chordés\",\"Oiseaux\",\"France métropolitaine\",\"Territoire\",\"FXX\",\"\",\"UICN Comité français, MNHN, LPO, SEOF &amp; ONCFS. 2016. <em>La Liste rouge des espèces menacées en France - Chapitre Oiseaux de France métropolitaine.</em> 31 pp.\",\"https://inpn.mnhn.fr/docs-web/docs/download/165208\",\"STATUTS\",\"VALUE\"",
        "3362,3362,198348,\"LRN\",\"Liste rouge nationale\",\"Liste rouge\",\"LC\",\"Préoccupation mineure\",\"NT (pr. D1) (-1) - Visiteur régulier\",\"TER971\",367849,\"Thalasseus sandvicensis\",\"(Latham, 1787)\",\"<i>Thalasseus sandvicensis</i> (Latham, 1787)\",\"<i>Thalasseus sandvicensis</i> (Latham, 1787)\",\"Animalia\",\"Chordata\",\"Aves\",\"Charadriiformes\",\"Laridae\",\"Chordés\",\"Oiseaux\",\"Guadeloupe\",\"Territoire\",\"GLP\",\"FR-GP\",\"UICN Comité français, OFB &amp; MNHN. 2021. <em>La Liste rouge des espèces menacées en France – Chapitres Faune de Guadeloupe. Fascicule</em>. UICN Comité français, OFB &amp; MNHN. Paris, France. 35 pp.\",\"https://inpn.mnhn.fr/docs-web/docs/download/367849\",\"STATUTS\",\"VALUE\"",
        "455727,455727,900068,\"LRN\",\"Liste rouge nationale\",\"Liste rouge\",\"CR*\",\"On ne sait pas si l'espèce n'est pas éteinte ou disparue\",\"B2ab(iii) D\",\"TER971\",300212,\"Annona mucosa\",\"Jacq., 1764\",\"<i>Annona mucosa</i> Jacq., 1764\",\"<i>Annona mucosa</i> Jacq., 1764\",\"Plantae\",\"\",\"Equisetopsida\",\"Magnoliales\",\"Annonaceae\",\"Trachéophytes\",\"Angiospermes\",\"Guadeloupe\",\"Territoire\",\"GLP\",\"FR-GP\",\"UICN Comité français, MNHN &amp; CBIG. 2019. <em>La Liste rouge des espèces menacées en France – Chapitre Flore vasculaire de Guadeloupe</em>. Paris, France. 19 pp.\",\"\",\"STATUTS\",\"VALUE\"",
        "161377,113682,,\"LRN\",\"Liste rouge nationale\",\"Liste rouge\",\"NT\",\"Quasi menacée\",\"pr. D2\",\"TERFXFR\",249369,\"Pinus mugo subsp. mugo\",\"Turra, 1764\",\"<i>Pinus mugo </i>Turra, 1764 subsp.<i> mugo</i>\",\"<i>Pinus mugo</i> Turra, 1764\",\"Plantae\",\"\",\"Equisetopsida\",\"Pinales\",\"Pinaceae\",\"Trachéophytes\",\"Gymnospermes\",\"France métropolitaine\",\"Territoire\",\"FXX\",\"\",\"UICN Comité français, FCBN, AFB &amp; MNHN. 2019. <em>La Liste rouge des espèces menacées en France - Chapitre Flore vasculaire de France métropolitaine. Fascicule.</em> UICN France, FCBN, AFB &amp; MNHN. Paris, France. 32 pp.\",\"https://inpn.mnhn.fr/docs-web/docs/download/249369\",\"STATUTS\",\"VALUE\"",
        "113682,113682,196293,\"LRN\",\"Liste rouge nationale\",\"Liste rouge\",\"LC\",\"Préoccupation mineure\",\"\",\"TERFXFR\",249369,\"Pinus mugo\",\"Turra, 1764\",\"<i>Pinus mugo</i> Turra, 1764\",\"<i>Pinus mugo</i> Turra, 1764\",\"Plantae\",\"\",\"Equisetopsida\",\"Pinales\",\"Pinaceae\",\"Trachéophytes\",\"Gymnospermes\",\"France métropolitaine\",\"Territoire\",\"FXX\",\"\",\"UICN Comité français, FCBN, AFB &amp; MNHN. 2019. <em>La Liste rouge des espèces menacées en France - Chapitre Flore vasculaire de France métropolitaine. Fascicule.</em> UICN France, FCBN, AFB &amp; MNHN. Paris, France. 32 pp.\",\"https://inpn.mnhn.fr/docs-web/docs/download/249369\",\"STATUTS\",\"VALUE\"",
        "3053,3053,191254,\"PN\",\"Protection nationale\",\"Protection\",\"NO3\",\"Liste des oiseaux protégés sur l'ensemble du territoire et les modalités de leur protection : Article 3\",\"\",\"TERFXFR\",713,\"Crex crex\",\"(Linnaeus, 1758)\",\"<i>Crex crex</i> (Linnaeus, 1758)\",\"<i>Crex crex</i> (Linnaeus, 1758)\",\"Animalia\",\"Chordata\",\"Aves\",\"Gruiformes\",\"Rallidae\",\"Chordés\",\"Oiseaux\",\"France métropolitaine\",\"Territoire\",\"FXX\",\"\",\"Arrêté interministériel du 29 octobre 2009 fixant la liste des oiseaux protégés sur l’ensemble du territoire et les modalités de leur protection (JORF 5 décembre 2009, p. 21056)\",\"http://legifrance.gouv.fr/affichTexte.do?cidTexte=JORFTEXT000021384277\",\"STATUTS\",\"VALUE\"",
        "113682,113682,196293,\"PN\",\"Protection nationale\",\"Protection\",\"NV1\",\"Liste des espèces végétales protégées sur l'ensemble du territoire français métropolitain : Article 1\",\"(spontané)  [Pinus mugo subsp. uncinata (Pin à crochets) volontairement supprimé car était à l'époque de l'arrêté considéré comme une espèce (J. Gourvil, comm. pers. 01/2015)]\",\"TERFXFR\",731,\"Pinus mugo\",\"Turra, 1764\",\"<i>Pinus mugo</i> Turra, 1764\",\"<i>Pinus mugo</i> Turra, 1764\",\"Plantae\",\"\",\"Equisetopsida\",\"Pinales\",\"Pinaceae\",\"Trachéophytes\",\"Gymnospermes\",\"France métropolitaine\",\"Territoire\",\"FXX\",\"\",\"Arrêté interministériel du 20 janvier 1982 relatif à la liste des espèces végétales protégées sur l'ensemble du territoire\",\"\",\"STATUTS\",\"VALUE\"",
    ];

    private static string BdcCsv() => Header + "\n" + string.Join("\n", Rows) + "\n";

    private static FranceBdcData ReadBdc() => FranceBdc.Read(new StringReader(BdcCsv()));

    [Fact]
    public void Stored_types_are_read_and_others_are_left_out() {
        var bdc = ReadBdc();
        Assert.Equal(9, bdc.RowsRead);
        Assert.Equal(8, bdc.Rows.Count);
        Assert.DoesNotContain(bdc.Rows, r => r.TypeCode == "LRM");
        // Every type is listed, the ones not stored too.
        Assert.Equal(["LRM:False", "LRN:True", "PN:True"], bdc.Types.Select(t => $"{t.Code}:{t.Stored}"));
        Assert.Equal(("Liste rouge nationale", "Liste rouge"), bdc.Types.Where(t => t.Code == "LRN").Select(t => (t.Label, t.Group)).Single());
        Assert.Equal(2, bdc.Rows.First().RowNumber);
    }

    [Fact]
    public void Bird_has_a_row_per_population() {
        var crex = ReadBdc().Rows.Where(r => r.Name == "Crex crex" && r.TypeCode == "LRN").ToList();
        Assert.Equal(2, crex.Count);
        var passage = crex[0];
        Assert.Equal((3053L, 3053L, "NA", "Non applicable", "TERFXFR", 31343L), (passage.CdNom, passage.CdRef, passage.Code, passage.Label, passage.TerritoryCode, passage.CdDoc));
        Assert.Equal(new FranceRemark("d - Visiteur", null, null, null, "d", "Visiteur", "visiting", false), passage.Remark);
        var breeding = crex[1];
        Assert.Equal(("EN", 165208L), (breeding.Code, breeding.CdDoc));
        Assert.Equal(new FranceRemark("A2a C1 - Nicheur", "A2a C1", null, null, null, "Nicheur", "breeding", false), breeding.Remark);
        Assert.Equal(("Animalia", "Chordata", "Aves", "Gruiformes", "Rallidae", "(Linnaeus, 1758)"),
            (breeding.Kingdom, breeding.Phylum, breeding.TaxClass, breeding.TaxOrder, breeding.Family, breeding.Author));
        // The 2016 list's breeding row does not replace the 2011 list's passage row.
        Assert.True(passage.IsCurrent);
        Assert.True(breeding.IsCurrent);
    }

    [Fact]
    public void Possibly_extinct_and_overseas_rows_are_read() {
        var rows = ReadBdc().Rows;
        var annona = rows.Single(r => r.Name == "Annona mucosa");
        Assert.Equal(("CR*", "TER971", "B2ab(iii) D", "Plantae", (string?)null), (annona.Code, annona.TerritoryCode, annona.Remark.Criteria, annona.Kingdom, annona.Phylum));
        var tern = rows.Single(r => r.Name == "Thalasseus sandvicensis");
        Assert.Equal(new FranceRemark("NT (pr. D1) (-1) - Visiteur régulier", "pr. D1", "NT", -1, null, "Visiteur régulier", "visiting", false), tern.Remark);
        Assert.Equal("LC", tern.Code);
    }

    [Fact]
    public void Places_and_documents_are_read_once() {
        var bdc = ReadBdc();
        Assert.Equal([
                new FranceTerritory("TER971", "Guadeloupe", "Guadeloupe", "Territoire", "GLP", "FR-GP"),
                new FranceTerritory("TERFXFR", "France métropolitaine", "Metropolitan France", "Territoire", "FXX", null),
            ], bdc.Territories);
        Assert.Equal([713L, 731L, 31343L, 165208L, 249369L, 300212L, 367849L], bdc.Documents.Select(d => d.CdDoc));
        var birds2016 = bdc.Documents.Single(d => d.CdDoc == 165208);
        Assert.Equal(2016, birds2016.Year);
        Assert.Equal("La Liste rouge des espèces menacées en France - Chapitre Oiseaux de France métropolitaine", birds2016.Title);
        Assert.Equal("UICN Comité français, MNHN, LPO, SEOF & ONCFS. 2016. La Liste rouge des espèces menacées en France - Chapitre Oiseaux de France métropolitaine. 31 pp.",
            birds2016.Citation);
        Assert.StartsWith("UICN Comité français, MNHN, LPO, SEOF &amp; ONCFS. 2016. <em>", birds2016.CitationHtml, StringComparison.Ordinal);
        Assert.Equal("https://inpn.mnhn.fr/docs-web/docs/download/165208", birds2016.Url);
        var decree = bdc.Documents.Single(d => d.CdDoc == 713);
        Assert.Equal((2009, (string?)null), (decree.Year, decree.Title));
        Assert.Null(bdc.Documents.Single(d => d.CdDoc == 300212).Url);
    }

    [Fact]
    public void Protection_rows_keep_no_remark_and_have_no_current_flag() {
        var protection = ReadBdc().Rows.Where(r => r.TypeCode == "PN").ToList();
        Assert.Equal(2, protection.Count);
        Assert.All(protection, r => Assert.Equal(FranceRemark.None, r.Remark));
        Assert.All(protection, r => Assert.Null(r.IsCurrent));
        Assert.Equal(("NO3", "Liste des oiseaux protégés sur l'ensemble du territoire et les modalités de leur protection : Article 3"),
            (protection[0].Code, protection[0].Label));
    }

    [Fact]
    public void Missing_column_is_an_error() {
        var csv = BdcCsv().Replace("\"RQ_STATUT\"", "\"REMARQUE\"", StringComparison.Ordinal);
        var ex = Assert.Throws<InvalidDataException>(() => FranceBdc.Read(new StringReader(csv)));
        Assert.Contains("RQ_STATUT", ex.Message, StringComparison.Ordinal);
    }

    // ---- remarks ----

    [Theory]
    [InlineData("", null, null, null, null, null, null)]
    [InlineData("/", null, null, null, null, null, null)]
    [InlineData("B2ab(iii,v) C2a(i)", "B2ab(iii,v) C2a(i)", null, null, null, null, null)]
    [InlineData("B1a,B1biii,B1bv,B2a,B2biii,B2bv", "B1a,B1biii,B1bv,B2a,B2biii,B2bv", null, null, null, null, null)]
    [InlineData("pr. B(1+2)b(iii)", "pr. B(1+2)b(iii)", null, null, null, null, null)]
    [InlineData("\"B1a,B1biii", "B1a,B1biii", null, null, null, null, null)]
    [InlineData("VU D1 (-1) - Nicheur", "D1", "VU", -1, null, "Nicheur", "breeding")]
    [InlineData("EN (B2ab(iii) D1) (+1)", "B2ab(iii) D1", "EN", 1, null, null, null)]
    [InlineData("CR (D) (-2) - Visiteur et possiblement nicheur", "D", "CR", -2, null, "Visiteur et possiblement nicheur", "visiting")]
    [InlineData("c - Hivernant", null, null, null, "c", "Hivernant", "wintering")]
    [InlineData("Reproducteur certain", null, null, null, null, "Reproducteur certain", "breeding")]
    [InlineData("Inconnu", null, null, null, null, "Inconnu", null)]
    [InlineData("pr. (D2 A2c)", null, null, null, null, null, null)]
    public void Remark_parts_are_read(string remark, string? criteria, string? adjustedFrom, int? adjustment, string? naReason,
        string? populationFr, string? population) {
        var code = naReason is null ? "VU" : "NA";
        var parts = FranceRedListRemark.Parse(code, remark);
        Assert.Equal((criteria, adjustedFrom, adjustment, naReason, populationFr, population),
            (parts.Criteria, parts.AdjustedFrom, parts.Adjustment, parts.NaReason, parts.PopulationFr, parts.Population));
        Assert.Equal(string.IsNullOrWhiteSpace(remark) ? null : remark, parts.Text);
    }

    [Fact]
    public void Letter_is_a_reason_only_for_NA() {
        Assert.Equal("a", FranceRedListRemark.Parse("NA", "a").NaReason);
        Assert.Null(FranceRedListRemark.Parse("LC", "a").NaReason);
    }

    [Fact]
    public void Sentence_is_not_stored_but_its_population_is() {
        var sentence = FranceRedListRemark.Parse("NA", "Espèce non connue dans la région au moment de l'évaluation");
        Assert.Equal((true, (string?)null, (string?)null), (sentence.IsSentence, sentence.Text, sentence.Criteria));
        var withPopulation = FranceRedListRemark.Parse("NA", "Espèce erratique non autochtone dans la région - Hivernant");
        Assert.Equal((true, (string?)null, "wintering"), (withPopulation.IsSentence, withPopulation.Text, withPopulation.Population));
    }

    // ---- the current row ----

    private static FranceStatusRow Row(long rowNumber, long cdNom, long cdRef, string code, long cdDoc, string? population = null,
        string type = "LRN", string territory = "TERFXFR") =>
        new(rowNumber, cdNom, cdRef, type, code, null, FranceRemark.None with { Population = population }, territory, $"name {cdNom}",
            null, null, null, null, null, null, cdDoc);

    private static Dictionary<long, bool?> Current(IEnumerable<FranceStatusRow> rows, Dictionary<long, int?> years) =>
        FranceBdc.MarkCurrent(rows.ToList(), years).ToDictionary(r => r.RowNumber, r => r.IsCurrent);

    [Fact]
    public void Newer_list_replaces_an_older_list_for_the_same_population() {
        var years = new Dictionary<long, int?> { [31343] = 2011, [165208] = 2016, [9] = null };
        var current = Current([
            Row(1, 3053, 3053, "VU", 31343, "breeding"),
            Row(2, 3053, 3053, "EN", 165208, "breeding"),
            Row(3, 3053, 3053, "NA", 31343, "wintering"),
            // A document with no year counts as older than any.
            Row(4, 3053, 3053, "LC", 9, "breeding"),
            // Another place.
            Row(5, 3053, 3053, "LC", 31343, "breeding", territory: "TER971"),
        ], years);
        Assert.Equal(new Dictionary<long, bool?> { [1] = false, [2] = true, [3] = true, [4] = false, [5] = true }, current);
    }

    [Fact]
    public void Row_of_the_accepted_name_wins_within_one_list() {
        var years = new Dictionary<long, int?> { [249369] = 2019 };
        var current = Current([Row(1, 161377, 113682, "NT", 249369), Row(2, 113682, 113682, "LC", 249369)], years);
        Assert.Equal(new Dictionary<long, bool?> { [1] = false, [2] = true }, current);

        // With the real rows: Pinus mugo subsp. mugo (NT) is a TAXREF synonym of Pinus mugo (LC).
        var pine = ReadBdc().Rows.Where(r => r.CdRef == 113682 && r.TypeCode == "LRN").ToDictionary(r => r.Name, r => r.IsCurrent);
        Assert.Equal(new Dictionary<string, bool?> { ["Pinus mugo subsp. mugo"] = false, ["Pinus mugo"] = true }, pine);
    }

    [Fact]
    public void Two_categories_in_one_list_both_stay_current() {
        // Sarcochilus hillii in the 2024 vascular plant list of New Caledonia: DD and VU.
        var years = new Dictionary<long, int?> { [452210] = 2024 };
        var current = Current([Row(1, 672295, 672295, "DD", 452210, territory: "TER988"), Row(2, 672295, 672295, "VU", 452210, territory: "TER988")], years);
        Assert.Equal(new Dictionary<long, bool?> { [1] = true, [2] = true }, current);
    }

    [Fact]
    public void Regional_and_national_lists_are_compared_apart_and_protection_has_no_flag() {
        var years = new Dictionary<long, int?> { [1] = 2010, [2] = 2020 };
        var current = Current([
            Row(1, 5, 5, "LC", 2, type: "LRN"),
            Row(2, 5, 5, "VU", 1, type: "LRR", territory: "INSEER42"),
            Row(3, 5, 5, "NO3", 1, type: "PN"),
        ], years);
        Assert.Equal(new Dictionary<long, bool?> { [1] = true, [2] = true, [3] = null }, current);
    }

    [Theory]
    [InlineData("Liste rouge des oiseaux nicheurs de Franche-Comté", "breeding")]
    [InlineData("Liste rouge des Oiseaux hivernants du Grand Est", "wintering")]
    [InlineData("Liste rouge régionale des oiseaux nicheurs, de passage et hivernants de Provence-Alpes-Côte d'Azur", null)]
    [InlineData("La Liste rouge des espèces menacées en France - Chapitre Oiseaux de France métropolitaine", null)]
    public void Bird_population_is_read_from_a_list_title(string title, string? population) =>
        Assert.Equal(population, FranceBdc.TitlePopulation(title));

    [Fact]
    public void Regional_breeding_bird_list_gives_its_birds_the_breeding_population() {
        var line = Rows[2]
            .Replace("\"LRN\",\"Liste rouge nationale\"", "\"LRR\",\"Liste rouge régionale\"", StringComparison.Ordinal)
            .Replace("\"A2a C1 - Nicheur\"", "\"\"", StringComparison.Ordinal)
            .Replace(",165208,", ",266649,", StringComparison.Ordinal)
            .Replace("<em>La Liste rouge des espèces menacées en France - Chapitre Oiseaux de France métropolitaine.</em>",
                "<em>Liste rouge des oiseaux nicheurs de Franche-Comté</em>", StringComparison.Ordinal);
        var bdc = FranceBdc.Read(new StringReader(Header + "\n" + line + "\n"));
        var row = Assert.Single(bdc.Rows);
        Assert.Equal(("breeding", (string?)null), (row.Remark.Population, row.Remark.PopulationFr));
        Assert.Equal(1, bdc.PopulationsFromTitles);
    }

    // ---- TAXREF ----

    // Cut down to the columns read (TAXREF's own files have 44 and 8). Crex crex (3053) with two
    // synonyms, Lynx lynx (60612), and a name whose accepted name is not wanted.
    private const string TaxrefNames =
        "\"REGNE\"\t\"CD_NOM\"\t\"CD_REF\"\t\"RANG\"\t\"LB_NOM\"\t\"LB_AUTEUR\"\r\n"
        + "\"Animalia\"\t3053\t3053\t\"ES\"\t\"Crex crex\"\t\"(Linnaeus, 1758)\"\r\n"
        + "\"Animalia\"\t3055\t3053\t\"ES\"\t\"Rallus crex\"\t\"Linnaeus, 1758\"\r\n"
        + "\"Animalia\"\t1053763\t3053\t\"ES\"\t\"Crex pratensis\"\t\"Bechstein, 1803\"\r\n"
        + "\"Animalia\"\t60612\t60612\t\"ES\"\t\"Lynx lynx\"\t\"(Linnaeus, 1758)\"\r\n"
        + "\"Animalia\"\t2504\t2504\t\"ES\"\t\"Ardea alba\"\t\"Linnaeus, 1758\"\r\n";

    // The corncrake's links in TAXREF_LIENS.txt (BirdLife, Catalogue of Life, GBIF, and Fauna
    // Europaea, which is not kept), the lynx's IUCN id, a link of a name not kept, and a row whose
    // title runs over two lines.
    private const string TaxrefLinks =
        "\"CT_NAME\"\t\"CT_TYPE\"\t\"CT_AUTHORS\"\t\"CT_TITLE\"\t\"CT_URL\"\t\"CD_NOM\"\t\"CT_SP_ID\"\t\"URL_SP\"\r\n"
        + "\"IUCN Red List > BirdLife\"\t\"GSD\"\t\"BirdLife International\"\t\"BirdLife International\"\t\"https://datazone.birdlife.org\"\t3053\t\"22692543\"\t\"http://datazone.birdlife.org/species/factsheet/22692543\"\r\n"
        + "\"Catalogue of Life\"\t\"CSD\"\t\t\"Catalogue of Life\"\t\"http://catalogueoflife.org/\"\t3053\t\"ZFFP\"\t\"https://www.catalogueoflife.org/data/taxon/ZFFP\"\r\n"
        + "\"Fauna Europaea\"\t\"RSD\"\t\t\"Fauna Europaea\"\t\"http://www.faunaeur.org/\"\t3053\t\"urn:lsid:faunaeur.org:taxname:96777\"\t\"\"\r\n"
        + "\"GBIF\"\t\"CSD\"\t\t\"Global Biodiversity\r\nInformation Facility\"\t\"https://www.gbif.org/\"\t3053\t\"4408498\"\t\"https://www.gbif.org/species/4408498\"\r\n"
        + "\"IUCN Red List\"\t\"TSD\"\t\t\"The IUCN Red List of Threatened Species\"\t\"http://www.iucnredlist.org/\"\t60612\t\"12519\"\t\"http://apiv3.iucnredlist.org/api/v3/taxonredirect/12519\"\r\n"
        + "\"IUCN Red List\"\t\"TSD\"\t\t\"The IUCN Red List of Threatened Species\"\t\"http://www.iucnredlist.org/\"\t99999\t\"1\"\t\"\"\r\n"
        + "\"IUCN Red List\"\t\"TSD\"\t\t\"The IUCN Red List of Threatened Species\"\t\"http://www.iucnredlist.org/\"\t60612\t\"12519\"\t\"\"\r\n";

    [Fact]
    public void Taxref_names_of_the_wanted_accepted_names_are_read() {
        var names = FranceTaxref.ReadNames(new StringReader(TaxrefNames), new HashSet<long> { 3053, 60612 });
        Assert.Equal([
                new TaxrefName(3053, 3053, "ES", "Crex crex", "(Linnaeus, 1758)"),
                new TaxrefName(3055, 3053, "ES", "Rallus crex", "Linnaeus, 1758"),
                new TaxrefName(1053763, 3053, "ES", "Crex pratensis", "Bechstein, 1803"),
                new TaxrefName(60612, 60612, "ES", "Lynx lynx", "(Linnaeus, 1758)"),
            ], names);
    }

    [Fact]
    public void Taxref_links_of_the_names_read_are_kept_once() {
        var accepted = new Dictionary<long, long> { [3053] = 3053, [3055] = 3053, [60612] = 60612 };
        var links = FranceTaxref.ReadLinks(new StringReader(TaxrefLinks), accepted);
        Assert.Equal([
                new TaxrefLink(3053, 3053, FranceTaxref.BirdLife, "22692543"),
                new TaxrefLink(3053, 3053, FranceTaxref.CatalogueOfLife, "ZFFP"),
                new TaxrefLink(3053, 3053, FranceTaxref.Gbif, "4408498"),
                new TaxrefLink(60612, 60612, FranceTaxref.IucnRedList, "12519"),
            ], links);
    }

    // ---- citations ----

    [Fact]
    public void Citations_follow_the_published_forms() {
        // TAXREF's form for version 18.0, as taxref.mnhn.fr gave it.
        Assert.Equal(
            "TAXREF [Eds] 2025. TAXREF v18.0, référentiel taxonomique pour la France. PatriNat (OFB-CNRS-MNHN-IRD), Muséum national d'Histoire naturelle, Paris. Archive de téléchargement contenant 8 fichiers générés le 9 janvier 2025. https://inpn.mnhn.fr/telechargement/referentielEspece/taxref/18.0/menu",
            FranceTaxref.Citation("18", new DateTime(2025, 1, 9), 8));
        // The BDC's form for version 17, as INPN gave it, then version 18.
        Assert.Equal(
            "Gargominy, O. & Régnier, C. 2024. Base de connaissance \"Statuts\" des espèces en France. Version pour TAXREF v17.0. PatriNat (OFB-MNHN-CNRS-IRD). Archive contenant deux fichiers. [version du 29 mai 2024]",
            FranceBdc.Citation("17", new DateTime(2024, 5, 29), 2));
        Assert.Equal(
            "Gargominy, O. & Régnier, C. 2025. Base de connaissance \"Statuts\" des espèces en France. Version pour TAXREF v18.0. PatriNat (OFB-MNHN-CNRS-IRD). Archive contenant trois fichiers. [version du 1er juillet 2025]",
            FranceBdc.Citation("18", new DateTime(2025, 7, 1), 3));
    }

    // ---- the run with --file and --taxref-file ----

    private string Zip(string name, params (string Entry, string Text, DateTime Date)[] entries) {
        var path = Path.Combine(_dir, name);
        using var zip = ZipFile.Open(path, ZipArchiveMode.Create);
        foreach (var (entry, text, date) in entries) {
            var e = zip.CreateEntry(entry);
            e.LastWriteTime = date;
            using var writer = new StreamWriter(e.Open(), new UTF8Encoding(false));
            writer.Write(text);
        }
        return path;
    }

    [Fact]
    public async Task Kept_zips_are_stored_with_both_source_rows() {
        var csvDate = new DateTime(2025, 7, 24, 18, 20, 0);
        var bdcZip = Zip("BDC-2025-11-28.zip",
            ("BDC_18/bdc_18_01.csv", BdcCsv(), csvDate),
            ("BDC_18/BDC_STATUTS_TYPES_18.csv", "\"CD_TYPE_STATUT\"\n\"LRN\"\n", csvDate),
            ("BDC_18/BDC_STATUTS_TYPES_18.xlsx", "x", csvDate));
        var taxrefDate = new DateTime(2025, 1, 9, 9, 24, 0);
        var taxrefZip = Zip("TAXREF_v18_2025-2025-11-28.zip",
            ("TAXREFv18.txt", TaxrefNames, taxrefDate),
            ("TAXREF_LIENS.txt", TaxrefLinks, taxrefDate),
            ("TAXREFv18.pdf", "pdf", taxrefDate));
        var storePath = Path.Combine(_dir, "status_lists.sqlite");

        var code = await StatusListImport.RunAsync(new FranceImportCommand.Settings {
            File = bdcZip, TaxrefFile = taxrefZip, StorePath = storePath, IniFile = Path.Combine(_dir, "paths.ini"), SettingsDir = _dir,
        }, FranceImportCommand.Spec(taxrefZip), CancellationToken.None);

        Assert.Equal(0, code);
        using var store = StatusListStore.OpenReadOnly(storePath)!;
        Assert.Equal(8, store.CountFranceStatuses());
        Assert.Equal(3, store.CountTaxrefNames());
        var sources = store.Sources().ToDictionary(s => s.Source);
        var bdc = sources[StatusSources.France];
        Assert.Equal((FranceBdc.DataGouvUrl, FranceBdc.Licence, "BDC-2025-11-28.zip", 8L), (bdc.Url, bdc.Licence, bdc.Version, bdc.RowCount));
        Assert.Equal(FranceBdc.Citation("18", csvDate, 3), bdc.Citation);
        var taxref = sources[StatusSources.Taxref];
        Assert.Equal((FranceTaxref.DataGouvUrl, "TAXREF_v18_2025-2025-11-28.zip", 3L), (taxref.Url, taxref.Version, taxref.RowCount));
        Assert.Equal(FranceTaxref.Citation("18", taxrefDate, 2), taxref.Citation);
    }

    [Fact]
    public async Task Bdc_file_that_is_not_a_zip_leaves_the_store_unchanged() {
        var notZip = Path.Combine(_dir, "BDC.zip");
        File.WriteAllText(notZip, "<html>moved</html>");
        var storePath = Path.Combine(_dir, "status_lists.sqlite");

        var code = await StatusListImport.RunAsync(new FranceImportCommand.Settings {
            File = notZip, TaxrefFile = notZip, StorePath = storePath, IniFile = Path.Combine(_dir, "paths.ini"), SettingsDir = _dir,
        }, FranceImportCommand.Spec(notZip), CancellationToken.None);

        Assert.Equal(1, code);
        Assert.False(File.Exists(storePath));
    }
}
