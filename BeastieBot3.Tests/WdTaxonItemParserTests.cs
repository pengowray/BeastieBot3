using System.Diagnostics;
using System.Text.Json.Nodes;
using BeastieBot3.WikidataEdits;
using Microsoft.Data.Sqlite;
using Xunit.Abstractions;

namespace BeastieBot3.Tests;

// Pins how cached Wikidata entity JSON becomes a WdTaxonItem for the IUCN status dry run. The
// fixtures at the bottom are real cached payloads (wbgetentities responses, downloaded Nov 2025)
// trimmed to the claims the parser reads; references were dropped from P31/P105/P171/P225 only.
public class WdTaxonItemParserTests {
    private static readonly DateTime Downloaded = new(2025, 11, 23, 3, 25, 16, DateTimeKind.Utc);

    private static WdTaxonItem ParseFixture(string json) {
        var item = WdTaxonItemParser.Parse(json, Downloaded);
        Assert.NotNull(item);
        return item!;
    }

    private static JsonObject FixtureStatement(string json, string qid, string property, int index) =>
        JsonNode.Parse(json)!["entities"]![qid]!["claims"]![property]![index]!.AsObject();

    [Fact]
    public void Parse_ReadsItemFields() {
        var item = ParseFixture(WdFixtures.Q24024);

        Assert.Equal("Q24024", item.Qid);
        Assert.Equal(2428761300, item.LastRevId);
        Assert.Equal(Downloaded, item.DownloadedAtUtc);
        Assert.Equal("Erythrina euodiphylla", item.LabelEn);
        Assert.Equal(new[] { "Q16521" }, item.InstanceOf);
        Assert.Equal(new[] { "Erythrina euodiphylla" }, item.TaxonNames);
        Assert.Equal(new[] { "Q7432" }, item.TaxonRanks);
        Assert.Equal(new[] { "Q1340386" }, item.ParentTaxa);
    }

    [Fact]
    public void Parse_KeepsBothRankedStatusStatements() {
        var item = ParseFixture(WdFixtures.Q24024);

        Assert.Equal(2, item.ConservationStatuses.Count);
        var normal = item.ConservationStatuses[0];
        var preferred = item.ConservationStatuses[1];

        // The id is kept verbatim, lower-case q and all: that is what wbeditentity matches.
        Assert.Equal("q24024$B909754A-CBF1-446E-A120-6BE29286DB74", normal.Id);
        Assert.Equal("P141", normal.Property);
        Assert.Equal("normal", normal.Rank);
        Assert.Equal("Q278113", normal.ValueId);
        Assert.Null(normal.ValueString);
        Assert.Empty(normal.Qualifiers);
        var normalRef = Assert.Single(normal.References);
        Assert.Equal("242d9b1722b5ea8d025be703d3c04e142fc328f4", normalRef.Hash);
        Assert.Equal(new[] { "Q115962546" }, normalRef.Values["P248"]);
        Assert.Equal(new[] { "31317" }, normalRef.Values["P627"]);
        Assert.Equal(new[] { "+2023-01-02T00:00:00Z" }, normalRef.Values["P813"]);

        Assert.Equal("Q24024$6721BE02-3D3B-44AB-A311-DA0F89DF2E2A", preferred.Id);
        Assert.Equal("preferred", preferred.Rank);
        Assert.Equal("Q96377276", preferred.ValueId);
        var preferredRef = Assert.Single(preferred.References);
        Assert.Equal(new[] { "Q136547248" }, preferredRef.Values["P248"]);
        Assert.Equal(new[] { "+2025-11-12T00:00:00Z" }, preferredRef.Values["P813"]);
        // Values follow snaks-order, which here puts P627 last.
        Assert.Equal(new[] { "P248", "P813", "P627" }, preferredRef.Values.Keys);

        var p627 = Assert.Single(item.IucnTaxonIds);
        Assert.Equal("31317", p627.ValueString);
        Assert.Null(p627.ValueId);
        Assert.Equal("normal", p627.Rank);
    }

    [Fact]
    public void Parse_RawIsAFaithfulDetachedCopy() {
        var item = ParseFixture(WdFixtures.Q24024);
        var statement = item.ConservationStatuses[1];
        var original = FixtureStatement(WdFixtures.Q24024, "Q24024", "P141", 1);

        // Read after Parse has disposed its document: the copy must stand on its own.
        var raw = JsonNode.Parse(statement.Raw.ToJsonString())!.AsObject();
        Assert.True(JsonNode.DeepEquals(original, raw));
        Assert.Equal(original.Select(p => p.Key), statement.Raw.Select(p => p.Key));
        Assert.Equal("13b31b2f926878fb09ec48aa2e8acd67930265ce", (string?)statement.Raw["references"]![0]!["hash"]);
        Assert.Equal(3, statement.Raw["references"]![0]!["snaks-order"]!.AsArray().Count);

        var reference = statement.References[0];
        Assert.True(JsonNode.DeepEquals(original["references"]![0], reference.Raw));
        Assert.Equal(new[] { "hash", "snaks", "snaks-order" }, reference.Raw.Select(p => p.Key));

        // A planner edits copies; the parsed item must not change underneath it.
        var copy = statement.Raw.DeepClone().AsObject();
        copy["rank"] = "deprecated";
        Assert.Equal("preferred", (string?)statement.Raw["rank"]);
    }

    [Fact]
    public void Parse_Subspecies() {
        var item = ParseFixture(WdFixtures.Q125932693);

        Assert.Equal(new[] { "Q68947" }, item.TaxonRanks);
        Assert.Equal(new[] { "Leontocebus weddelli weddelli" }, item.TaxonNames);
        var p627 = Assert.Single(item.IucnTaxonIds);
        Assert.Equal("43954", p627.ValueString);
        Assert.Empty(p627.References);

        var status = Assert.Single(item.ConservationStatuses);
        Assert.Equal("Q211005", status.ValueId);
        var reference = Assert.Single(status.References);
        Assert.Equal(new[] { "P627", "P813" }, reference.Values.Keys);
        Assert.Equal(new[] { "43954" }, reference.Values["P627"]);
    }

    [Fact]
    public void Parse_ItemWithTwoIucnTaxonIds() {
        var item = ParseFixture(WdFixtures.Q1519650);

        Assert.Equal(new[] { "15623273", "198751" }, item.IucnTaxonIds.Select(s => s.ValueString));
        Assert.Equal(new[] { "Trigloporus lastoviza", "Chelidonichthys lastoviza" }, item.TaxonNames);
        Assert.Equal(new[] { "Q18521131", "Q1983412" }, item.ParentTaxa);

        var status = Assert.Single(item.ConservationStatuses);
        Assert.Equal(2, status.References.Count);
        Assert.Equal(new[] { "15623273" }, status.References[0].Values["P627"]);
        Assert.Equal(new[] { "198751" }, status.References[1].Values["P627"]);
    }

    [Fact]
    public void Parse_StatementWithoutReferences() {
        var item = ParseFixture(WdFixtures.Q134055162);

        Assert.Null(item.LabelEn);
        Assert.Empty(item.IucnTaxonIds);
        var status = Assert.Single(item.ConservationStatuses);
        Assert.Equal("Q3245245", status.ValueId);
        Assert.Empty(status.References);
        Assert.Empty(status.Qualifiers);
        Assert.False(status.Raw.ContainsKey("references"));
    }

    [Fact]
    public void Parse_ReferenceCitingADifferentTaxonId() {
        var item = ParseFixture(WdFixtures.Q28319263);

        Assert.Equal("184213", Assert.Single(item.IucnTaxonIds).ValueString);
        var reference = Assert.Single(Assert.Single(item.ConservationStatuses).References);
        Assert.Equal(new[] { "173338417" }, reference.Values["P627"]);
    }

    [Fact]
    public void Parse_DeprecatedIucnTaxonIdKeepsRankQualifiersAndPropertyValues() {
        var item = ParseFixture(WdFixtures.Q137528);

        var p627 = Assert.Single(item.IucnTaxonIds);
        Assert.Equal("deprecated", p627.Rank);
        Assert.Equal("19823", p627.ValueString);
        Assert.Equal(new[] { "Q117259237" }, p627.Qualifiers["P2241"]);
        Assert.Equal(2, p627.References.Count);
        // P10551 has a property as its value.
        Assert.Equal(new[] { "P2241" }, p627.References[1].Values["P10551"]);
        Assert.Equal("CR", WikidataIucnStatusValues.CodeForQid(Assert.Single(item.ConservationStatuses).ValueId!));
    }

    [Fact]
    public void Parse_NoValueStatusStatement() {
        var item = ParseFixture(WdFixtures.Q215836);

        var status = Assert.Single(item.ConservationStatuses);
        Assert.Null(status.ValueId);
        Assert.Null(status.ValueString);
        Assert.Equal("P141", status.Property);
        Assert.Equal(new[] { "+2021-00-00T00:00:00Z" }, status.Qualifiers["P585"]);
        Assert.Equal(new[] { "Panthera tigris jacksoni" }, item.TaxonNames);
    }

    [Fact]
    public void Parse_FiltersDeprecatedAndNonValueTaxonNames() {
        const string json = """
            {"entities":{"Q1":{"type":"item","id":"Q1","lastrevid":5,"labels":{},"claims":{
            "P225":[
            {"mainsnak":{"snaktype":"value","property":"P225","datavalue":{"value":"Old name","type":"string"},"datatype":"string"},"type":"statement","id":"Q1$a","rank":"deprecated"},
            {"mainsnak":{"snaktype":"somevalue","property":"P225","datatype":"string"},"type":"statement","id":"Q1$b","rank":"normal"},
            {"mainsnak":{"snaktype":"value","property":"P225","datavalue":{"value":"Current name","type":"string"},"datatype":"string"},"type":"statement","id":"Q1$c","rank":"preferred"}
            ],
            "P31":[
            {"mainsnak":{"snaktype":"value","property":"P31","datavalue":{"value":{"entity-type":"item","numeric-id":16521},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q1$d","rank":"normal"}
            ]}}},"success":1}
            """;
        var item = ParseFixture(json);

        Assert.Equal(new[] { "Current name" }, item.TaxonNames);
        // An entity value with only numeric-id still renders as an id.
        Assert.Equal(new[] { "Q16521" }, item.InstanceOf);
    }

    [Fact]
    public void Parse_RendersOtherSnakTypesAsPlainStrings() {
        const string json = """
            {"entities":{"Q2":{"type":"item","id":"Q2","lastrevid":7,"claims":{
            "P141":[{"mainsnak":{"snaktype":"value","property":"P141","datavalue":{"value":{"entity-type":"item","numeric-id":211005,"id":"Q211005"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},
            "type":"statement","id":"Q2$x","rank":"normal",
            "references":[{"hash":"h1","snaks":{
              "P1476":[{"snaktype":"value","property":"P1476","datavalue":{"value":{"text":"Panthera leo","language":"en"},"type":"monolingualtext"},"datatype":"monolingualtext"}],
              "P854":[{"snaktype":"value","property":"P854","datavalue":{"value":"https://www.iucnredlist.org/species/15951/259030422","type":"string"},"datatype":"url"}],
              "P1104":[{"snaktype":"value","property":"P1104","datavalue":{"value":{"amount":"+12","unit":"1"},"type":"quantity"},"datatype":"quantity"}],
              "P577":[{"snaktype":"somevalue","property":"P577","datatype":"time"}]
            },"snaks-order":["P854","P1476","P1104","P577"]}]}]}}},"success":1}
            """;
        var reference = Assert.Single(Assert.Single(ParseFixture(json).ConservationStatuses).References);

        Assert.Equal(new[] { "P854", "P1476", "P1104", "P577" }, reference.Values.Keys);
        Assert.Equal(new[] { "https://www.iucnredlist.org/species/15951/259030422" }, reference.Values["P854"]);
        Assert.Equal(new[] { "Panthera leo" }, reference.Values["P1476"]);
        Assert.Equal(new[] { "+12" }, reference.Values["P1104"]);
        Assert.Equal(new[] { "somevalue" }, reference.Values["P577"]);
    }

    [Theory]
    [InlineData("""{"entities":{"Q404":{"id":"Q404","missing":""}},"success":1}""")]
    [InlineData("""{"entities":{"Q1":{"type":"item","id":"Q1","claims":{}}},"success":1}""")]
    [InlineData("""{"entities":{},"success":1}""")]
    [InlineData("""{"error":{"code":"no-such-entity"}}""")]
    [InlineData("")]
    public void Parse_ReturnsNullWithoutAUsableItem(string json) {
        Assert.Null(WdTaxonItemParser.Parse(json, null));
    }

    [Theory]
    [InlineData("Q24024", 24024L)]
    [InlineData(" q7 ", 7L)]
    public void TryParseQid_AcceptsItemIds(string qid, long expected) {
        Assert.True(WdTaxonItemReader.TryParseQid(qid, out var numeric));
        Assert.Equal(expected, numeric);
    }

    [Theory]
    [InlineData("P141")]
    [InlineData("Q")]
    [InlineData("Q-5")]
    [InlineData("24024")]
    [InlineData(null)]
    public void TryParseQid_RejectsEverythingElse(string? qid) {
        Assert.False(WdTaxonItemReader.TryParseQid(qid, out _));
    }
}

// The link and item readers over a throwaway SQLite file shaped like the cache's tables.
public sealed class WikidataCacheReaderTests : IDisposable {
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"wd-reader-{Guid.NewGuid():N}.sqlite");

    public WikidataCacheReaderTests() {
        using var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = _path, Pooling = false }.ConnectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE wikidata_entities (entity_numeric_id INTEGER PRIMARY KEY, entity_id TEXT NOT NULL,
                json_downloaded INTEGER NOT NULL DEFAULT 0, downloaded_at TEXT, json TEXT);
            CREATE TABLE wikidata_p627_values (entity_numeric_id INTEGER NOT NULL, source TEXT NOT NULL, value TEXT NOT NULL,
                PRIMARY KEY(entity_numeric_id, source, value));
            CREATE TABLE wikidata_pending_iucn_matches (iucn_taxon_id TEXT PRIMARY KEY, entity_numeric_id INTEGER NOT NULL,
                entity_id TEXT NOT NULL, matched_name TEXT, match_method TEXT NOT NULL, is_synonym INTEGER NOT NULL);
            INSERT INTO wikidata_p627_values VALUES
                (24024, 'claim', '31317'), (24024, 'reference', '31317'),
                (1519650, 'claim', '15623273'), (1519650, 'claim', '198751'),
                (1814604, 'claim', '40028/22064188');
            INSERT INTO wikidata_pending_iucn_matches VALUES
                ('100', 5, 'Q5', 'Aus bus', 'TaxonName', 0),
                ('101', 6, 'Q6', 'Aus cus', 'TaxonName', 1),
                ('31317', 24024, 'Q24024', 'Erythrina euodiphylla', 'CachedName', 0),
                ('102', 7, 'Q7', 'Dog', 'Label', 1),
                ('103', 8, 'Q8', NULL, 'Guess', 0);
            """;
        command.ExecuteNonQuery();

        using var insert = connection.CreateCommand();
        insert.CommandText = "INSERT INTO wikidata_entities VALUES (@id, @entity, @downloaded, @at, @json)";
        var id = insert.Parameters.Add("@id", SqliteType.Integer);
        var entity = insert.Parameters.Add("@entity", SqliteType.Text);
        var downloaded = insert.Parameters.Add("@downloaded", SqliteType.Integer);
        var at = insert.Parameters.Add("@at", SqliteType.Text);
        var json = insert.Parameters.Add("@json", SqliteType.Text);
        foreach (var (n, payload, isDownloaded) in new (long, string?, int)[] {
                     (24024, WdFixtures.Q24024, 1), (1519650, WdFixtures.Q1519650, 1),
                     (215836, WdFixtures.Q215836, 0), (999, "{not json", 1) }) {
            id.Value = n;
            entity.Value = "Q" + n;
            downloaded.Value = isDownloaded;
            at.Value = "2025-11-23T03:25:16.9814577Z";
            json.Value = (object?)payload ?? DBNull.Value;
            insert.ExecuteNonQuery();
        }
    }

    [Fact]
    public void LinkReader_ReturnsClaimsAndPendingMatchesWithoutResolvingThem() {
        using var reader = TaxonItemLinkReader.Open(_path);
        var set = reader.ReadAll();

        Assert.Contains(new TaxonItemLink(31317, "Q24024", LinkSource.P627Claim, null), set.Links);
        Assert.Contains(new TaxonItemLink(15623273, "Q1519650", LinkSource.P627Claim, null), set.Links);
        Assert.Contains(new TaxonItemLink(198751, "Q1519650", LinkSource.P627Claim, null), set.Links);
        // Same taxon, same item, second route: both kept.
        Assert.Contains(new TaxonItemLink(31317, "Q24024", LinkSource.CachedName, "Erythrina euodiphylla"), set.Links);
        Assert.Contains(new TaxonItemLink(100, "Q5", LinkSource.SearchTaxonName, "Aus bus"), set.Links);
        Assert.Contains(new TaxonItemLink(101, "Q6", LinkSource.SearchTaxonNameSynonym, "Aus cus"), set.Links);
        Assert.Contains(new TaxonItemLink(102, "Q7", LinkSource.Label, "Dog"), set.Links);
        Assert.Equal(7, set.Links.Count);

        Assert.Equal(2, set.Skipped.Count);
        Assert.Contains(set.Skipped, s => s.Qid == "Q1814604" && s.Value == "40028/22064188" && s.Source == "claim");
        Assert.Contains(set.Skipped, s => s.Qid == "Q8" && s.Source == "pending");

        var counts = set.CountBySource();
        Assert.Equal(3, counts[LinkSource.P627Claim]);
        Assert.Equal(1, counts[LinkSource.SearchTaxonName]);
        Assert.Equal(1, counts[LinkSource.SearchTaxonNameSynonym]);
        Assert.Equal(1, counts[LinkSource.CachedName]);
        Assert.Equal(1, counts[LinkSource.Label]);
    }

    [Fact]
    public void ItemReader_GetGetManyAndReadAll() {
        using var reader = WdTaxonItemReader.Open(_path);

        var one = reader.Get("Q24024");
        Assert.NotNull(one);
        Assert.Equal(new DateTime(2025, 11, 23, 3, 25, 16, DateTimeKind.Utc).AddTicks(9814577), one!.DownloadedAtUtc);
        Assert.Equal(DateTimeKind.Utc, one.DownloadedAtUtc!.Value.Kind);
        Assert.Null(reader.Get("Q215836")); // not downloaded
        Assert.Null(reader.Get("nonsense"));

        var many = reader.GetMany(new[] { "Q24024", "q1519650", "Q215836", "Q404", "junk", "Q24024" });
        Assert.Equal(new[] { "Q1519650", "Q24024" }, many.Keys.Order());

        var all = reader.ReadAll().Select(i => i.Qid).ToList();
        Assert.Equal(new[] { "Q24024", "Q1519650" }, all);
        Assert.Equal(1, reader.SkippedRows); // the "{not json" row
        Assert.Equal(3, reader.CountDownloaded());
    }

    public void Dispose() {
        try {
            File.Delete(_path);
        }
        catch (IOException) {
        }
    }
}

public class TaxonItemLinkSourceTests {
    [Theory]
    [InlineData("TaxonName", false, nameof(LinkSource.SearchTaxonName))]
    [InlineData("TaxonName", true, nameof(LinkSource.SearchTaxonNameSynonym))]
    [InlineData("CachedName", false, nameof(LinkSource.CachedName))]
    [InlineData("CachedName", true, nameof(LinkSource.CachedName))]
    [InlineData("Label", false, nameof(LinkSource.Label))]
    [InlineData("Label", true, nameof(LinkSource.Label))]
    [InlineData("taxonname", false, nameof(LinkSource.SearchTaxonName))]
    public void SourceForPendingMatch_MapsBackfillMethods(string method, bool isSynonym, string expected) {
        // LinkSource is internal, so the expectation travels as its name.
        Assert.Equal(Enum.Parse<LinkSource>(expected), TaxonItemLinkReader.SourceForPendingMatch(method, isSynonym));
    }

    [Theory]
    [InlineData("Guess")]
    [InlineData("")]
    [InlineData(null)]
    public void SourceForPendingMatch_UnknownMethodIsNull(string? method) {
        Assert.Null(TaxonItemLinkReader.SourceForPendingMatch(method, false));
    }

    [Theory]
    [InlineData("31317", true)]
    [InlineData("200035703", true)]
    [InlineData("40028/22064188", false)]
    [InlineData("78426641/", false)]
    [InlineData("-5", false)]
    [InlineData("0", false)]
    [InlineData("", false)]
    public void TryParseTaxonId_OnlyPlainNumbers(string value, bool expected) {
        Assert.Equal(expected, TaxonItemLinkReader.TryParseTaxonId(value, out _));
    }
}

public class WikidataIucnStatusValuesTests {
    [Theory]
    [InlineData("EX", "Q237350")]
    [InlineData("EW", "Q239509")]
    [InlineData("CR", "Q219127")]
    [InlineData("EN", "Q96377276")]
    [InlineData("VU", "Q278113")]
    [InlineData("NT", "Q719675")]
    [InlineData("LC", "Q211005")]
    [InlineData("DD", "Q3245245")]
    [InlineData("NE", "Q3350324")]
    public void CurrentCodes_RoundTrip(string code, string qid) {
        Assert.Equal(qid, WikidataIucnStatusValues.QidForCode(code));
        Assert.Equal(code, WikidataIucnStatusValues.CodeForQid(qid));
        Assert.True(WikidataIucnStatusValues.IsAllowedValue(qid));
    }

    [Fact]
    public void EveryAllowedValueRoundTrips() {
        Assert.Equal(9, WikidataIucnStatusValues.AllowedValues.Count);
        foreach (var value in WikidataIucnStatusValues.AllowedValues) {
            Assert.Equal(value.Qid, WikidataIucnStatusValues.QidForCode(value.IucnCode!));
            Assert.Equal(value.IucnCode, WikidataIucnStatusValues.CodeForQid(value.Qid));
        }
    }

    [Theory]
    [InlineData("LR/nt", "Q719675", "NT")]
    [InlineData("LR/lc", "Q211005", "LC")]
    [InlineData("lr/NT", "Q719675", "NT")]
    [InlineData(" LR/LC ", "Q211005", "LC")]
    public void LowerRiskCodes_FoldIntoTheirSuccessors(string code, string qid, string readBack) {
        Assert.True(WikidataIucnStatusValues.IsLowerRiskCode(code));
        Assert.Equal(qid, WikidataIucnStatusValues.QidForCode(code));
        // Not a round trip: the item says NT/LC, and that is what reads back.
        Assert.Equal(readBack, WikidataIucnStatusValues.CodeForQid(qid));
    }

    [Fact]
    public void ConservationDependent_HasNoAllowedValue() {
        Assert.Null(WikidataIucnStatusValues.QidForCode("LR/cd"));
        Assert.True(WikidataIucnStatusValues.IsLowerRiskCode("LR/cd"));

        Assert.False(WikidataIucnStatusValues.IsAllowedValue(WikidataIucnStatusValues.ConservationDependentQid));
        Assert.Null(WikidataIucnStatusValues.CodeForQid(WikidataIucnStatusValues.ConservationDependentQid));
        Assert.Equal("LR/cd", WikidataIucnStatusValues.Describe(WikidataIucnStatusValues.ConservationDependentQid)!.IucnCode);
    }

    [Theory]
    [InlineData("Q6693756")]   // Lower Risk
    [InlineData("Q85304919")]  // possibly extinct in the wild
    [InlineData("Q56660246")]  // Czech Red List "endangered"
    public void KnownDisallowedValues_AreDescribedButNotMapped(string qid) {
        Assert.False(WikidataIucnStatusValues.IsAllowedValue(qid));
        Assert.Null(WikidataIucnStatusValues.CodeForQid(qid));
        Assert.NotNull(WikidataIucnStatusValues.Describe(qid));
    }

    [Theory]
    [InlineData("cr", "Q219127")]
    [InlineData(" En ", "Q96377276")]
    public void QidForCode_IsCaseInsensitive(string code, string qid) {
        Assert.Equal(qid, WikidataIucnStatusValues.QidForCode(code));
    }

    [Theory]
    [InlineData("CR(PE)")]
    [InlineData("RE")]
    [InlineData("NA")]
    [InlineData("LR")]
    [InlineData("Critically Endangered")]
    [InlineData("")]
    public void QidForCode_NullWithoutAnAllowedValue(string code) {
        Assert.Null(WikidataIucnStatusValues.QidForCode(code));
    }

    [Theory]
    [InlineData("Q404")]
    [InlineData("")]
    public void CodeForQid_UnknownIsNull(string qid) {
        Assert.Null(WikidataIucnStatusValues.CodeForQid(qid));
        Assert.Null(WikidataIucnStatusValues.Describe(qid));
    }
}

// Streams the real cache when BEASTIEBOT_WIKIDATA_CACHE points at it; otherwise returns at once.
// It reads only (Mode=ReadOnly) and exists to time a full pass and sanity-check the counts.
public class WikidataCacheFullPassTests {
    private readonly ITestOutputHelper _output;

    public WikidataCacheFullPassTests(ITestOutputHelper output) {
        _output = output;
    }

    [Fact]
    public void ReadAll_StreamsTheWholeCache() {
        var path = Environment.GetEnvironmentVariable("BEASTIEBOT_WIKIDATA_CACHE");
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path)) {
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        using var links = TaxonItemLinkReader.Open(path);
        var set = links.ReadAll();
        _output.WriteLine($"links: {set.Links.Count} in {stopwatch.Elapsed.TotalSeconds:F1}s; skipped {set.Skipped.Count}");
        foreach (var (source, count) in set.CountBySource()) {
            _output.WriteLine($"  {source}: {count}");
        }

        foreach (var skipped in set.Skipped) {
            _output.WriteLine($"  skipped {skipped.Source} {skipped.Qid} '{skipped.Value}'");
        }

        stopwatch.Restart();
        using var reader = WdTaxonItemReader.Open(path);
        int items = 0, statuses = 0, taxonIds = 0, references = 0;
        foreach (var item in reader.ReadAll()) {
            items++;
            statuses += item.ConservationStatuses.Count;
            taxonIds += item.IucnTaxonIds.Count;
            foreach (var statement in item.ConservationStatuses) {
                references += statement.References.Count;
            }
        }

        _output.WriteLine($"items: {items} in {stopwatch.Elapsed.TotalSeconds:F1}s; skipped rows {reader.SkippedRows}; " +
                          $"P141 statements {statuses} ({references} references); P627 statements {taxonIds}");
        Assert.True(items > 0);
    }
}

internal static class WdFixtures {
    internal const string Q24024 = """
        {"entities":{"Q24024":{"type":"item","id":"Q24024","lastrevid":2428761300,"modified":"2025-11-12T19:31:42Z",
        "labels":{"en":{"language":"en","value":"Erythrina euodiphylla"}},
        "claims":{
        "P31":[
        {"mainsnak":{"snaktype":"value","property":"P31","hash":"06629d890d7ab0ff85c403d8aadf57ce9809c01f","datavalue":{"value":{"entity-type":"item","numeric-id":16521,"id":"Q16521"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q24024$1EE22A5B-106E-496D-BC39-A71C3DE7FB3E","rank":"normal"}
        ],
        "P105":[
        {"mainsnak":{"snaktype":"value","property":"P105","hash":"aebf3611b23ed90c7c0fc80f6cd1cb7be110ea59","datavalue":{"value":{"entity-type":"item","numeric-id":7432,"id":"Q7432"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q24024$639670E5-5116-49E9-9CDF-10CD221EE58E","rank":"normal"}
        ],
        "P171":[
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"9e2f7d833e8d90a58a3e88e127a213ac7911b5d4","datavalue":{"value":{"entity-type":"item","numeric-id":1340386,"id":"Q1340386"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q24024$E2F32CE7-83B7-4E1F-B441-1FB85E8C0556","rank":"normal"}
        ],
        "P225":[
        {"mainsnak":{"snaktype":"value","property":"P225","hash":"a21c54a5f759004a5f9ce13decbb363deb9e1746","datavalue":{"value":"Erythrina euodiphylla","type":"string"},"datatype":"string"},"type":"statement","qualifiers":{"P405":[{"snaktype":"value","property":"P405","hash":"a14843fa9e3f9a17d93adc20162f903c0aea6dd4","datavalue":{"value":{"entity-type":"item","numeric-id":68616,"id":"Q68616"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P574":[{"snaktype":"value","property":"P574","hash":"1377e744f0e368b1aa738339f850079dfb80b192","datavalue":{"value":{"time":"+1858-00-00T00:00:00Z","timezone":0,"before":0,"after":0,"precision":9,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}],"P697":[{"snaktype":"value","property":"P697","hash":"8dc31cd8636c4f75cced9e5bd24b523aa23c9c34","datavalue":{"value":{"entity-type":"item","numeric-id":2587046,"id":"Q2587046"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"qualifiers-order":["P405","P574","P697"],"id":"q24024$256323D7-C50F-4E08-86CF-31DBB70892CF","rank":"normal"}
        ],
        "P627":[
        {"mainsnak":{"snaktype":"value","property":"P627","hash":"7b19b76df2a491909e8942e1009a93829083840e","datavalue":{"value":"31317","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q24024$C168C040-E5EB-4CED-ACDC-DBAF35AEA54B","rank":"normal","references":[{"hash":"182efbdb9110d036ca433f3b49bd3a1ae312858b","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"ba14d022d7e0c8b74595e7b8aaa1bc2451dd806a","datavalue":{"value":{"entity-type":"item","numeric-id":32059,"id":"Q32059"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","hash":"8c1c5174f4811115ea8a0def725fdc074c2ef036","datavalue":{"value":{"time":"+2016-07-10T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}
        ],
        "P141":[
        {"mainsnak":{"snaktype":"value","property":"P141","hash":"80026ea5b2066a2538fee5c0897b459bb6770689","datavalue":{"value":{"entity-type":"item","numeric-id":278113,"id":"Q278113"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q24024$B909754A-CBF1-446E-A120-6BE29286DB74","rank":"normal","references":[{"hash":"242d9b1722b5ea8d025be703d3c04e142fc328f4","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"4bf80ee227bb9925a1d31430fab2d8be4248a071","datavalue":{"value":{"entity-type":"item","numeric-id":115962546,"id":"Q115962546"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P627":[{"snaktype":"value","property":"P627","hash":"7b19b76df2a491909e8942e1009a93829083840e","datavalue":{"value":"31317","type":"string"},"datatype":"external-id"}],"P813":[{"snaktype":"value","property":"P813","hash":"2dee56994fe37443d6330343aa7e15a3aee57c92","datavalue":{"value":{"time":"+2023-01-02T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P627","P813"]}]},
        {"mainsnak":{"snaktype":"value","property":"P141","hash":"af95854211f8592bb7a7090fbd3569c2ad885252","datavalue":{"value":{"entity-type":"item","numeric-id":96377276,"id":"Q96377276"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q24024$6721BE02-3D3B-44AB-A311-DA0F89DF2E2A","rank":"preferred","references":[{"hash":"13b31b2f926878fb09ec48aa2e8acd67930265ce","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"96bf7eb91770f83b1c0a874ed8875467db0b3578","datavalue":{"value":{"entity-type":"item","numeric-id":136547248,"id":"Q136547248"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","hash":"d37abfc8bc7ff9e740f6f4cfb7d2f86ab8cc2bc9","datavalue":{"value":{"time":"+2025-11-12T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}],"P627":[{"snaktype":"value","property":"P627","hash":"7b19b76df2a491909e8942e1009a93829083840e","datavalue":{"value":"31317","type":"string"},"datatype":"external-id"}]},"snaks-order":["P248","P813","P627"]}]}
        ]
        }}},"success":1}
        """;

    internal const string Q125932693 = """
        {"entities":{"Q125932693":{"type":"item","id":"Q125932693","lastrevid":2365811489,"modified":"2025-06-22T18:54:13Z",
        "labels":{"en":{"language":"en","value":"Leontocebus weddelli weddelli"}},
        "claims":{
        "P31":[
        {"mainsnak":{"snaktype":"value","property":"P31","hash":"06629d890d7ab0ff85c403d8aadf57ce9809c01f","datavalue":{"value":{"entity-type":"item","numeric-id":16521,"id":"Q16521"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q125932693$b84a0528-425f-d7bd-e62c-fc80752ad8b9","rank":"normal"}
        ],
        "P105":[
        {"mainsnak":{"snaktype":"value","property":"P105","hash":"e44184dde557014194a9ee8a713c46b37258a927","datavalue":{"value":{"entity-type":"item","numeric-id":68947,"id":"Q68947"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q125932693$9f25b58e-4d8f-fbd9-aee7-9799d9ea928d","rank":"normal"}
        ],
        "P171":[
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"4bdb8a1008dee1885001575f3ddcda4611c2454b","datavalue":{"value":{"entity-type":"item","numeric-id":22675245,"id":"Q22675245"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q125932693$b1efe3d5-49df-e4e5-c2ed-e28d2e0938f7","rank":"normal"}
        ],
        "P225":[
        {"mainsnak":{"snaktype":"value","property":"P225","hash":"16bbd2d5cd393f121c6648aa982bcc26c4477094","datavalue":{"value":"Leontocebus weddelli weddelli","type":"string"},"datatype":"string"},"type":"statement","id":"Q125932693$74048db0-48a1-590f-f6cf-a30991a3ef6d","rank":"normal"}
        ],
        "P627":[
        {"mainsnak":{"snaktype":"value","property":"P627","hash":"a6648a2fe6559badbdfc5fedd50b3ccb384a8fdf","datavalue":{"value":"43954","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q125932693$87ab9340-4908-327b-306c-5dc5b1704744","rank":"normal"}
        ],
        "P141":[
        {"mainsnak":{"snaktype":"value","property":"P141","hash":"f6158175530e6b445bf4165a2355180396d6765a","datavalue":{"value":{"entity-type":"item","numeric-id":211005,"id":"Q211005"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q125932693$3f41a647-4e3d-b7f4-b216-09b4a30e92d5","rank":"normal","references":[{"hash":"c311508edfc42175980e66087cd3f82fef91e91d","snaks":{"P627":[{"snaktype":"value","property":"P627","hash":"a6648a2fe6559badbdfc5fedd50b3ccb384a8fdf","datavalue":{"value":"43954","type":"string"},"datatype":"external-id"}],"P813":[{"snaktype":"value","property":"P813","hash":"2d024887b4f2ee69513e0be2b658ae9e75372e6e","datavalue":{"value":{"time":"+2024-05-15T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P627","P813"]}]}
        ]
        }}},"success":1}
        """;

    internal const string Q1519650 = """
        {"entities":{"Q1519650":{"type":"item","id":"Q1519650","lastrevid":2403496738,"modified":"2025-09-11T09:16:17Z",
        "labels":{"en":{"language":"en","value":"Trigloporus lastoviza"}},
        "claims":{
        "P31":[
        {"mainsnak":{"snaktype":"value","property":"P31","hash":"06629d890d7ab0ff85c403d8aadf57ce9809c01f","datavalue":{"value":{"entity-type":"item","numeric-id":16521,"id":"Q16521"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q1519650$DF6545EA-056C-41CD-A8D4-156C79E2F544","rank":"normal"}
        ],
        "P105":[
        {"mainsnak":{"snaktype":"value","property":"P105","hash":"aebf3611b23ed90c7c0fc80f6cd1cb7be110ea59","datavalue":{"value":{"entity-type":"item","numeric-id":7432,"id":"Q7432"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q1519650$BE413BDE-47EA-42FA-BF90-E35B9730994A","rank":"normal"}
        ],
        "P171":[
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"05eebdd60ff36e8454a482916284bc568336c17d","datavalue":{"value":{"entity-type":"item","numeric-id":18521131,"id":"Q18521131"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q1519650$59F19AD9-499D-4AED-97AD-A9FCCD214930","rank":"normal"},
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"01d2f8313edfee42ba5eca2e5ae1b95a0d53cca7","datavalue":{"value":{"entity-type":"item","numeric-id":1983412,"id":"Q1983412"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q1519650$6F5C125F-FF43-4CD5-8133-FAE234EC8190","rank":"normal"}
        ],
        "P225":[
        {"mainsnak":{"snaktype":"value","property":"P225","hash":"9205694ae4fcf78a2676926dac2ff5a9624d99c5","datavalue":{"value":"Trigloporus lastoviza","type":"string"},"datatype":"string"},"type":"statement","qualifiers":{"P405":[{"snaktype":"value","property":"P405","hash":"9fede5baa98a9854e23050275b0ed960622bd777","datavalue":{"value":{"entity-type":"item","numeric-id":128989,"id":"Q128989"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P574":[{"snaktype":"value","property":"P574","hash":"91d90a0cfa0e3282bd3fa45a8caeab8ae04b26d5","datavalue":{"value":{"time":"+1788-00-00T00:00:00Z","timezone":0,"before":0,"after":0,"precision":9,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}],"P3831":[{"snaktype":"value","property":"P3831","hash":"b52a1e9a7233e55d3b29cf35e56ed21446f7f457","datavalue":{"value":{"entity-type":"item","numeric-id":14594740,"id":"Q14594740"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"qualifiers-order":["P405","P574","P3831"],"id":"q1519650$9791A73A-9C1E-451E-A09F-BBC1A778B654","rank":"normal"},
        {"mainsnak":{"snaktype":"value","property":"P225","hash":"5e2e065194c3731e9f49c02c0c02b0a3120724f5","datavalue":{"value":"Chelidonichthys lastoviza","type":"string"},"datatype":"string"},"type":"statement","id":"Q1519650$CE98721C-E558-4524-91FC-35829D6438A1","rank":"normal"}
        ],
        "P627":[
        {"mainsnak":{"snaktype":"value","property":"P627","hash":"48b4a000f7b83dd3b23cc4e57cade6ba4ae7caa0","datavalue":{"value":"15623273","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q1519650$C84F2C06-A12A-4306-9745-84296C89CA79","rank":"normal","references":[{"hash":"6f8626cb03bb3ca6ee0a5c2ece82d7e62dc83598","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"ba14d022d7e0c8b74595e7b8aaa1bc2451dd806a","datavalue":{"value":{"entity-type":"item","numeric-id":32059,"id":"Q32059"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","hash":"f0cde5a6da346a8a887f7acbb8cb3969e389856f","datavalue":{"value":{"time":"+2016-07-11T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]},
        {"mainsnak":{"snaktype":"value","property":"P627","hash":"a261b48f502e423644f544a7488ea1bb6e2d8d63","datavalue":{"value":"198751","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q1519650$5DC5B980-F175-4923-94A5-1D7C7F586BED","rank":"normal","references":[{"hash":"543b9d1679332b1e10adc538753c8551ebd408ea","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"ba14d022d7e0c8b74595e7b8aaa1bc2451dd806a","datavalue":{"value":{"entity-type":"item","numeric-id":32059,"id":"Q32059"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","hash":"d0ff7740c2ee82665282a8af13cffdf7e3d78279","datavalue":{"value":{"time":"+2017-12-21T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}
        ],
        "P141":[
        {"mainsnak":{"snaktype":"value","property":"P141","hash":"f6158175530e6b445bf4165a2355180396d6765a","datavalue":{"value":{"entity-type":"item","numeric-id":211005,"id":"Q211005"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q1519650$0595B730-B3B4-4479-8250-9A153E90250C","rank":"normal","references":[{"hash":"b72a5914366198fa05bced95b263a67bbb8cca3b","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"4bf80ee227bb9925a1d31430fab2d8be4248a071","datavalue":{"value":{"entity-type":"item","numeric-id":115962546,"id":"Q115962546"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P627":[{"snaktype":"value","property":"P627","hash":"48b4a000f7b83dd3b23cc4e57cade6ba4ae7caa0","datavalue":{"value":"15623273","type":"string"},"datatype":"external-id"}],"P813":[{"snaktype":"value","property":"P813","hash":"8f779ab36be5789cb54d1ce1ee8ac477f570ce5b","datavalue":{"value":{"time":"+2023-01-03T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P627","P813"]},{"hash":"d4a8cbe2778ce8f82f55e0d5b5546caf790a1985","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"4bf80ee227bb9925a1d31430fab2d8be4248a071","datavalue":{"value":{"entity-type":"item","numeric-id":115962546,"id":"Q115962546"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P627":[{"snaktype":"value","property":"P627","hash":"a261b48f502e423644f544a7488ea1bb6e2d8d63","datavalue":{"value":"198751","type":"string"},"datatype":"external-id"}],"P813":[{"snaktype":"value","property":"P813","hash":"8f779ab36be5789cb54d1ce1ee8ac477f570ce5b","datavalue":{"value":{"time":"+2023-01-03T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P627","P813"]}]}
        ]
        }}},"success":1}
        """;

    internal const string Q134055162 = """
        {"entities":{"Q134055162":{"type":"item","id":"Q134055162","lastrevid":2340953235,"modified":"2025-04-23T10:10:01Z",
        "labels":{},
        "claims":{
        "P31":[
        {"mainsnak":{"snaktype":"value","property":"P31","hash":"06629d890d7ab0ff85c403d8aadf57ce9809c01f","datavalue":{"value":{"entity-type":"item","numeric-id":16521,"id":"Q16521"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q134055162$72ad7ecd-4e7a-1962-e3a0-571e396fb2a0","rank":"normal"}
        ],
        "P105":[
        {"mainsnak":{"snaktype":"value","property":"P105","hash":"aebf3611b23ed90c7c0fc80f6cd1cb7be110ea59","datavalue":{"value":{"entity-type":"item","numeric-id":7432,"id":"Q7432"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q134055162$d74ed1a6-4da6-1d36-97d4-6b14ef419c7b","rank":"normal"}
        ],
        "P171":[
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"a3101722607320443dca70b15d9f24ec9a5c7b2a","datavalue":{"value":{"entity-type":"item","numeric-id":74941,"id":"Q74941"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q134055162$972acb7d-4b66-3b67-426f-c670461ce3d6","rank":"normal"}
        ],
        "P225":[
        {"mainsnak":{"snaktype":"value","property":"P225","hash":"8eb800ec73b0f414c1e6d2e0b405aff82b9e450a","datavalue":{"value":"Nycticebus cayan","type":"string"},"datatype":"string"},"type":"statement","id":"Q134055162$2126ba3e-4fde-8144-1590-160c11daa5fa","rank":"normal"}
        ],
        "P141":[
        {"mainsnak":{"snaktype":"value","property":"P141","hash":"d33f757f748e08b119eaa5d3637ec30b130ccf0b","datavalue":{"value":{"entity-type":"item","numeric-id":3245245,"id":"Q3245245"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q134055162$0beeb7bd-4dba-aba1-3cba-788315d7ab9b","rank":"normal"}
        ]
        }}},"success":1}
        """;

    internal const string Q28319263 = """
        {"entities":{"Q28319263":{"type":"item","id":"Q28319263","lastrevid":2359998325,"modified":"2025-06-12T14:49:06Z",
        "labels":{"en":{"language":"en","value":"Mastigogomphus chapini"}},
        "claims":{
        "P31":[
        {"mainsnak":{"snaktype":"value","property":"P31","hash":"06629d890d7ab0ff85c403d8aadf57ce9809c01f","datavalue":{"value":{"entity-type":"item","numeric-id":16521,"id":"Q16521"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q28319263$fc998ed6-420b-12e1-4ad0-ff91c07bebc1","rank":"normal"}
        ],
        "P105":[
        {"mainsnak":{"snaktype":"value","property":"P105","hash":"aebf3611b23ed90c7c0fc80f6cd1cb7be110ea59","datavalue":{"value":{"entity-type":"item","numeric-id":7432,"id":"Q7432"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q28319263$d5d381a0-40e8-ad87-8aef-e2818952a6af","rank":"normal"}
        ],
        "P171":[
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"066515aca3238f17e30f75bd66a84f52eef1ba23","datavalue":{"value":{"entity-type":"item","numeric-id":28318834,"id":"Q28318834"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q28319263$c0dacd2e-42cd-c758-e277-cf31bd3323d8","rank":"normal"}
        ],
        "P225":[
        {"mainsnak":{"snaktype":"value","property":"P225","hash":"1093c1398e6f8152c258ba130b768892c437dcb7","datavalue":{"value":"Mastigogomphus chapini","type":"string"},"datatype":"string"},"type":"statement","id":"Q28319263$66cd655a-4065-433c-e96a-fc362c80dcf0","rank":"normal"}
        ],
        "P627":[
        {"mainsnak":{"snaktype":"value","property":"P627","hash":"ffe2f493e6bc0961c984c9495ee13b32a6ab93c4","datavalue":{"value":"184213","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q28319263$250A9DB8-A0D2-4EF8-8CC6-5CD36FC3A553","rank":"normal","references":[{"hash":"3e74549127853b4fd2e816c5d33f2c7fc6bd12b7","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"ba14d022d7e0c8b74595e7b8aaa1bc2451dd806a","datavalue":{"value":{"entity-type":"item","numeric-id":32059,"id":"Q32059"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","hash":"08beed27e93f007094c23c3ddf150d85f16499b2","datavalue":{"value":{"time":"+2017-01-14T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}
        ],
        "P141":[
        {"mainsnak":{"snaktype":"value","property":"P141","hash":"f6158175530e6b445bf4165a2355180396d6765a","datavalue":{"value":{"entity-type":"item","numeric-id":211005,"id":"Q211005"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q28319263$409E5AE8-C04E-40A4-A245-98D694F05BC5","rank":"normal","references":[{"hash":"6177a3eb62f298cf184c4cc24609f6006697c2ec","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"4bf80ee227bb9925a1d31430fab2d8be4248a071","datavalue":{"value":{"entity-type":"item","numeric-id":115962546,"id":"Q115962546"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P627":[{"snaktype":"value","property":"P627","hash":"159fa488ccec3214ac7d0da286fcdcd7d096555b","datavalue":{"value":"173338417","type":"string"},"datatype":"external-id"}],"P813":[{"snaktype":"value","property":"P813","hash":"43d10596f7e11eaca13e261241c4676dfcb8232c","datavalue":{"value":{"time":"+2023-01-04T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P627","P813"]}]}
        ]
        }}},"success":1}
        """;

    internal const string Q137528 = """
        {"entities":{"Q137528":{"type":"item","id":"Q137528","lastrevid":2414023665,"modified":"2025-10-08T11:24:03Z",
        "labels":{"en":{"language":"en","value":"cotton-top tamarin"}},
        "claims":{
        "P31":[
        {"mainsnak":{"snaktype":"value","property":"P31","hash":"06629d890d7ab0ff85c403d8aadf57ce9809c01f","datavalue":{"value":{"entity-type":"item","numeric-id":16521,"id":"Q16521"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q137528$1D0FF288-E347-4CBB-9562-8C3AAD1A01FB","rank":"normal"}
        ],
        "P105":[
        {"mainsnak":{"snaktype":"value","property":"P105","hash":"aebf3611b23ed90c7c0fc80f6cd1cb7be110ea59","datavalue":{"value":{"entity-type":"item","numeric-id":7432,"id":"Q7432"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q137528$EDD28C05-DF9D-41D0-A447-A1ECAC2B58CB","rank":"normal"}
        ],
        "P171":[
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"7c59559b765cf7c4f4db4599c8e520c36852a41a","datavalue":{"value":{"entity-type":"item","numeric-id":240034,"id":"Q240034"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q137528$20B7E8AC-2CA7-4342-9564-38E9CC531CED","rank":"normal"}
        ],
        "P225":[
        {"mainsnak":{"snaktype":"value","property":"P225","hash":"ad355c534c07fbbb78b0add07358fd57767184af","datavalue":{"value":"Saguinus oedipus","type":"string"},"datatype":"string"},"type":"statement","qualifiers":{"P405":[{"snaktype":"value","property":"P405","hash":"a817d3670bc2f9a3586b6377a65d54fff72ef888","datavalue":{"value":{"entity-type":"item","numeric-id":1043,"id":"Q1043"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P574":[{"snaktype":"value","property":"P574","hash":"506af9838b7d37b45786395b95170263f1951a31","datavalue":{"value":{"time":"+1758-01-01T00:00:00Z","timezone":0,"before":0,"after":0,"precision":9,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}],"P3831":[{"snaktype":"value","property":"P3831","hash":"b52a1e9a7233e55d3b29cf35e56ed21446f7f457","datavalue":{"value":{"entity-type":"item","numeric-id":14594740,"id":"Q14594740"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"qualifiers-order":["P405","P574","P3831"],"id":"q137528$66437422-48AC-44CB-962B-CE5EF27864CA","rank":"normal"}
        ],
        "P627":[
        {"mainsnak":{"snaktype":"value","property":"P627","hash":"31741e6dccc608c7a579acbeefa78bf6d33a9d2d","datavalue":{"value":"19823","type":"string"},"datatype":"external-id"},"type":"statement","qualifiers":{"P2241":[{"snaktype":"value","property":"P2241","hash":"50a58873847e117ebd3e754bb3fb40f4f367f73e","datavalue":{"value":{"entity-type":"item","numeric-id":117259237,"id":"Q117259237"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}]},"qualifiers-order":["P2241"],"id":"Q137528$27B6EB15-0717-4F20-B4E7-CC1EBB440A93","rank":"deprecated","references":[{"hash":"6f8626cb03bb3ca6ee0a5c2ece82d7e62dc83598","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"ba14d022d7e0c8b74595e7b8aaa1bc2451dd806a","datavalue":{"value":{"entity-type":"item","numeric-id":32059,"id":"Q32059"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","hash":"f0cde5a6da346a8a887f7acbb8cb3969e389856f","datavalue":{"value":{"time":"+2016-07-11T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]},{"hash":"e843ae055b33c9c4eaa84cda1c2b1616368aebc9","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"ba14d022d7e0c8b74595e7b8aaa1bc2451dd806a","datavalue":{"value":{"entity-type":"item","numeric-id":32059,"id":"Q32059"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","hash":"bf86b56efc6578f9f8197015052c9ff832217746","datavalue":{"value":{"time":"+2025-10-08T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}],"P10551":[{"snaktype":"value","property":"P10551","hash":"44b00cfe05d44b364ed72bfd490c73261161e6b2","datavalue":{"value":{"entity-type":"property","numeric-id":2241,"id":"P2241"},"type":"wikibase-entityid"},"datatype":"wikibase-property"}]},"snaks-order":["P248","P813","P10551"]}]}
        ],
        "P141":[
        {"mainsnak":{"snaktype":"value","property":"P141","hash":"9c34b29bf4b1067e0ce6ecdfd83be8d3cfc4ec0f","datavalue":{"value":{"entity-type":"item","numeric-id":219127,"id":"Q219127"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"q137528$E33A9E9F-965D-4580-AABC-08F4BCAE5F11","rank":"normal","references":[{"hash":"87f07d4893a2e591f0cbb95d18abdaa2ccc5c2c2","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"4bf80ee227bb9925a1d31430fab2d8be4248a071","datavalue":{"value":{"entity-type":"item","numeric-id":115962546,"id":"Q115962546"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P627":[{"snaktype":"value","property":"P627","hash":"31741e6dccc608c7a579acbeefa78bf6d33a9d2d","datavalue":{"value":"19823","type":"string"},"datatype":"external-id"}],"P813":[{"snaktype":"value","property":"P813","hash":"2dee56994fe37443d6330343aa7e15a3aee57c92","datavalue":{"value":{"time":"+2023-01-02T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P627","P813"]}]}
        ]
        }}},"success":1}
        """;

    internal const string Q215836 = """
        {"entities":{"Q215836":{"type":"item","id":"Q215836","lastrevid":2411842231,"modified":"2025-10-02T12:35:37Z",
        "labels":{"en":{"language":"en","value":"Malayan tiger"}},
        "claims":{
        "P31":[
        {"mainsnak":{"snaktype":"value","property":"P31","hash":"06629d890d7ab0ff85c403d8aadf57ce9809c01f","datavalue":{"value":{"entity-type":"item","numeric-id":16521,"id":"Q16521"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q215836$CBAA05ED-80A5-4442-A89C-81F71D5B6290","rank":"normal"}
        ],
        "P105":[
        {"mainsnak":{"snaktype":"value","property":"P105","hash":"e44184dde557014194a9ee8a713c46b37258a927","datavalue":{"value":{"entity-type":"item","numeric-id":68947,"id":"Q68947"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q215836$52e527ab-4064-e406-671c-c57a6e18f17f","rank":"normal"}
        ],
        "P171":[
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"c2e543d04106eca53f0022b1a484c7f83900eaa5","datavalue":{"value":{"entity-type":"item","numeric-id":19939,"id":"Q19939"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q215836$1459cc81-443a-b3e0-95d7-d4c6011f2a14","rank":"normal"},
        {"mainsnak":{"snaktype":"value","property":"P171","hash":"58213b00ecbdb76edb9e72f46cea77736217b2ba","datavalue":{"value":{"entity-type":"item","numeric-id":55621783,"id":"Q55621783"},"type":"wikibase-entityid"},"datatype":"wikibase-item"},"type":"statement","id":"Q215836$56EDFF93-839F-47B4-B143-4230FBE765FB","rank":"normal"}
        ],
        "P225":[
        {"mainsnak":{"snaktype":"value","property":"P225","hash":"bd11f181d812e13d1128261af55166cb26c0befa","datavalue":{"value":"Panthera tigris jacksoni","type":"string"},"datatype":"string"},"type":"statement","id":"Q215836$8186e1d4-46ea-f827-dcc0-f994e5a5366a","rank":"normal"}
        ],
        "P627":[
        {"mainsnak":{"snaktype":"value","property":"P627","hash":"ee8facdd847aebf4103895c09544a53bd9722142","datavalue":{"value":"136893","type":"string"},"datatype":"external-id"},"type":"statement","id":"Q215836$8D1401D9-BFBC-42C3-AB36-6E9C87E44746","rank":"normal","references":[{"hash":"182efbdb9110d036ca433f3b49bd3a1ae312858b","snaks":{"P248":[{"snaktype":"value","property":"P248","hash":"ba14d022d7e0c8b74595e7b8aaa1bc2451dd806a","datavalue":{"value":{"entity-type":"item","numeric-id":32059,"id":"Q32059"},"type":"wikibase-entityid"},"datatype":"wikibase-item"}],"P813":[{"snaktype":"value","property":"P813","hash":"8c1c5174f4811115ea8a0def725fdc074c2ef036","datavalue":{"value":{"time":"+2016-07-10T00:00:00Z","timezone":0,"before":0,"after":0,"precision":11,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"snaks-order":["P248","P813"]}]}
        ],
        "P141":[
        {"mainsnak":{"snaktype":"novalue","property":"P141","hash":"1504de72a169664aac5ca037d7661f41df65cbf6","datatype":"wikibase-item"},"type":"statement","qualifiers":{"P585":[{"snaktype":"value","property":"P585","hash":"4a6fe7f861358928efef462e361b21704446d129","datavalue":{"value":{"time":"+2021-00-00T00:00:00Z","timezone":0,"before":0,"after":0,"precision":9,"calendarmodel":"http://www.wikidata.org/entity/Q1985727"},"type":"time"},"datatype":"time"}]},"qualifiers-order":["P585"],"id":"Q215836$928334c1-490b-b8ae-3b3c-6a11a20cb902","rank":"normal"}
        ]
        }}},"success":1}
        """;
}
