using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using BeastieBot3.WikidataEdits;
using Xunit;

namespace BeastieBot3.Tests;

// The dry run's two decisions: how sure we are an item is the IUCN taxon (tier), and what to change
// on it in each rank variant. Items are built by hand here so each rule has one clear case.
public class WikidataIucnPlannerTests {
    private const string VU = "Q278113";
    private const string EN = "Q96377276";
    private const string Edition2022 = "Q115962546";
    private const string Edition2026 = "Q999000001";

    private static readonly WikidataIucnEditConfig Config = new() { EditionItem = Edition2026 };

    private static IucnGlobalAssessment Assessment(string code = "EN", long taxonId = 31317, bool infrarank = false, string name = "Tapirus indicus") => new() {
        TaxonId = taxonId,
        AssessmentId = 45173922,
        ScientificName = name,
        IsInfrarank = infrarank,
        CategoryCode = code,
        YearPublished = 2026,
        Url = $"https://www.iucnredlist.org/species/{taxonId}/45173922",
        DownloadedAtUtc = new DateTime(2026, 8, 18, 3, 0, 0, DateTimeKind.Utc),
        Credits = new[] { new IucnCredit("assessor", "Traeholt, C.", 1), new IucnCredit("assessor", "Novarino, W.", 2), new IucnCredit("evaluator", "Cooke, R.", 1) },
    };

    private static WdReference Ref(params (string Property, string Value)[] values) => new(
        null,
        values.GroupBy(v => v.Property).ToDictionary(g => g.Key, g => (IReadOnlyList<string>)g.Select(v => v.Value).ToList()),
        new JsonObject { ["snaks"] = new JsonObject() });

    private static WdStatement Status(string id, string value, string rank, params WdReference[] refs) => new(
        id, "P141", rank, value, null,
        new Dictionary<string, IReadOnlyList<string>>(), refs,
        new JsonObject {
            ["id"] = id, ["rank"] = rank, ["type"] = "statement",
            ["mainsnak"] = WbEditPayloadBuilder.ItemSnak("P141", value),
            ["references"] = new JsonArray(refs.Select(r => (JsonNode)r.Raw.DeepClone()).ToArray()),
        });

    private static WdStatement TaxonId(string id, string value, string rank = "normal") => new(
        id, "P627", rank, null, value, new Dictionary<string, IReadOnlyList<string>>(), Array.Empty<WdReference>(),
        new JsonObject { ["id"] = id, ["rank"] = rank, ["mainsnak"] = WbEditPayloadBuilder.StringSnak("P627", value) });

    private static WdTaxonItem Item(IEnumerable<WdStatement>? statuses = null, IEnumerable<WdStatement>? ids = null,
        string name = "Tapirus indicus", string rank = TaxonLinkClassifier.RankSpecies) => new() {
        Qid = "Q24024",
        LastRevId = 2400000000,
        InstanceOf = new[] { "Q16521" },
        TaxonNames = new[] { name },
        TaxonRanks = new[] { rank },
        IucnTaxonIds = (ids ?? new[] { TaxonId("Q24024$id", "31317") }).ToList(),
        ConservationStatuses = (statuses ?? Array.Empty<WdStatement>()).ToList(),
    };

    private static LinkClassificationContext Context(long[]? current = null, Dictionary<long, int>? holders = null, Dictionary<string, int>? taxaPerItem = null) => new() {
        CurrentTaxonIds = new HashSet<long>(current ?? new long[] { 31317 }),
        ItemsPerTaxonIdClaim = holders ?? new Dictionary<long, int> { [31317] = 1 },
        TaxaPerItem = taxaPerItem ?? new Dictionary<string, int> { ["Q24024"] = 1 },
    };

    private static readonly HashSet<long> Current = new() { 31317 };

    private static LinkClassification Classify(WdTaxonItem item, LinkSource source = LinkSource.P627Claim, IucnGlobalAssessment? a = null, LinkClassificationContext? ctx = null) =>
        TaxonLinkClassifier.Classify(a ?? Assessment(), new TaxonItemLink(31317, item.Qid, source, null), item, ctx ?? Context());

    // ------------------------------------------------------------ tiers

    [Fact]
    public void An_unambiguous_id_claim_with_matching_name_and_rank_is_tier_A() {
        var c = Classify(Item());
        Assert.Equal(ConfidenceTier.A, c.Tier);
        Assert.Empty(c.Flags);
    }

    [Fact]
    public void A_different_name_on_an_id_claim_is_tier_B() {
        var c = Classify(Item(name: "Acrocodia indica"));
        Assert.Equal(ConfidenceTier.B, c.Tier);
        Assert.Contains(LinkFlag.NameDiffers, c.Flags);
    }

    [Fact]
    public void Infraspecific_rank_words_do_not_make_names_differ() {
        Assert.True(TaxonLinkClassifier.SameName("Alcelaphus buselaphus swaynei", "Alcelaphus buselaphus ssp. swaynei"));
        Assert.True(TaxonLinkClassifier.SameName("Abies nordmanniana subsp. equi-trojani", "Abies nordmanniana ssp. equi-trojani"));
    }

    [Fact]
    public void A_subspecies_on_a_species_ranked_item_is_tier_D() {
        var c = Classify(Item(name: "Polypterus senegalus senegalus"), a: Assessment(infrarank: true, name: "Polypterus senegalus ssp. senegalus"));
        Assert.Equal(ConfidenceTier.D, c.Tier);
        Assert.Contains(LinkFlag.RankMismatch, c.Flags);
    }

    [Fact]
    public void Search_links_start_at_C_or_D_by_how_they_were_found() {
        Assert.Equal(ConfidenceTier.C, Classify(Item(ids: Array.Empty<WdStatement>()), LinkSource.SearchTaxonName).Tier);
        Assert.Equal(ConfidenceTier.D, Classify(Item(ids: Array.Empty<WdStatement>()), LinkSource.SearchTaxonNameSynonym).Tier);
        Assert.Equal(ConfidenceTier.D, Classify(Item(ids: Array.Empty<WdStatement>()), LinkSource.Label).Tier);
    }

    [Fact]
    public void An_item_holding_another_current_taxon_id_is_tier_D_but_a_renumbered_one_only_flags() {
        var ids = new[] { TaxonId("Q24024$old", "14368") };
        var conflicting = Classify(Item(ids: ids), LinkSource.CachedName, ctx: Context(current: new long[] { 31317, 14368 }));
        Assert.Contains(LinkFlag.ItemHasOtherCurrentTaxonId, conflicting.Flags);

        var renumbered = Classify(Item(ids: ids), LinkSource.SearchTaxonName);
        Assert.Equal(ConfidenceTier.C, renumbered.Tier);
        Assert.Contains(LinkFlag.ItemHasRenumberedTaxonId, renumbered.Flags);
    }

    [Fact]
    public void A_deprecated_id_claim_is_tier_D() {
        var c = Classify(Item(ids: new[] { TaxonId("Q24024$id", "31317", rank: "deprecated") }));
        Assert.Equal(ConfidenceTier.D, c.Tier);
        Assert.Contains(LinkFlag.TaxonIdDeprecated, c.Flags);
    }

    [Fact]
    public void An_id_on_two_items_needs_review() {
        var c = Classify(Item(), ctx: Context(holders: new Dictionary<long, int> { [31317] = 2 }));
        Assert.Equal(ConfidenceTier.C, c.Tier);
        Assert.Contains(LinkFlag.TaxonIdOnSeveralItems, c.Flags);
    }

    // ------------------------------------------------------------ plans

    private static StatusEditPlan Plan(WdTaxonItem item, IucnGlobalAssessment? a = null, ExistingAssessmentItem? existing = null) =>
        IucnStatusEditPlanner.Plan(a ?? Assessment(), item, existing, Config, Current);

    [Fact]
    public void Agreeing_status_gets_both_references_and_no_new_statement() {
        var item = Item(new[] { Status("Q24024$s1", EN, "normal", Ref(("P248", Edition2022), ("P627", "31317"))) });
        var plan = Plan(item);
        Assert.Equal(PlanCategory.ReferencesOnly, plan.Category);
        Assert.False(plan.VariantsDiffer);
        var action = Assert.IsType<AddStatusReferences>(Assert.Single(plan.Actions[EditVariant.Preferred]));
        Assert.True(action.ReleaseReference);
        Assert.True(action.AssessmentReference);
        Assert.True(plan.CreatesAssessmentItem);
    }

    [Fact]
    public void Fully_referenced_agreeing_status_is_no_change() {
        var existing = new ExistingAssessmentItem { Qid = "Q56912428", AssessmentId = 45173922 };
        var item = Item(new[] { Status("Q24024$s1", EN, "normal",
            Ref(("P248", Edition2026), ("P627", "31317")), Ref(("P248", "Q56912428"))) });
        var plan = Plan(item, existing: existing);
        Assert.Equal(PlanCategory.NoChange, plan.Category);
        Assert.Empty(plan.Actions[EditVariant.Preferred]);
        Assert.False(plan.CreatesAssessmentItem);
    }

    [Fact]
    public void Changed_status_differs_by_variant() {
        var item = Item(new[] { Status("Q24024$s1", VU, "preferred", Ref(("P248", Edition2022), ("P627", "31317"))) });
        var plan = Plan(item);
        Assert.Equal(PlanCategory.StatusChanged, plan.Category);
        Assert.True(plan.VariantsDiffer);

        Assert.Equal(new PlannedAction[] { new SetStatusRank("Q24024$s1", "normal"), new AddStatusStatement(EN, "preferred") },
            plan.Actions[EditVariant.Preferred]);
        Assert.Equal(new PlannedAction[] { new ReplaceStatusValue("Q24024$s1", EN), new SetStatusRank("Q24024$s1", "normal") },
            plan.Actions[EditVariant.Replace]);
    }

    [Fact]
    public void A_status_that_changed_back_promotes_the_old_statement_instead_of_duplicating_it() {
        var item = Item(new[] {
            Status("Q24024$old", EN, "normal", Ref(("P248", Edition2022), ("P627", "31317"))),
            Status("Q24024$new", VU, "preferred"),
        });
        var actions = Plan(item).Actions[EditVariant.Preferred];
        Assert.Contains(new SetStatusRank("Q24024$new", "normal"), actions);
        Assert.Contains(new SetStatusRank("Q24024$old", "preferred"), actions);
        Assert.DoesNotContain(actions, a => a is AddStatusStatement);
    }

    [Fact]
    public void No_status_adds_one_and_a_missing_or_renumbered_id_is_fixed() {
        var item = Item(ids: new[] { TaxonId("Q24024$old", "14368") });
        var plan = Plan(item);
        Assert.Equal(PlanCategory.StatusAdded, plan.Category);
        Assert.Equal(new PlannedAction[] {
            new AddTaxonIdClaim(31317),
            new DeprecateTaxonIdClaim("Q24024$old", "14368"),
            new AddStatusStatement(EN, "normal"),
        }, plan.Actions[EditVariant.Preferred]);
    }

    [Fact]
    public void Unmapped_category_plans_nothing() {
        var plan = Plan(Item(), a: Assessment(code: "XX"));
        Assert.Equal(PlanCategory.UnmappedCategory, plan.Category);
        Assert.All(plan.Actions.Values, a => Assert.Empty(a));
    }

    // ------------------------------------------------------------ payloads

    [Fact]
    public void Preferred_payload_demotes_the_old_statement_and_adds_a_fully_referenced_new_one() {
        var item = Item(new[] { Status("Q24024$s1", VU, "preferred", Ref(("P248", Edition2022), ("P627", "31317"))) });
        var a = Assessment();
        var edit = WbEditPayloadBuilder.Build(item, a, Plan(item), EditVariant.Preferred, Config);

        Assert.Equal(2400000000, edit.BaseRevId);
        var claims = edit.Data["claims"]!.AsArray();
        Assert.Equal(2, claims.Count);
        Assert.Equal("Q24024$s1", (string)claims[0]!["id"]!);
        Assert.Equal("normal", (string)claims[0]!["rank"]!);

        var added = claims[1]!;
        Assert.Equal("preferred", (string)added["rank"]!);
        Assert.Equal(96377276, (long)added["mainsnak"]!["datavalue"]!["value"]!["numeric-id"]!);
        var refs = added["references"]!.AsArray();
        Assert.Equal(Edition2026, (string)refs[0]!["snaks"]!["P248"]![0]!["datavalue"]!["value"]!["id"]!);
        Assert.Equal("31317", (string)refs[0]!["snaks"]!["P627"]![0]!["datavalue"]!["value"]!);
        Assert.Equal(a.Url, (string)refs[0]!["snaks"]!["P854"]![0]!["datavalue"]!["value"]!);
        Assert.Equal("+2026-08-18T00:00:00Z", (string)refs[0]!["snaks"]!["P813"]![0]!["datavalue"]!["value"]!["time"]!);
        // The assessment item doesn't exist yet: a placeholder, with no numeric id.
        var assessmentSnak = refs[1]!["snaks"]!["P248"]![0]!["datavalue"]!["value"]!;
        Assert.Equal("CREATE:iucn-assessment:45173922", (string)assessmentSnak["id"]!);
        Assert.Null(assessmentSnak["numeric-id"]);
    }

    // Most planned edits are this one: the status already agrees and only needs citing.
    [Fact]
    public void References_only_payload_keeps_the_statement_and_its_old_reference() {
        var oldRef = Ref(("P248", Edition2022), ("P627", "31317"));
        oldRef.Raw["snaks"]!.AsObject()["P248"] = new JsonArray(WbEditPayloadBuilder.ItemSnak("P248", Edition2022));
        var item = Item(new[] { Status("Q24024$s1", EN, "normal", oldRef) });
        var edit = WbEditPayloadBuilder.Build(item, Assessment(), Plan(item), EditVariant.Preferred, Config);

        var claim = Assert.Single(edit.Data["claims"]!.AsArray())!;
        Assert.Equal("Q24024$s1", (string)claim["id"]!);
        Assert.Equal("normal", (string)claim["rank"]!);
        var refs = claim["references"]!.AsArray();
        Assert.Equal(3, refs.Count);
        Assert.Equal(115962546, (long)refs[0]!["snaks"]!["P248"]![0]!["datavalue"]!["value"]!["numeric-id"]!);
        Assert.Equal(Edition2026, (string)refs[1]!["snaks"]!["P248"]![0]!["datavalue"]!["value"]!["id"]!);
        var assessmentSnak = refs[2]!["snaks"]!["P248"]![0]!["datavalue"]!["value"]!;
        Assert.Equal("CREATE:iucn-assessment:45173922", (string)assessmentSnak["id"]!);
        Assert.Null(assessmentSnak["numeric-id"]);
    }

    [Fact]
    public void Payload_changes_a_copy_never_the_cached_statement() {
        var item = Item(new[] { Status("Q24024$s1", VU, "preferred") });
        WbEditPayloadBuilder.Build(item, Assessment(), Plan(item), EditVariant.Replace, Config);
        Assert.Equal("preferred", (string)item.ConservationStatuses[0].Raw["rank"]!);
        Assert.Equal(278113, (long)item.ConservationStatuses[0].Raw["mainsnak"]!["datavalue"]!["value"]!["numeric-id"]!);
    }

    [Fact]
    public void Assessment_item_lists_assessors_in_order_as_name_strings() {
        var payload = AssessmentItemPayloadBuilder.Build(Assessment(), "Q24024", Config);
        var authors = payload["claims"]!.AsArray()
            .Where(c => (string)c!["mainsnak"]!["property"]! == "P2093")
            .Select(c => ((string)c!["mainsnak"]!["datavalue"]!["value"]!, (string)c!["qualifiers"]!["P1545"]![0]!["datavalue"]!["value"]!))
            .ToList();
        Assert.Equal(new[] { ("Traeholt, C.", "1"), ("Novarino, W.", "2") }, authors);
        Assert.Equal("Tapirus indicus. The IUCN Red List of Threatened Species 2026: e.T31317A45173922",
            (string)payload["labels"]!["en"]!["value"]!);
    }
}
