using System;
using System.Collections.Generic;
using System.Linq;

// How sure we are that a Wikidata item is the IUCN taxon, before anything is written to it.
//
// The tier starts from how the link was made (an IUCN id already on the item is strong; a name
// found by search is weaker; a synonym, a label or a local name match weaker still) and drops for
// every sign the pairing is wrong or ambiguous: the id on several items, a subspecies on a
// species-ranked item, an item that already carries a different current IUCN id. Flags record
// each reason so the report can say why a pair needs a person to look at it.
//
// Pure: the context holds every cross-taxon lookup, so FlowStepProbes-style tests pin the rules.

namespace BeastieBot3.WikidataEdits;

internal enum ConfidenceTier {
    /// IUCN id already on the item, unambiguous, name and rank agree. Edit.
    A,
    /// IUCN id on the item, but something differs (name, missing rank). Edit, flagged.
    B,
    /// Plausible but unconfirmed (exact name search, or an ambiguous id). Review before editing.
    C,
    /// Weak or contradictory. Review only; never edited without a person's decision.
    D,
}

internal enum LinkFlag {
    // lowers the tier
    NameDiffers,
    TaxonIdDeprecated,
    RankMissing,
    RankMismatch,
    NotTaxonItem,
    TaxonIdOnSeveralItems,
    ItemHasSeveralTaxonIds,
    ItemHasOtherCurrentTaxonId,
    ItemLinkedToSeveralTaxa,
    FoundByNameSearch,
    FoundBySynonym,
    FoundByCachedName,
    FoundByLabel,
    // informational: shown in the report, tier unchanged
    ItemHasRenumberedTaxonId,
    ReferenceCitesOtherTaxonId,
    PossiblyExtinct,
    LegacyCategory,
}

internal sealed record LinkClassification(ConfidenceTier Tier, IReadOnlyList<LinkFlag> Flags) {
    public bool Editable => Tier is ConfidenceTier.A or ConfidenceTier.B;
}

/// Cross-taxon facts the classifier needs, precomputed once per run.
internal sealed class LinkClassificationContext {
    public required IReadOnlySet<long> CurrentTaxonIds { get; init; }
    /// For each taxon id, the items carrying it as a non-deprecated P627 claim.
    public required IReadOnlyDictionary<long, int> ItemsPerTaxonIdClaim { get; init; }
    /// For each item, how many different IUCN taxa this run links to it (claims and searches).
    public required IReadOnlyDictionary<string, int> TaxaPerItem { get; init; }
}

internal static class TaxonLinkClassifier {
    public const string RankSpecies = "Q7432";
    public const string RankSubspecies = "Q68947";

    // P31 values that make an item a taxon for this purpose.
    private static readonly HashSet<string> TaxonClasses = new(StringComparer.Ordinal) {
        "Q16521",     // taxon
        "Q310890",    // monotypic taxon
        "Q98961713",  // extinct taxon
        "Q23038290",  // fossil taxon
    };

    public static LinkClassification Classify(
        IucnGlobalAssessment assessment,
        TaxonItemLink link,
        WdTaxonItem item,
        LinkClassificationContext context) {
        var flags = new List<LinkFlag>();
        var tier = link.Source switch {
            LinkSource.P627Claim => ConfidenceTier.A,
            LinkSource.SearchTaxonName => Lower(ConfidenceTier.A, ConfidenceTier.C, flags, LinkFlag.FoundByNameSearch),
            LinkSource.SearchTaxonNameSynonym => Lower(ConfidenceTier.A, ConfidenceTier.D, flags, LinkFlag.FoundBySynonym),
            LinkSource.CachedName => Lower(ConfidenceTier.A, ConfidenceTier.D, flags, LinkFlag.FoundByCachedName),
            LinkSource.Label => Lower(ConfidenceTier.A, ConfidenceTier.D, flags, LinkFlag.FoundByLabel),
            _ => ConfidenceTier.D,
        };

        var taxonId = assessment.TaxonId.ToString(System.Globalization.CultureInfo.InvariantCulture);

        if (item.InstanceOf.Count > 0 && !item.InstanceOf.Any(TaxonClasses.Contains)) {
            tier = Lower(tier, ConfidenceTier.D, flags, LinkFlag.NotTaxonItem);
        }

        if (!item.TaxonNames.Any(n => SameName(n, assessment.ScientificName))) {
            tier = Lower(tier, ConfidenceTier.B, flags, LinkFlag.NameDiffers);
        }

        var expectedRank = assessment.IsInfrarank ? RankSubspecies : RankSpecies;
        if (item.TaxonRanks.Count == 0) {
            tier = Lower(tier, ConfidenceTier.B, flags, LinkFlag.RankMissing);
        } else if (!item.TaxonRanks.Contains(expectedRank)) {
            tier = Lower(tier, ConfidenceTier.D, flags, LinkFlag.RankMismatch);
        }

        var itemIds = item.IucnTaxonIds
            .Where(s => s.Rank != "deprecated" && s.ValueString is not null)
            .Select(s => s.ValueString!.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();
        // The claim table behind LinkSource.P627Claim does not record rank: a deprecated P627 on
        // the item arrives as a claim link, though someone marked it wrong.
        if (link.Source == LinkSource.P627Claim && !itemIds.Contains(taxonId)) {
            tier = Lower(tier, ConfidenceTier.D, flags, LinkFlag.TaxonIdDeprecated);
        }

        var otherIds = itemIds.Where(v => v != taxonId).ToList();
        if (otherIds.Count > 0) {
            var otherCurrent = otherIds.Any(v => long.TryParse(v, out var id) && context.CurrentTaxonIds.Contains(id));
            if (otherCurrent) {
                tier = Lower(tier, ConfidenceTier.D, flags, LinkFlag.ItemHasOtherCurrentTaxonId);
            } else {
                flags.Add(LinkFlag.ItemHasRenumberedTaxonId);
            }
            if (itemIds.Contains(taxonId)) {
                tier = Lower(tier, ConfidenceTier.C, flags, LinkFlag.ItemHasSeveralTaxonIds);
            }
        }

        if (context.ItemsPerTaxonIdClaim.TryGetValue(assessment.TaxonId, out var holders) && holders > 1) {
            tier = Lower(tier, ConfidenceTier.C, flags, LinkFlag.TaxonIdOnSeveralItems);
        }

        if (context.TaxaPerItem.TryGetValue(item.Qid, out var taxa) && taxa > 1) {
            tier = Lower(tier, ConfidenceTier.D, flags, LinkFlag.ItemLinkedToSeveralTaxa);
        }

        // Informational only.
        var refIds = item.ConservationStatuses
            .Where(s => s.Rank != "deprecated")
            .SelectMany(s => s.References)
            .SelectMany(r => r.Values.TryGetValue("P627", out var v) ? v : Array.Empty<string>())
            .Select(v => v.Trim());
        if (refIds.Any(v => v != taxonId)) flags.Add(LinkFlag.ReferenceCitesOtherTaxonId);
        if (assessment.PossiblyExtinct || assessment.PossiblyExtinctInTheWild) flags.Add(LinkFlag.PossiblyExtinct);
        if (assessment.CategoryCode.StartsWith("LR", StringComparison.OrdinalIgnoreCase)) flags.Add(LinkFlag.LegacyCategory);

        return new LinkClassification(tier, flags);
    }

    // Moves the tier down to at least `floor` and records why. Never raises it.
    private static ConfidenceTier Lower(ConfidenceTier current, ConfidenceTier floor, List<LinkFlag> flags, LinkFlag flag) {
        flags.Add(flag);
        return (ConfidenceTier)Math.Max((int)current, (int)floor);
    }

    /// Compares an IUCN name with a Wikidata taxon name, ignoring case, spacing and the infraspecific
    /// rank word: IUCN writes "Alcelaphus buselaphus ssp. swaynei", zoological items
    /// "Alcelaphus buselaphus swaynei", botanical items "... subsp. ...".
    public static bool SameName(string wikidataName, string iucnName) =>
        string.Equals(Canonical(wikidataName), Canonical(iucnName), StringComparison.OrdinalIgnoreCase);

    private static readonly HashSet<string> RankWords = new(StringComparer.OrdinalIgnoreCase) {
        "ssp.", "ssp", "subsp.", "subsp", "var.", "var", "f.", "forma",
    };

    private static string Canonical(string name) =>
        string.Join(' ', name.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(p => !RankWords.Contains(p)));
}
