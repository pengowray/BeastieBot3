using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// Pins DisplayPreferencesConfig layering: per-field null-coalescing Merge + ResolveAgainst the
// global defaults baseline. The old merger could only OR booleans *upward* (a base-true could
// never be turned off) and unconditionally clobbered the other half of the fields with the YAML
// deserializer's own defaults (an unset value could never inherit). These tests lock in the two
// capabilities that fixed: a later layer can both SET a field and UNSET-to-false over a base-true,
// and a field left null inherits cleanly.
public class DisplayPreferencesConfigTests {
    // The all-true defaults block from wikipedia-lists.yml (the three flags that differ from the
    // C# hard defaults of false), resolved as the global baseline. InfraspecificDisplayMode /
    // ListingStyle / ExcludeRegionalAssessments are not set in YAML, so they keep their C# defaults.
    private static DisplayPreferences GlobalDefaults() => new DisplayPreferences {
        PreferCommonNames = true,
        ItalicizeScientific = true,
        IncludeStatusTemplate = true,
        IncludeStatusLabel = true,
        GroupSubspecies = true,
        SeparateInfraspecificSections = true,
        IncludeFamilyInOtherBucket = true,
    };

    [Fact]
    public void Merge_NullSides_ReturnTheOtherLayer() {
        var cfg = new DisplayPreferencesConfig { GroupSubspecies = true };
        Assert.Same(cfg, DisplayPreferencesConfig.Merge(null, cfg));
        Assert.Same(cfg, DisplayPreferencesConfig.Merge(cfg, null));
        Assert.Null(DisplayPreferencesConfig.Merge(null, null));
    }

    [Fact]
    public void Merge_OverWinsPerField_AndUnderFallsThrough() {
        var under = new DisplayPreferencesConfig {
            ListingStyle = ListingStyle.ScientificNameFocus,
            GroupSubspecies = true,
            IncludeFamilyInOtherBucket = true,
        };
        var over = new DisplayPreferencesConfig {
            ListingStyle = ListingStyle.CommonNameOnly, // set -> wins
            GroupSubspecies = false,                    // explicitly false -> wins (impossible under the old OR merger)
            // IncludeFamilyInOtherBucket left null -> inherits `under`
        };

        var merged = DisplayPreferencesConfig.Merge(under, over)!;

        Assert.Equal(ListingStyle.CommonNameOnly, merged.ListingStyle);
        Assert.False(merged.GroupSubspecies);
        Assert.True(merged.IncludeFamilyInOtherBucket);
    }

    [Fact]
    public void ResolveAgainst_FillsUnsetFromDefaults_AndSetFieldsWin() {
        var cfg = new DisplayPreferencesConfig {
            ListingStyle = ListingStyle.CommonNameOnly,
            GroupSubspecies = false, // override a defaulted-true back off
        };

        var resolved = cfg.ResolveAgainst(GlobalDefaults());

        Assert.Equal(ListingStyle.CommonNameOnly, resolved.ListingStyle); // from cfg
        Assert.False(resolved.GroupSubspecies);                           // cfg wins over default-true
        Assert.True(resolved.SeparateInfraspecificSections);              // inherited from defaults
        Assert.True(resolved.IncludeFamilyInOtherBucket);                 // inherited from defaults
        Assert.Equal(InfraspecificDisplayMode.SeparateSections, resolved.InfraspecificDisplayMode); // C# default via defaults
    }

    [Fact]
    public void Layering_ReproducesMammalsCrChain() {
        // preset(cr)=none, taxa-group(mammals)={listing_style: CommonNameOnly}, list=none.
        // Expected resolved output == the pre-refactor behaviour: CommonNameOnly plus the three
        // defaulted-true flags carried through from the global defaults block.
        DisplayPreferencesConfig? preset = null;
        var taxaGroup = new DisplayPreferencesConfig { ListingStyle = ListingStyle.CommonNameOnly };
        DisplayPreferencesConfig? list = null;

        var merged = DisplayPreferencesConfig.Merge(DisplayPreferencesConfig.Merge(preset, taxaGroup), list);
        var resolved = merged!.ResolveAgainst(GlobalDefaults());

        Assert.Equal(ListingStyle.CommonNameOnly, resolved.ListingStyle);
        Assert.True(resolved.GroupSubspecies);
        Assert.True(resolved.SeparateInfraspecificSections);
        Assert.True(resolved.IncludeFamilyInOtherBucket);
        Assert.Equal(InfraspecificDisplayMode.SeparateSections, resolved.InfraspecificDisplayMode);
    }

    [Fact]
    public void Layering_PresetInfraModeSurvivesTaxaGroupListingStyle() {
        // preset(ex)={infraspecific_display_mode: GroupedUnderSpecies}, taxa-group(mammals)={CommonNameOnly}.
        // Both distinct keys must survive into the resolved prefs.
        var preset = new DisplayPreferencesConfig { InfraspecificDisplayMode = InfraspecificDisplayMode.GroupedUnderSpecies };
        var taxaGroup = new DisplayPreferencesConfig { ListingStyle = ListingStyle.CommonNameOnly };

        var merged = DisplayPreferencesConfig.Merge(DisplayPreferencesConfig.Merge(preset, taxaGroup), null);
        var resolved = merged!.ResolveAgainst(GlobalDefaults());

        Assert.Equal(ListingStyle.CommonNameOnly, resolved.ListingStyle);
        Assert.Equal(InfraspecificDisplayMode.GroupedUnderSpecies, resolved.InfraspecificDisplayMode);
    }

    [Fact]
    public void NullConfig_ResolvesToBareDefaults() {
        // The no-override path: generator does `definition.Display?.ResolveAgainst(...) ?? defaults`.
        DisplayPreferencesConfig? none = null;
        var resolved = none?.ResolveAgainst(GlobalDefaults()) ?? GlobalDefaults();
        Assert.True(resolved.GroupSubspecies);
        Assert.Equal(ListingStyle.CommonNameFocus, resolved.ListingStyle);
    }
}
