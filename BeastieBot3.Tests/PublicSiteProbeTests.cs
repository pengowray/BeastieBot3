using System;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Web.Flows;
using Xunit;

namespace BeastieBot3.Tests;

// Lights for the "Update the public species site" workflow. What each light has to get right:
// the GBIF step compares the release of the newest checklist zip with the IUCN Red List database
// (an older checklist gives no DOI to any assessment new in the release), and the build step says
// why the site database is out of date, naming the inputs that changed after it was built.
public class PublicSiteProbeTests {
    private static readonly DateTime Built = new(2026, 10, 3, 3, 14, 0, DateTimeKind.Utc);

    private static PublicSiteState Site() => new() {
        IucnExists = true,
        IucnRelease = "2026-1",
        GbifDir = "/data/gbif-iucn",
        GbifZipName = "iucn-checklist-2026-07-28.zip",
        GbifRelease = "2026-1",
        GbifPublished = "2026-07-28",
        DoiCachePath = "/data/iucn_doi_cache.sqlite",
        DoiCacheExists = true,
        SitePath = "/data/site.sqlite",
        SiteExists = true,
        SiteSchemaVersion = SiteDbSchema.Version,
        SiteBuiltAtUtc = Built,
        SiteIucnRelease = "2026-1",
        SiteTaxonCount = 237_412,
        Inputs = new[] {
            new SiteInputChange(PublicSiteStateReader.IucnInput, Built.AddDays(-50)),
            new SiteInputChange(PublicSiteStateReader.ApiCacheInput, Built.AddDays(-28)),
            new SiteInputChange(PublicSiteStateReader.GbifInput, Built.AddHours(-3)),
            new SiteInputChange(PublicSiteStateReader.WikidataInput, Built.AddDays(-2)),
        },
    };

    // ---- GBIF checklist ----

    [Fact]
    public void Gbif_same_release_as_the_csv_is_ok() {
        var r = PublicSiteProbes.GbifStep(Site());
        Assert.Equal("ok", r.Status);
        Assert.Equal("Release 2026-1, the same release as the IUCN Red List database (iucn-checklist-2026-07-28.zip, published 2026-07-28).", r.Detail);
    }

    // The case that loses DOIs: every assessment new in 2026-1 has a different assessment id from
    // the one the 2025-2 checklist cites, so site build-db rejects the checklist's DOI for it.
    [Fact]
    public void Gbif_older_than_the_csv_is_todo_and_says_what_it_costs() {
        var r = PublicSiteProbes.GbifStep(Site() with { GbifRelease = "2025-2" });
        Assert.Equal("todo", r.Status);
        Assert.Contains("release 2025-2", r.Detail);
        Assert.Contains("holds release 2026-1", r.Detail);
        Assert.Contains("assessments new in release 2026-1 get no DOI from the checklist", r.Detail);
        Assert.Contains("GBIF does not have release 2026-1 yet", r.Detail);
    }

    [Fact]
    public void Gbif_newer_than_the_csv_points_to_the_iucn_import() {
        var r = PublicSiteProbes.GbifStep(Site() with { GbifRelease = "2026-2" });
        Assert.Equal("todo", r.Status);
        Assert.Contains("newer than the IUCN Red List database (release 2026-1)", r.Detail);
        Assert.Contains("Import IUCN data", r.Detail);
    }

    [Fact]
    public void Gbif_release_that_cannot_be_ordered_still_reports_both_releases() {
        var r = PublicSiteProbes.GbifStep(Site() with { GbifRelease = "2026-01" });
        Assert.Equal("todo", r.Status);
        Assert.Equal("The newest checklist is release 2026-01, but the IUCN Red List database holds release 2026-1.", r.Detail);
    }

    [Fact]
    public void Gbif_without_a_csv_release_reports_the_checklist_alone() {
        var r = PublicSiteProbes.GbifStep(Site() with { IucnRelease = null });
        Assert.Equal("ok", r.Status);
        Assert.Equal("Release 2026-1 (iucn-checklist-2026-07-28.zip, published 2026-07-28).", r.Detail);
    }

    // The metadata names no release: nothing to compare, so the light must not claim a match.
    [Fact]
    public void Gbif_metadata_without_a_release_says_so() {
        var r = PublicSiteProbes.GbifStep(Site() with { GbifRelease = null });
        Assert.Equal("ok", r.Status);
        Assert.DoesNotContain("same release", r.Detail);
        Assert.Contains("does not say which Red List release", r.Detail);
    }

    [Fact]
    public void Gbif_not_downloaded_or_unreadable_is_todo() {
        Assert.Equal("todo", PublicSiteProbes.GbifStep(Site() with { GbifZipName = null }).Status);
        Assert.Equal("todo", PublicSiteProbes.GbifStep(Site() with { GbifDir = null, GbifZipName = null }).Status);

        var unreadable = PublicSiteProbes.GbifStep(Site() with { GbifReadError = "The zip has no meta.xml." });
        Assert.Equal("todo", unreadable.Status);
        Assert.Equal("Could not read iucn-checklist-2026-07-28.zip: The zip has no meta.xml. Download it again.", unreadable.Detail);
    }

    [Theory]
    [InlineData("2025-2", "2026-1", -1)]
    [InlineData("2026-2", "2026-1", 1)]
    [InlineData("2026-1", "2026-1", 0)]
    [InlineData("2026", "2026-1", -1)]
    public void Releases_order_by_year_then_part(string a, string b, int expected) =>
        Assert.Equal(expected, Math.Sign(PublicSiteProbes.CompareReleases(a, b)!.Value));

    [Theory]
    [InlineData("unknown", "2026-1")]
    [InlineData("2026-x", "2026-1")]
    [InlineData("2026-1-2", "2026-1")]
    public void Releases_not_in_year_part_form_cannot_be_ordered(string a, string b) =>
        Assert.Null(PublicSiteProbes.CompareReleases(a, b));

    // ---- DOI cache ----

    // The light only knows whether the cache exists; with one, it leaves the step to run history.
    [Fact]
    public void Dois_todo_only_without_a_cache() {
        Assert.Equal("todo", PublicSiteProbes.DoiStep(Site() with { DoiCacheExists = false })!.Status);
        Assert.Null(PublicSiteProbes.DoiStep(Site()));
        Assert.Null(PublicSiteProbes.DoiStep(Site() with { DoiCachePath = null, DoiCacheExists = false }));
    }

    // ---- site database ----

    [Fact]
    public void Build_current_is_ok_with_release_and_taxon_count() {
        var r = PublicSiteProbes.BuildStep(Site());
        Assert.Equal("ok", r.Status);
        Assert.Equal("Built 2026-10-03 03:14 UTC from release 2026-1, with 237,412 taxa.", r.Detail);
    }

    [Fact]
    public void Build_names_every_input_changed_after_it_in_build_order() {
        var s = Site() with {
            Inputs = new[] {
                new SiteInputChange(PublicSiteStateReader.IucnInput, Built.AddDays(-50)),
                new SiteInputChange(PublicSiteStateReader.ApiCacheInput, Built.AddHours(5)),
                new SiteInputChange(PublicSiteStateReader.CommonNamesInput, Built.AddDays(-1)),
                new SiteInputChange(PublicSiteStateReader.WikidataInput, Built.AddMinutes(1)),
                new SiteInputChange(PublicSiteStateReader.SpratInput, Built.AddDays(2)),
            },
        };
        var r = PublicSiteProbes.BuildStep(s);
        Assert.Equal("todo", r.Status);
        Assert.Equal("Out of date: the IUCN API cache, the Wikidata cache, and the SPRAT (EPBC) database changed after the site database was built on 2026-10-03 03:14 UTC.", r.Detail);
    }

    [Fact]
    public void Build_with_one_changed_input_names_it() {
        var s = Site() with { Inputs = new[] { new SiteInputChange(PublicSiteStateReader.GbifInput, Built.AddSeconds(1)) } };
        Assert.Equal("Out of date: the GBIF checklist changed after the site database was built on 2026-10-03 03:14 UTC.",
            PublicSiteProbes.BuildStep(s).Detail);
    }

    // built_at_utc is written after every input has been read, so an input written in the same
    // second as the build is one the build read.
    [Fact]
    public void Build_input_changed_at_the_build_time_is_not_newer() {
        var s = Site() with { Inputs = new[] { new SiteInputChange(PublicSiteStateReader.WikipediaInput, Built) } };
        Assert.Equal("ok", PublicSiteProbes.BuildStep(s).Status);
    }

    [Fact]
    public void Build_from_another_release_is_todo() {
        var r = PublicSiteProbes.BuildStep(Site() with { SiteIucnRelease = "2025-2" });
        Assert.Equal("todo", r.Status);
        Assert.Equal("Built 2026-10-03 03:14 UTC from release 2025-2, but the IUCN Red List database now holds release 2026-1.", r.Detail);
    }

    // The site refuses a database with another schema version, so this outranks stale inputs.
    [Fact]
    public void Build_with_another_schema_version_is_todo_before_anything_else() {
        var s = Site() with {
            SiteSchemaVersion = SiteDbSchema.Version - 1,
            SiteIucnRelease = "2025-2",
            Inputs = new[] { new SiteInputChange(PublicSiteStateReader.ApiCacheInput, Built.AddDays(1)) },
        };
        var r = PublicSiteProbes.BuildStep(s);
        Assert.Equal("todo", r.Status);
        Assert.Equal($"Built with schema version {SiteDbSchema.Version - 1}. The site in this checkout needs schema version {SiteDbSchema.Version} and refuses any other version.", r.Detail);
    }

    [Fact]
    public void Build_without_a_schema_version_is_todo() {
        var r = PublicSiteProbes.BuildStep(Site() with { SiteSchemaVersion = null });
        Assert.Equal("todo", r.Status);
        Assert.StartsWith("The site database does not record its schema version.", r.Detail);
    }

    [Fact]
    public void Build_missing_unreadable_or_unconfigured_is_todo() {
        var missing = PublicSiteProbes.BuildStep(Site() with { SiteExists = false });
        Assert.Equal("todo", missing.Status);
        Assert.Equal("Not built yet: no file at /data/site.sqlite.", missing.Detail);

        var unreadable = PublicSiteProbes.BuildStep(Site() with { SiteReadError = "SQLite Error 1: 'no such table: meta'" });
        Assert.Equal("todo", unreadable.Status);
        Assert.Equal("Could not read the site database: SQLite Error 1: 'no such table: meta'.", unreadable.Detail);

        Assert.Equal("todo", PublicSiteProbes.BuildStep(Site() with { SitePath = null, SiteExists = false }).Status);
        Assert.Equal("todo", PublicSiteProbes.BuildStep(Site() with { SiteBuiltAtUtc = null }).Status);
    }

    [Fact]
    public void Evaluate_routes_each_key_and_ignores_others() {
        Assert.True(PublicSiteProbes.IsProbe(PublicSiteProbes.Gbif));
        Assert.True(PublicSiteProbes.IsProbe(PublicSiteProbes.Dois));
        Assert.True(PublicSiteProbes.IsProbe(PublicSiteProbes.Build));
        Assert.False(PublicSiteProbes.IsProbe(FlowStepProbes.WikiUpdateAll));
        Assert.Equal("ok", PublicSiteProbes.Evaluate(PublicSiteProbes.Build, Site())!.Status);
        Assert.Null(PublicSiteProbes.Evaluate("not-a-probe", Site()));
    }
}
