using System;
using System.Collections.Generic;
using System.IO;
using BeastieBot3.Col;
using BeastieBot3.Web.Flows;

namespace BeastieBot3.Tests;

// Pins the Catalogue of Life workflow lights. CoL has no version check anywhere else — a database
// from the previous release looks perfectly healthy — and its consumers degrade quietly instead of
// failing, so their output silently stays frozen on the old release. These steps say which.
public class FlowColProbeTests {
    private static readonly DateTime Imported = new(2026, 6, 29, 12, 39, 0, DateTimeKind.Utc);

    private static ColLoadedRelease Loaded(string label = "COL26.5 XR", string version = "26.5",
                                           string issued = "2026-05-15", bool complete = true,
                                           bool exists = true, DateTime? importedAt = null) => new() {
        Path = @"D:\datasets\beastiebot\col_coldp_COL26.5_XR.sqlite",
        Exists = exists,
        Complete = complete,
        Label = label,
        Version = version,
        Issued = issued,
        ImportedAt = importedAt ?? Imported,
    };

    private static ColInputRelease Input(string label = "COL26.5 XR", string version = "26.5",
                                         string issued = "2026-05-15") => new() {
        Dir = @"D:\datasets\Catalogue_of_Life_2026-05-15_XR",
        ArchiveCount = 1,
        Label = label,
        Version = version,
        Issued = issued,
    };

    private static ColUpdateState State(ColLoadedRelease? loaded = null, ColInputRelease? input = null,
                                        bool configDisagrees = false,
                                        IReadOnlyList<ColLeftover>? leftovers = null) {
        loaded ??= Loaded();
        input ??= Input();
        var (status, fresh, message) = ColUpdateStateReader.Evaluate(loaded, input);
        return new ColUpdateState {
            Loaded = loaded,
            Input = input,
            Status = status,
            Fresh = fresh,
            Message = message,
            ConfigDisagrees = configDisagrees,
            Leftovers = leftovers ?? Array.Empty<ColLeftover>(),
            CurrentSince = loaded.Exists ? loaded.ImportedAt : null,
        };
    }

    // ---- import ----

    [Fact]
    public void Import_MatchingRelease_IsOk() {
        var r = FlowStepProbes.ColImportStep(State())!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("COL26.5 XR", r.Detail);
    }

    [Fact]
    public void Import_NewerReleaseWaitingInTheFolder_IsTodo() {
        var r = FlowStepProbes.ColImportStep(State(
            input: Input("COL26.9 XR", "26.9", "2026-09-01")))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("COL26.9 XR", r.Detail);
        Assert.Contains("COL26.5 XR", r.Detail);
    }

    // Nothing else detects a half-written import: the success signal is internal.
    [Fact]
    public void Import_HalfWrittenDatabase_IsTodo() =>
        Assert.Equal("todo", FlowStepProbes.ColImportStep(State(loaded: Loaded(complete: false)))!.Status);

    [Fact]
    public void Import_NoDatabase_IsTodo() =>
        Assert.Equal("todo", FlowStepProbes.ColImportStep(State(loaded: Loaded(exists: false)))!.Status);

    // ---- repoint ----

    [Fact]
    public void Repoint_PointingAtTheCurrentRelease_IsOk() {
        var r = FlowStepProbes.ColRepointStep(State())!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("already points at", r.Detail);
    }

    // COL_dir and COL_sqlite move independently and nothing else warns when only one changes.
    // Here COL_sqlite is on the current release while COL_dir was left on the previous one, so
    // there is nothing newer to import and the drift is the only thing to report.
    [Fact]
    public void Repoint_ConfigKeysDisagree_IsTodo() {
        var r = FlowStepProbes.ColRepointStep(State(
            input: Input("COL25.10 XR", "25.10", "2025-10-01"), configDisagrees: true))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("paths.ini mismatch", r.Detail);
    }

    [Fact]
    public void Repoint_StillOnTheOldRelease_IsTodo() {
        var r = FlowStepProbes.ColRepointStep(State(input: Input("COL26.9 XR", "26.9", "2026-09-01")))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("COL_sqlite still points to", r.Detail);
    }

    [Fact]
    public void Repoint_NoDatabaseAtAll_IsTodo() =>
        Assert.Equal("todo", FlowStepProbes.ColRepointStep(State(loaded: Loaded(exists: false)))!.Status);

    // ---- CoL placement ----

    [Fact]
    public void Placement_SaysNothingWithoutADatabaseToBuildFrom() =>
        Assert.Null(FlowStepProbes.ColPlacementStep(null));

    [Fact]
    public void Placement_IsOkWhenCurrent() {
        var r = FlowStepProbes.ColPlacementStep(new ColPlacementSummary {
            State = "current", BuiltAtUtc = new DateTime(2026, 10, 2, 0, 59, 0, DateTimeKind.Utc), Species = 175_923, Matched = 174_411,
        })!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("175,923 IUCN species", r.Detail);
    }

    [Theory]
    [InlineData("no-file", "No placement for this IUCN database yet")]
    [InlineData("iucn-changed", "the IUCN database changed")]
    [InlineData("col-changed", "the CoL database changed")]
    [InlineData("rules-changed", "older version")]
    public void Placement_OutOfDate_SaysRegeneratingRebuildsIt(string state, string expected) {
        var r = FlowStepProbes.ColPlacementStep(new ColPlacementSummary {
            State = state, BuiltAtUtc = new DateTime(2026, 10, 2, 0, 59, 0, DateTimeKind.Utc),
        })!;
        Assert.Equal("todo", r.Status);
        Assert.Contains(expected, r.Detail);
        Assert.Contains("Regenerating the Wikipedia lists", r.Detail);
    }

    [Fact]
    public void Placement_UnreadableRulesFile_SaysToFixTheFile() {
        var r = FlowStepProbes.ColPlacementStep(new ColPlacementSummary { State = "not-assigned-unreadable", Error = "bad key" })!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("iucn-not-assigned.yml: bad key", r.Detail);
        Assert.DoesNotContain("Delete", r.Detail);
    }

    // ---- leftovers ----

    [Fact]
    public void Cleanup_SaysNothingWhenThereIsNothingToDelete() =>
        Assert.Null(FlowStepProbes.ColCleanupStep(State()));

    [Fact]
    public void Cleanup_ReportsTheSpaceTheOldReleaseIsHolding() {
        var r = FlowStepProbes.ColCleanupStep(State(leftovers: new[] {
            new ColLeftover { Path = @"D:\x\col_coldp_COL25.10_XR.sqlite", Bytes = 13_359_329_280 },
            new ColLeftover { Path = @"D:\x\col_coldp_COL25.10_XR.sqlite.enrich-cache.sqlite", Bytes = 20_328_448 },
        }))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("2 files", r.Detail);
        Assert.Contains("GB", r.Detail);
    }

    [Fact]
    public void Cleanup_NamesTheSingleFileWhenThereIsOnlyOne() {
        var r = FlowStepProbes.ColCleanupStep(State(leftovers: new[] {
            new ColLeftover { Path = @"D:\x\col_coldp_COL25.10_XR.sqlite", Bytes = 13_359_329_280 },
        }))!;
        Assert.Contains("col_coldp_COL25.10_XR.sqlite", r.Detail);
    }

    // ---- outputs built from CoL ----

    [Fact]
    public void Rebuild_BuiltAfterTheImport_IsOk() {
        var r = FlowStepProbes.ColRebuild(State(), Imported.AddDays(1))!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("after Catalogue of Life COL26.5 XR was imported", r.Detail);
    }

    [Fact]
    public void Rebuild_BuiltBeforeTheImport_IsTodo() {
        var r = FlowStepProbes.ColRebuild(State(), Imported.AddDays(-3))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("still reflects the previous release", r.Detail);
    }

    [Fact]
    public void Rebuild_NeverBuilt_IsTodo() =>
        Assert.Equal("todo", FlowStepProbes.ColRebuild(State(), null)!.Status);

    [Fact]
    public void Rebuild_SaysNothingWithoutAnImportedDatabase() =>
        Assert.Null(FlowStepProbes.ColRebuild(State(loaded: Loaded(exists: false)), Imported.AddDays(1)));

    // The reference is the import, NOT when paths.ini last changed: paths.ini also changes for
    // reasons unrelated to CoL, and using it would mark every one of these stale forever.
    [Fact]
    public void Rebuild_IgnoresUnrelatedConfigEdits() {
        var builtJustAfterImport = Imported.AddMinutes(30);
        var r = FlowStepProbes.ColRebuild(State(), builtJustAfterImport)!;
        Assert.Equal("ok", r.Status);
    }

    // ---- the two config keys drifting apart ----
    // COL_sqlite and COL_dir move independently and nothing else in the project notices when only
    // one of them has been changed.

    [Fact]
    public void ConfigDisagrees_FalseWhenBothNameTheSameRelease() =>
        Assert.False(ColUpdateStateReader.ConfigDisagrees(Loaded(), Input()));

    [Fact]
    public void ConfigDisagrees_TrueWhenTheyNameDifferentReleases() =>
        Assert.True(ColUpdateStateReader.ConfigDisagrees(Loaded(), Input("COL25.10 XR", "25.10", "2025-10-01")));

    // The importer turns the alias into a filename, so the two are compared loosely rather than
    // by exact text.
    [Fact]
    public void ConfigDisagrees_IgnoresHowTheAliasBecomesAFilename() =>
        Assert.False(ColUpdateStateReader.ConfigDisagrees(Loaded(label: "COL26.5 XR"), Input("COL26_5_XR")));

    [Fact]
    public void ConfigDisagrees_FalseWhenEitherSideIsUnknown() {
        Assert.False(ColUpdateStateReader.ConfigDisagrees(Loaded(exists: false), Input()));
        Assert.False(ColUpdateStateReader.ConfigDisagrees(Loaded(), null));
    }

    // While a newer release is still waiting to be imported there is nothing to repoint at yet,
    // so the import advice has to come before the "set both keys" advice.
    [Fact]
    public void Repoint_NewerReleaseWaiting_SaysImportFirstNotSetBothKeys() {
        var r = FlowStepProbes.ColRepointStep(State(
            input: Input("COL26.9 XR", "26.9", "2026-09-01"), configDisagrees: true))!;
        Assert.Equal("todo", r.Status);
        Assert.Contains("Import the newer release first", r.Detail);
    }

    // ---- the shared version comparison ----

    [Fact]
    public void Evaluate_SameVersion_IsFresh() {
        var (status, fresh, _) = ColUpdateStateReader.Evaluate(Loaded(), Input());
        Assert.Equal("fresh", status);
        Assert.True(fresh);
    }

    [Fact]
    public void Evaluate_NewerIssueDateInTheFolder_OffersAnUpdate() {
        var (status, fresh, _) = ColUpdateStateReader.Evaluate(Loaded(), Input("COL26.9 XR", "26.9", "2026-09-01"));
        Assert.Equal("update-available", status);
        Assert.False(fresh);
    }

    [Fact]
    public void Evaluate_NoInputFolder_CannotTell() {
        var (status, fresh, _) = ColUpdateStateReader.Evaluate(Loaded(), null);
        Assert.Equal("no-input", status);
        Assert.Null(fresh);
    }

    // The first read of a multi-GB archive takes about twenty seconds, which the polled workflow
    // page cannot wait for. Until it is warm the answer is "not known yet", never "nothing newer".
    [Fact]
    public void Evaluate_ArchiveNotReadYet_DoesNotClaimThereIsNothingNewer() {
        var (status, fresh, _) = ColUpdateStateReader.Evaluate(Loaded(), null, inputPending: true);
        Assert.Equal("input-pending", status);
        Assert.Null(fresh);
    }

    [Fact]
    public void Import_ArchiveNotReadYet_ReportsWhatIsImportedAndKeepsChecking() {
        var pending = State() with { Status = "input-pending", InputPending = true, Input = null };
        var r = FlowStepProbes.ColImportStep(pending)!;
        Assert.Equal("ok", r.Status);
        Assert.Contains("COL26.5 XR", r.Detail);
        Assert.Contains("Still checking", r.Detail);
    }

    // A `redlist audit-site --limit` run writes to redlist-audit-2026-limited. Its partial pages
    // must not mark the CoL flow's "Rebuild the Red List audit site" step as done.
    [Fact]
    public void AuditSiteIndex_IgnoresLimitedRunFolder() {
        var reports = Path.Combine(Path.GetTempPath(), "col-art-" + Guid.NewGuid().ToString("N"));
        var full = Path.Combine(reports, "redlist-audit-2026", "index.html");
        var limited = Path.Combine(reports, "redlist-audit-2026-limited", "index.html");
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        Directory.CreateDirectory(Path.GetDirectoryName(limited)!);
        try {
            File.WriteAllText(full, "full");
            File.WriteAllText(limited, "limited");
            File.SetLastWriteTimeUtc(full, new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc));
            File.SetLastWriteTimeUtc(limited, new DateTime(2026, 9, 20, 0, 0, 0, DateTimeKind.Utc));
            Assert.Equal(full, ColArtifacts.NewestAuditSiteIndex(reports));

            File.Delete(full);
            Assert.Null(ColArtifacts.NewestAuditSiteIndex(reports));
        } finally {
            Directory.Delete(reports, recursive: true);
        }
    }
}
