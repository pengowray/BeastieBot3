using System;
using BeastieBot3.CommonNames;
using BeastieBot3.Web.Flows;

namespace BeastieBot3.Tests;

// Pins the ambiguous-name light. The list is derived from the names in the hub, so aggregating
// again leaves it out of date with nothing downstream complaining: generation treats a name that
// has since become ambiguous as if it were unique. "The command ran" cannot answer that, so these
// are the cases the probe has to get right.
public class FlowCommonNameProbeTests {
    private static readonly DateTime Built = new(2026, 8, 20, 10, 0, 0, DateTimeKind.Utc);

    private static CommonNameHubState State(DateTime? builtAt, DateTime? namesChangedAt,
                                            long conflicts = 1234, bool exists = true, bool readable = true) => new() {
        HubPath = @"D:\datasets\beastiebot\common_names.sqlite",
        HubExists = exists,
        Readable = readable,
        ConflictCount = conflicts,
        ConflictsBuiltAt = builtAt,
        NamesChangedAt = namesChangedAt,
    };

    [Fact]
    public void Ok_when_built_after_the_last_aggregate() {
        var r = FlowStepProbes.Conflicts(State(Built, Built.AddHours(-2)));
        Assert.Equal("ok", r!.Status);
        Assert.Contains("1,234 ambiguous names", r.Detail);
    }

    [Fact]
    public void Ok_when_names_have_never_changed_since() {
        var r = FlowStepProbes.Conflicts(State(Built, null));
        Assert.Equal("ok", r!.Status);
    }

    [Fact]
    public void Todo_when_names_were_aggregated_after_the_list_was_built() {
        var r = FlowStepProbes.Conflicts(State(Built, Built.AddDays(1)));
        Assert.Equal("todo", r!.Status);
        Assert.Contains("before the names were last aggregated", r.Detail);
    }

    // aggregate --replace empties the conflict list, so the hub is left flagging nothing at all.
    [Fact]
    public void Todo_when_the_list_is_empty_and_was_never_built() {
        var r = FlowStepProbes.Conflicts(State(null, Built, conflicts: 0));
        Assert.Equal("todo", r!.Status);
        Assert.Contains("Not built yet", r.Detail);
    }

    // Conflicts with no date to judge them by: say nothing rather than guess, and let the step
    // fall back to its run history.
    [Fact]
    public void Silent_when_conflicts_exist_but_carry_no_date() {
        Assert.Null(FlowStepProbes.Conflicts(State(null, Built, conflicts: 42)));
    }

    [Fact]
    public void Silent_when_the_hub_is_missing_or_unreadable() {
        Assert.Null(FlowStepProbes.Conflicts(State(Built, null, exists: false)));
        Assert.Null(FlowStepProbes.Conflicts(State(Built, null, readable: false)));
    }

    // Timestamps are stored as UTC "O" strings; parsing them as local time would shift every
    // comparison above by the machine's offset.
    [Fact]
    public void Stored_timestamps_are_read_as_utc() {
        var parsed = CommonNameHubStateReader.ParseStoredUtc("2026-08-20T10:00:00.0000000Z");
        Assert.Equal(DateTimeKind.Utc, parsed!.Value.Kind);
        Assert.Equal(new DateTime(2026, 8, 20, 10, 0, 0, DateTimeKind.Utc), parsed.Value);
    }
}
