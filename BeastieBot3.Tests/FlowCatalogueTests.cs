using System;
using System.Collections.Generic;
using System.Linq;
using BeastieBot3.Web.Commands;
using BeastieBot3.Web.Flows;
using Xunit;

namespace BeastieBot3.Tests;

// Pins the workflow catalogue against the command registry: every button on a workflow page runs a
// registered command with options that command has, and every light names a probe some evaluator
// answers. A button for a command that does not exist only shows "Unknown command" when clicked,
// and a probe key nothing answers leaves the step without a light, so neither shows up otherwise.
public class FlowCatalogueTests {
    // Commands a workflow step names before the command itself is merged. A command listed here
    // may be missing from the registry; once it is registered it is checked like any other.
    private static readonly HashSet<string> PendingCommands = new(StringComparer.Ordinal);

    private static IEnumerable<(FlowDefinition Flow, FlowStep Step, string Command)> StepCommands() =>
        from flow in FlowCatalogue.All
        from step in flow.Steps
        from command in step.Commands
        select (flow, step, command);

    // The web UI splits a step's command the same way: the longest registered command path that
    // starts the string, then the rest as arguments.
    private static (RegisteredCommand? Command, string[] Args) Split(string text) {
        var tokens = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        for (var n = tokens.Length; n > 0; n--) {
            if (CommandRegistry.FindByPath(string.Join(' ', tokens.Take(n))) is { } command) {
                return (command, tokens.Skip(n).ToArray());
            }
        }
        return (null, tokens);
    }

    [Fact]
    public void Every_step_command_is_a_registered_command() {
        var unknown = StepCommands()
            .Where(x => Split(x.Command).Command is null && !PendingCommands.Contains(x.Command))
            .Select(x => $"{x.Flow.Id}/{x.Step.Id}: {x.Command}")
            .ToList();
        Assert.True(unknown.Count == 0, "No such command: " + string.Join("; ", unknown));
    }

    [Fact]
    public void Every_option_in_a_step_command_is_one_the_command_has() {
        var wrong = new List<string>();
        foreach (var (flow, step, text) in StepCommands()) {
            var (command, args) = Split(text);
            if (command is null) continue;
            var names = CommandReflector.BuildSchema(command.Type).Fields
                .SelectMany(f => f.AltNames.Prepend(f.Name))
                .ToHashSet(StringComparer.Ordinal);
            wrong.AddRange(args
                .Where(a => a.StartsWith('-') && !names.Contains(a))
                .Select(a => $"{flow.Id}/{step.Id}: {command.Path} has no option {a}"));
        }
        Assert.True(wrong.Count == 0, string.Join("; ", wrong));
    }

    [Fact]
    public void Every_probe_key_is_answered_by_an_evaluator() {
        static bool Known(string probe) =>
            FlowStepProbes.IsIucnCsvProbe(probe) || FlowStepProbes.IsIucnApiProbe(probe)
            || FlowStepProbes.IsColProbe(probe) || FlowStepProbes.IsWikiProbe(probe)
            || WikidataIucnProbes.IsProbe(probe) || PublicSiteProbes.IsProbe(probe);

        var unknown = FlowCatalogue.All
            .SelectMany(f => f.Steps.Where(s => s.Probe is not null && !Known(s.Probe)).Select(s => $"{f.Id}/{s.Id}: {s.Probe}"))
            .ToList();
        Assert.True(unknown.Count == 0, "No evaluator for: " + string.Join("; ", unknown));
    }

    // The web UI keeps open steps by id across a poll, and finds a flow by id.
    [Fact]
    public void Flow_ids_and_step_ids_are_unique() {
        Assert.Equal(FlowCatalogue.All.Count, FlowCatalogue.All.Select(f => f.Id).Distinct().Count());
        foreach (var flow in FlowCatalogue.All) {
            var duplicates = flow.Steps.GroupBy(s => s.Id).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
            Assert.True(duplicates.Count == 0, $"{flow.Id} repeats step ids: {string.Join(", ", duplicates)}");
        }
    }

    // cache-assessments has three queues, one per run; the API flow's "Step by step" panel has a
    // step for each of the two that `iucn api cache-all` runs as phases of their own.
    [Fact]
    public void Api_step_by_step_has_the_csv_missing_and_stale_latest_queues() {
        var flow = FlowCatalogue.Find("iucn-import")!;
        var commands = flow.Steps.Where(s => s.Section == FlowSection.StepByStep).SelectMany(s => s.Commands).ToList();
        Assert.Contains("iucn api cache-assessments --csv-missing", commands);
        Assert.Contains("iucn api cache-assessments --stale-latest", commands);

        // Both come before the projection, which the panel says to run last.
        var ids = flow.Steps.Select(s => s.Id).ToList();
        Assert.True(ids.IndexOf("api-csv-missing") < ids.IndexOf("api-project-view"));
        Assert.True(ids.IndexOf("api-stale-latest") < ids.IndexOf("api-project-view"));
    }

    // The public site flow, in the order the work has to happen: the DOI sources before the build,
    // the build before the check and the deploy.
    [Fact]
    public void Public_site_flow_runs_its_steps_in_order() {
        var flow = FlowCatalogue.Find("public-site");
        Assert.NotNull(flow);
        var pipeline = flow!.Steps.Where(s => s.Section == FlowSection.Pipeline).ToList();
        string[] commands = pipeline.Select(s => s.Commands.FirstOrDefault() ?? "(by hand)").ToArray();
        var order = new[] { "iucn gbif-download", "iucn resolve-dois", "site build-db", "site check-citations", "(by hand)" };
        Assert.Equal(order, commands.Where(c => order.Contains(c)).ToArray());

        var deploy = pipeline[^1];
        Assert.Empty(deploy.Commands);
        Assert.Null(deploy.Probe);
        Assert.Contains(deploy.GuideSteps, g => g.Contains("deploy/oracle/deploy-db.sh"));
        Assert.True(pipeline.Single(s => s.Commands.Contains("site check-citations")).Optional);
    }
}
