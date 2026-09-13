using System;
using System.IO;
using System.Text.Json;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Endpoints;
using BeastieBot3.WikidataEdits;
using Microsoft.Data.Sqlite;

// What the Wikidata IUCN status workflow page needs to light its steps: whether the release item
// is set in rules/wikidata/iucn-status.yml, whether existing assessment items have been looked up,
// and what the last dry run found. All three are small reads (a YAML file, a count over a table of
// a few thousand rows, one row from the plan store), so they are read on the poll directly.
//
// The lights for the manual steps on Wikidata (contacting the other bot's operator, the proposals,
// the bot request) have nothing local to read and carry no probe.

namespace BeastieBot3.Web.Flows;

public sealed record WikidataIucnPlanRun {
    public DateTime FinishedAtUtc { get; init; }
    public string Release { get; init; } = "";
    public string? EditionItem { get; init; }
    public long Pairs { get; init; }
    public long Editable { get; init; }
    public long ForReview { get; init; }
    public long StaleItems { get; init; }
    public long AssessmentItemsToCreate { get; init; }
}

public sealed record WikidataIucnFlowState {
    public string Release { get; init; } = "";
    public string? EditionItem { get; init; }
    public bool AssessmentItemTableExists { get; init; }
    public long AssessmentItems { get; init; }
    public DateTime? AssessmentItemsCheckedAtUtc { get; init; }
    public WikidataIucnPlanRun? LastPlan { get; init; }
}

public static class WikidataIucnFlowStateReader {
    public static WikidataIucnFlowState Read(PathsService paths) {
        var config = LoadConfig(paths);
        var state = new WikidataIucnFlowState { Release = config.Release, EditionItem = config.EditionItemOrNull };

        var cache = paths.GetWikidataCachePath();
        if (!string.IsNullOrWhiteSpace(cache) && File.Exists(cache)) {
            try {
                using var conn = OpenReadOnly(cache);
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*), MAX(fetched_at) FROM wikidata_iucn_assessment_items";
                using var r = cmd.ExecuteReader();
                if (r.Read()) {
                    state = state with {
                        AssessmentItemTableExists = true,
                        AssessmentItems = r.GetInt64(0),
                        AssessmentItemsCheckedAtUtc = r.IsDBNull(1) ? null : ParseUtc(r.GetString(1)),
                    };
                }
            } catch (SqliteException) {
                // table not created yet: never looked up
            }
        }

        var planPath = paths.GetWikidataIucnPlanPath();
        if (!string.IsNullOrWhiteSpace(planPath) && File.Exists(planPath)) {
            try {
                using var conn = OpenReadOnly(planPath);
                using var cmd = conn.CreateCommand();
                cmd.CommandText = """
                    SELECT finished_at, release, edition_item, counts_json FROM plan_runs
                    WHERE finished_at IS NOT NULL ORDER BY run_id DESC LIMIT 1
                    """;
                using var r = cmd.ExecuteReader();
                if (r.Read() && ParseUtc(r.GetString(0)) is { } finished) {
                    state = state with { LastPlan = PlanRun(finished, r.GetString(1), r.IsDBNull(2) ? null : r.GetString(2), r.IsDBNull(3) ? null : r.GetString(3)) };
                }
            } catch (SqliteException) {
                // no finished run
            }
        }
        return state;
    }

    internal static WikidataIucnPlanRun PlanRun(DateTime finished, string release, string? edition, string? countsJson) {
        long editable = 0, review = 0, pairs = 0, stale = 0, create = 0;
        if (countsJson is not null) {
            using var doc = JsonDocument.Parse(countsJson);
            var root = doc.RootElement;
            long Get(string name) => root.TryGetProperty(name, out var v) && v.TryGetInt64(out var n) ? n : 0;
            pairs = Get(nameof(WikidataIucnPlanTally.Pairs));
            stale = Get(nameof(WikidataIucnPlanTally.StaleItems));
            create = Get(nameof(WikidataIucnPlanTally.AssessmentItemsToCreate));
            if (root.TryGetProperty(nameof(WikidataIucnPlanTally.TierByCategory), out var tiers)) {
                foreach (var tier in tiers.EnumerateObject()) {
                    foreach (var cat in tier.Value.EnumerateObject()) {
                        if (cat.Name is nameof(PlanCategory.NoChange) or nameof(PlanCategory.UnmappedCategory)) continue;
                        var n = cat.Value.GetInt64();
                        if (tier.Name is "A" or "B") editable += n; else review += n;
                    }
                }
            }
        }
        return new WikidataIucnPlanRun {
            FinishedAtUtc = finished, Release = release, EditionItem = edition, Pairs = pairs,
            Editable = editable, ForReview = review, StaleItems = stale, AssessmentItemsToCreate = create,
        };
    }

    private static WikidataIucnEditConfig LoadConfig(PathsService paths) {
        try {
            var rules = RulesPaths.Resolve(paths);
            foreach (var dir in new[] { rules.SourceRulesDir, rules.BuildOutputRulesDir }) {
                if (File.Exists(WikidataIucnEditConfig.PathFor(dir))) return WikidataIucnEditConfig.Load(dir);
            }
        } catch {
            // unreadable settings read as defaults: release item not set
        }
        return new WikidataIucnEditConfig();
    }

    private static SqliteConnection OpenReadOnly(string path) {
        var conn = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        conn.Open();
        return conn;
    }

    private static DateTime? ParseUtc(string value) =>
        DateTime.TryParse(value, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal, out var d)
            ? d : null;
}
