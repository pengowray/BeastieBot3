using System;
using System.Collections.Generic;
using System.Text.Json;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// The dry run's output: one row per IUCN taxon to Wikidata item pairing with its tier, flags,
// category and planned actions, plus a row per run with the headline counts the flow page shows.
//
// Only the latest plan is kept (a full plan is ~190,000 pairs); run rows are history. Review
// decisions live in their own table keyed by taxon and item, so they survive every re-plan: a pair
// someone confirmed stays confirmed when the plan is rebuilt after a new Wikidata download.

namespace BeastieBot3.WikidataEdits;

internal sealed record PlanPairRow(
    long TaxonId,
    string Qid,
    string ScientificName,
    long AssessmentId,
    string IucnCode,
    LinkSource Source,
    ConfidenceTier Tier,
    IReadOnlyList<LinkFlag> Flags,
    PlanCategory Category,
    string? TargetValueQid,
    IReadOnlyList<string> CurrentValueQids,
    string AssessmentRef,
    bool CreatesAssessmentItem,
    bool VariantsDiffer,
    long BaseRevId,
    IReadOnlyDictionary<EditVariant, IReadOnlyList<PlannedAction>> Actions);

internal sealed class WikidataIucnPlanStore : SqliteStore {
    private static readonly JsonSerializerOptions Json = new() { WriteIndented = false };

    private WikidataIucnPlanStore(SqliteConnection connection) : base(connection) { }

    public static WikidataIucnPlanStore Open(string databasePath) {
        var store = new WikidataIucnPlanStore(OpenConnection(databasePath));
        store.EnsureSchema();
        return store;
    }

    internal static WikidataIucnPlanStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new WikidataIucnPlanStore(connection);
        store.EnsureSchema();
        return store;
    }

    protected override void EnsureSchema() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS plan_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                release TEXT NOT NULL,
                edition_item TEXT,
                oldest_item_download TEXT,
                counts_json TEXT
            );
            CREATE TABLE IF NOT EXISTS plan_pairs (
                taxon_id INTEGER NOT NULL,
                qid TEXT NOT NULL,
                scientific_name TEXT NOT NULL,
                assessment_id INTEGER NOT NULL,
                iucn_code TEXT NOT NULL,
                link_source TEXT NOT NULL,
                tier TEXT NOT NULL,
                flags TEXT NOT NULL,
                category TEXT NOT NULL,
                target_value TEXT,
                current_values TEXT NOT NULL,
                assessment_ref TEXT NOT NULL,
                creates_assessment_item INTEGER NOT NULL,
                variants_differ INTEGER NOT NULL,
                base_rev_id INTEGER NOT NULL,
                actions_json TEXT NOT NULL,
                PRIMARY KEY (taxon_id, qid)
            );
            CREATE INDEX IF NOT EXISTS ix_plan_pairs_tier_category ON plan_pairs (tier, category);
            CREATE INDEX IF NOT EXISTS ix_plan_pairs_qid ON plan_pairs (qid);
            CREATE TABLE IF NOT EXISTS review_decisions (
                taxon_id INTEGER NOT NULL,
                qid TEXT NOT NULL,
                decision TEXT NOT NULL CHECK (decision IN ('confirmed', 'rejected')),
                note TEXT,
                decided_at TEXT NOT NULL,
                PRIMARY KEY (taxon_id, qid)
            );
            """;
        cmd.ExecuteNonQuery();
    }

    public long BeginRun(string release, string? editionItem, DateTime startedAtUtc) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            INSERT INTO plan_runs (started_at, release, edition_item) VALUES (@at, @release, @edition);
            SELECT last_insert_rowid();
            """;
        cmd.Parameters.AddWithValue("@at", startedAtUtc.ToString("O"));
        cmd.Parameters.AddWithValue("@release", release);
        cmd.Parameters.AddWithValue("@edition", (object?)editionItem ?? DBNull.Value);
        return (long)cmd.ExecuteScalar()!;
    }

    public void ClearPairs() {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "DELETE FROM plan_pairs";
        cmd.ExecuteNonQuery();
    }

    public void InsertPairs(IReadOnlyList<PlanPairRow> rows) {
        if (rows.Count == 0) return;
        using var tx = _connection.BeginTransaction();
        using var cmd = _connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = """
            INSERT OR REPLACE INTO plan_pairs (taxon_id, qid, scientific_name, assessment_id, iucn_code, link_source,
                tier, flags, category, target_value, current_values, assessment_ref, creates_assessment_item,
                variants_differ, base_rev_id, actions_json)
            VALUES (@taxon, @qid, @name, @assessment, @code, @source, @tier, @flags, @category, @target, @current,
                @ref, @creates, @differ, @rev, @actions)
            """;
        var p = new Dictionary<string, SqliteParameter>();
        foreach (var name in new[] { "@taxon", "@qid", "@name", "@assessment", "@code", "@source", "@tier", "@flags", "@category",
                     "@target", "@current", "@ref", "@creates", "@differ", "@rev", "@actions" }) {
            p[name] = cmd.Parameters.Add(name, SqliteType.Text);
        }
        foreach (var r in rows) {
            p["@taxon"].Value = r.TaxonId;
            p["@qid"].Value = r.Qid;
            p["@name"].Value = r.ScientificName;
            p["@assessment"].Value = r.AssessmentId;
            p["@code"].Value = r.IucnCode;
            p["@source"].Value = r.Source.ToString();
            p["@tier"].Value = r.Tier.ToString();
            p["@flags"].Value = string.Join(',', r.Flags);
            p["@category"].Value = r.Category.ToString();
            p["@target"].Value = (object?)r.TargetValueQid ?? DBNull.Value;
            p["@current"].Value = string.Join(',', r.CurrentValueQids);
            p["@ref"].Value = r.AssessmentRef;
            p["@creates"].Value = r.CreatesAssessmentItem ? 1 : 0;
            p["@differ"].Value = r.VariantsDiffer ? 1 : 0;
            p["@rev"].Value = r.BaseRevId;
            p["@actions"].Value = SerializeActions(r.Actions);
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
    }

    public void FinishRun(long runId, DateTime finishedAtUtc, DateTime? oldestItemDownloadUtc, object counts) {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            UPDATE plan_runs SET finished_at = @at, oldest_item_download = @oldest, counts_json = @counts WHERE run_id = @run
            """;
        cmd.Parameters.AddWithValue("@at", finishedAtUtc.ToString("O"));
        cmd.Parameters.AddWithValue("@oldest", (object?)oldestItemDownloadUtc?.ToString("O") ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@counts", JsonSerializer.Serialize(counts, Json));
        cmd.Parameters.AddWithValue("@run", runId);
        cmd.ExecuteNonQuery();
    }

    public IReadOnlyDictionary<(long TaxonId, string Qid), string> LoadReviewDecisions() {
        var result = new Dictionary<(long, string), string>();
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT taxon_id, qid, decision FROM review_decisions";
        using var reader = cmd.ExecuteReader();
        while (reader.Read()) {
            result[(reader.GetInt64(0), reader.GetString(1))] = reader.GetString(2);
        }
        return result;
    }

    public static string SerializeActions(IReadOnlyDictionary<EditVariant, IReadOnlyList<PlannedAction>> actions) {
        var byName = new SortedDictionary<string, IReadOnlyList<PlannedAction>>(StringComparer.Ordinal);
        foreach (var (variant, list) in actions) byName[variant.ToString().ToLowerInvariant()] = list;
        return JsonSerializer.Serialize(byName, Json);
    }
}
