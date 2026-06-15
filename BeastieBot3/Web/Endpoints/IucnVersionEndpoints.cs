using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.Iucn;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Web.Endpoints;

// "Is my imported IUCN release out of date?" endpoint. Always returns the LOCAL
// version (import_metadata.redlist_version from the CSV main DB). The remote half
// — the current published version from the live IUCN API — is only attempted when
// IUCN_API_TOKEN is configured, and only on demand (the dashboard does NOT poll
// this so we never hammer the external API). The remote result is cached briefly.
// Degrades gracefully: no token -> { hasToken:false }; call fails -> { error }.

public static class IucnVersionEndpoints {
    private static readonly TimeSpan RemoteCacheTtl = TimeSpan.FromMinutes(30);
    private static readonly object Gate = new();
    private static string? _cachedLatest;
    private static string? _cachedError;
    private static DateTimeOffset _cachedAt = DateTimeOffset.MinValue;

    public static void MapIucnVersionEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/iucn-version", async (HttpContext ctx, PathsService paths) => {
            var local = ReadLocalVersion(paths);

            var forceRefresh = ctx.Request.Query.TryGetValue("refresh", out var r) && r == "1";
            var (hasToken, latest, error, checkedAt) = await GetLatestAsync(forceRefresh, ctx.RequestAborted).ConfigureAwait(false);

            bool? fresh = (local is not null && latest is not null)
                ? string.Equals(local, latest, StringComparison.OrdinalIgnoreCase)
                : (bool?)null;

            return Results.Json(new {
                local,
                latest,
                fresh,
                hasToken,
                checkedAt,
                error,
            });
        });
    }

    private static string? ReadLocalVersion(PathsService paths) {
        string? dbPath;
        try { dbPath = paths.GetIucnDatabasePath(); } catch { return null; }
        if (string.IsNullOrWhiteSpace(dbPath)) return null;
        var full = Path.GetFullPath(dbPath);
        if (!File.Exists(full)) return null;
        try {
            var csb = new SqliteConnectionStringBuilder { DataSource = full, Mode = SqliteOpenMode.ReadOnly };
            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT DISTINCT redlist_version FROM import_metadata LIMIT 1";
            return cmd.ExecuteScalar() as string;
        } catch { return null; }
    }

    private static async Task<(bool hasToken, string? latest, string? error, DateTimeOffset? checkedAt)>
        GetLatestAsync(bool forceRefresh, CancellationToken ct) {
        // Cheap token presence check — does NOT make a network call.
        EnvFileLoader.LoadIfPresent();
        var hasToken = !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("IUCN_API_TOKEN"));

        // The dashboard poller calls this WITHOUT refresh: only ever return the
        // already-cached remote result (or nothing) — never trigger a live call.
        if (!forceRefresh) {
            lock (Gate) {
                if (_cachedAt != DateTimeOffset.MinValue && DateTimeOffset.UtcNow - _cachedAt < RemoteCacheTtl) {
                    return (hasToken, _cachedLatest, _cachedError, _cachedAt);
                }
            }
            return (hasToken, null, null, null); // not checked yet — user must click "Check"
        }

        if (!hasToken) {
            return (false, null, null, null); // no token configured — local-only mode
        }

        IucnApiConfiguration config;
        try {
            config = IucnApiConfiguration.FromEnvironment();
        } catch {
            return (false, null, null, null);
        }

        string? latest = null;
        string? error = null;
        try {
            using var client = new IucnApiClient(config);
            var response = await client.GetRedListVersionAsync(ct).ConfigureAwait(false);
            latest = ExtractVersion(response.Body);
            if (latest is null) error = "Could not parse version from IUCN API response.";
        } catch (Exception ex) {
            error = ex.Message;
        }

        var now = DateTimeOffset.UtcNow;
        lock (Gate) {
            _cachedLatest = latest;
            _cachedError = error;
            _cachedAt = now;
        }
        return (true, latest, error, now);
    }

    // Defensive parse: the IUCN v4 response shape isn't contractually documented,
    // so accept the likely keys and fall back to the first scalar string.
    private static string? ExtractVersion(string json) {
        if (string.IsNullOrWhiteSpace(json)) return null;
        try {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.String) return root.GetString();
            if (root.ValueKind != JsonValueKind.Object) return null;

            foreach (var key in new[] { "red_list_version", "redlist_version", "version" }) {
                if (root.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.String) {
                    return v.GetString();
                }
            }
            // Some responses nest under a single property — take the first string value.
            foreach (var prop in root.EnumerateObject()) {
                if (prop.Value.ValueKind == JsonValueKind.String) return prop.Value.GetString();
            }
            return null;
        } catch (JsonException) {
            return null;
        }
    }
}
