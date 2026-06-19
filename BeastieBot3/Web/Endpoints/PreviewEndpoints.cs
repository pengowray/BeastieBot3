using System.Text.Json;
using BeastieBot3.Configuration;
using BeastieBot3.WikipediaLists;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Wikitext outputs browser + on-demand Wikipedia preview.
//
//   GET /api/wikitext/list                 -> the generated *.wikitext files (name, title, size, modified)
//                                             enriched with the cached taxa count + taxa-group/status
//                                             metadata so the UI can show a count column and a
//                                             grouped "by taxa" view without parsing the files.
//   GET /api/wikitext/preview?file=<name>  -> reads that file and POSTs its wikitext to the MediaWiki
//                                             action=parse API, returning the rendered HTML.
//
// Preview is on-demand for the single file the user picks — there is no bulk/auto preview — so we
// never hammer Wikipedia. The file must be a bare *.wikitext name inside the wikipedia-output root.

public static class PreviewEndpoints {
    private const string ParseApi = "https://en.wikipedia.org/w/api.php";
    private static readonly HttpClient Http = CreateClient();

    public static void MapPreviewEndpoints(this IEndpointRouteBuilder app) {
        app.MapGet("/api/wikitext/list", (PathsService paths) => {
            var dir = paths.GetWikipediaOutputDirectory();
            if (string.IsNullOrWhiteSpace(dir) || !Directory.Exists(dir)) {
                return Results.Json(new { dir, files = Array.Empty<object>() });
            }

            // The generator drops a structure-metrics.json sidecar next to the *.wikitext files,
            // recording the taxa count (and exact byte size) of every list it built, plus the run's
            // generation timestamp. We read counts from there instead of re-parsing each article.
            var metrics = LoadMetricsMap(dir, out var generatedAt);
            // Authoritative listId/taxa-group/preset come from the parsed list definitions (keyed by
            // output filename) — parsing the filename would be unreliable (hyphens in both group and
            // preset names).
            var defs = LoadDefinitionMap(paths);

            var files = new DirectoryInfo(dir)
                .EnumerateFiles("*.wikitext")
                .OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase)
                .Select(f => {
                    metrics.TryGetValue(f.Name, out var m);
                    var hasDef = defs.TryGetValue(f.Name, out var d);
                    // The cached count is valid only if the file wasn't regenerated after the metrics
                    // run that produced it. (A full run writes the sidecar last, so generatedAt >= every
                    // file's mtime; a later single-list regen leaves that file newer than the sidecar.)
                    bool stale = m != null && generatedAt.HasValue
                        && f.LastWriteTimeUtc > generatedAt.Value.AddSeconds(2);
                    return new {
                        name = f.Name,
                        title = DeriveTitle(f.Name),
                        size = f.Length,
                        modified = f.LastWriteTimeUtc,
                        taxa = (int?)(m?.TotalTaxa),
                        taxaStale = stale,
                        listId = hasDef ? d.ListId : null,
                        taxaGroup = hasDef ? d.TaxaGroup : null,
                        preset = hasDef ? d.Preset : null,
                        isParent = hasDef ? d.IsParent : (m?.IsParent ?? false),
                    };
                });
            return Results.Json(new { dir, generatedAt, files });
        });

        app.MapGet("/api/wikitext/preview", async (string file, PathsService paths, CancellationToken ct) => {
            var dir = paths.GetWikipediaOutputDirectory();
            if (string.IsNullOrWhiteSpace(dir)) {
                return Results.BadRequest(new { error = "No Wikipedia output directory is configured." });
            }
            // Only a bare *.wikitext filename inside the output dir — no path traversal.
            var name = Path.GetFileName(file ?? string.Empty);
            if (string.IsNullOrWhiteSpace(name) || !name.EndsWith(".wikitext", StringComparison.OrdinalIgnoreCase)) {
                return Results.BadRequest(new { error = "Expected a .wikitext filename." });
            }
            var full = Path.Combine(dir, name);
            if (!File.Exists(full)) {
                return Results.NotFound(new { error = $"File not found: {name}" });
            }

            string wikitext;
            try {
                wikitext = await File.ReadAllTextAsync(full, ct);
            } catch (Exception ex) {
                return Results.Json(new { error = ex.Message }, statusCode: 500);
            }

            try {
                var html = await ParseWikitextAsync(name, wikitext, ct);
                return Results.Json(new { name, title = DeriveTitle(name), html });
            } catch (Exception ex) {
                return Results.Json(new { error = "Wikipedia preview failed: " + ex.Message }, statusCode: 502);
            }
        });
    }

    private static async Task<string> ParseWikitextAsync(string name, string wikitext, CancellationToken ct) {
        var form = new Dictionary<string, string> {
            ["action"] = "parse",
            ["format"] = "json",
            ["formatversion"] = "2",
            ["contentmodel"] = "wikitext",
            ["prop"] = "text",
            ["disablelimitreport"] = "1",
            ["disableeditsection"] = "1",
            ["title"] = DeriveTitle(name),
            ["text"] = wikitext,
        };
        using var content = new FormUrlEncodedContent(form);
        using var resp = await Http.PostAsync(ParseApi, content, ct);
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.TryGetProperty("error", out var err)) {
            var info = err.TryGetProperty("info", out var i) ? i.GetString() : "API error";
            throw new Exception(info);
        }
        return doc.RootElement.GetProperty("parse").GetProperty("text").GetString() ?? string.Empty;
    }

    private static string DeriveTitle(string fileName) =>
        Path.GetFileNameWithoutExtension(fileName).Replace('_', ' ');

    // Read the generator's structure-metrics.json sidecar (taxa counts + byte sizes per list, plus the
    // run timestamp). Returns an empty map — never throws — when the sidecar is absent or unreadable.
    private static Dictionary<string, ListStructureMetrics> LoadMetricsMap(string dir, out DateTime? generatedAt) {
        generatedAt = null;
        var map = new Dictionary<string, ListStructureMetrics>(StringComparer.OrdinalIgnoreCase);
        var path = Path.Combine(dir, "structure-metrics.json");
        if (!File.Exists(path)) return map;
        try {
            var report = JsonSerializer.Deserialize<GenerationMetricsReport>(File.ReadAllText(path));
            if (report == null) return map;
            if (DateTime.TryParse(report.GeneratedAt, null,
                    System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal,
                    out var ts)) {
                generatedAt = ts;
            }
            foreach (var m in report.Lists) {
                if (!string.IsNullOrEmpty(m.FileName)) map[m.FileName] = m;
            }
        } catch {
            // Malformed sidecar — degrade to "no cached counts" rather than failing the listing.
        }
        return map;
    }

    // Map output filename -> the list definition's taxa-group / preset / parent metadata, so the UI can
    // group and order lists by taxon. Parsing YAML on every request is cheap relative to the page itself
    // and keeps the data authoritative. Returns an empty map on any failure.
    private static Dictionary<string, (string ListId, string? TaxaGroup, string? Preset, bool IsParent)>
            LoadDefinitionMap(PathsService paths) {
        var map = new Dictionary<string, (string, string?, string?, bool)>(StringComparer.OrdinalIgnoreCase);
        try {
            var configPath = Path.Combine(paths.BaseDirectory, "rules", "wikipedia-lists.yml");
            if (!File.Exists(configPath)) return map;
            var config = new WikipediaListDefinitionLoader().Load(configPath);
            foreach (var def in config.Lists) {
                if (string.IsNullOrEmpty(def.OutputFile)) continue;
                map[def.OutputFile] = (def.Id, def.TaxaGroup, def.Preset, def.SubLists.Count > 0);
            }
        } catch {
            // Bad/unparseable rules — the listing still works, just without taxa grouping.
        }
        return map;
    }

    private static HttpClient CreateClient() {
        var client = new HttpClient { Timeout = TimeSpan.FromSeconds(90) };
        // Wikipedia requires a descriptive User-Agent.
        client.DefaultRequestHeaders.Add("User-Agent",
            "BeastieBot3/1.0 (Wikipedia list-article preview; +https://www.iucnredlist.org/)");
        return client;
    }
}
