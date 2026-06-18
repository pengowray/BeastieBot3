using System.Text.Json;
using BeastieBot3.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace BeastieBot3.Web.Endpoints;

// Wikitext outputs browser + on-demand Wikipedia preview.
//
//   GET /api/wikitext/list                 -> the generated *.wikitext files (name, title, size, modified)
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
            var files = new DirectoryInfo(dir)
                .EnumerateFiles("*.wikitext")
                .OrderBy(f => f.Name, StringComparer.OrdinalIgnoreCase)
                .Select(f => new {
                    name = f.Name,
                    title = DeriveTitle(f.Name),
                    size = f.Length,
                    modified = f.LastWriteTimeUtc,
                });
            return Results.Json(new { dir, files });
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

    private static HttpClient CreateClient() {
        var client = new HttpClient { Timeout = TimeSpan.FromSeconds(90) };
        // Wikipedia requires a descriptive User-Agent.
        client.DefaultRequestHeaders.Add("User-Agent",
            "BeastieBot3/1.0 (Wikipedia list-article preview; +https://www.iucnredlist.org/)");
        return client;
    }
}
