using System.Net.Http.Headers;
using System.Text.Json;

// HTTP and file code shared by the status list downloads: the HttpClient they use, and the writing of
// a downloaded file. A file is written as <file>.part and renamed when it is complete, so a download
// that stops part way never leaves a file under the name that `--file` imports.
//
// The kept JSON files hold the rows as each source gave them, so an import reads them the same way as
// a download: NZTCS keeps {"assessments": [...], "species": [...]} (WriteJsonObjectAsync) and SALVE
// keeps one array (WriteJsonArrayAsync).

namespace BeastieBot3.StatusLists;

internal static class StatusListDownload {
    /// An HttpClient with BeastieBot3's User-Agent (NatureServeClient.UserAgent), asking for JSON when
    /// <paramref name="acceptJson"/> is set.
    public static HttpClient CreateClient(TimeSpan timeout, bool acceptJson = false) {
        var http = new HttpClient { Timeout = timeout };
        http.DefaultRequestHeaders.UserAgent.ParseAdd(NatureServeClient.UserAgent);
        if (acceptJson) {
            http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }
        return http;
    }

    /// Downloads <paramref name="url"/> into <paramref name="file"/> as it is.
    public static async Task SaveAsync(HttpClient http, string url, string file, CancellationToken cancellationToken) {
        using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        await WriteAsync(file, output => response.Content.CopyToAsync(output, cancellationToken)).ConfigureAwait(false);
    }

    /// Writes the rows as one JSON array.
    public static Task WriteJsonArrayAsync(string file, IEnumerable<JsonElement> rows) =>
        WriteAsync(file, async output => {
            await using var writer = new Utf8JsonWriter(output);
            WriteArray(writer, rows);
        });

    /// Writes one JSON object with an array of rows for each name, in the order given.
    public static Task WriteJsonObjectAsync(string file, params (string Name, IEnumerable<JsonElement> Rows)[] arrays) =>
        WriteAsync(file, async output => {
            await using var writer = new Utf8JsonWriter(output);
            writer.WriteStartObject();
            foreach (var (name, rows) in arrays) {
                writer.WritePropertyName(name);
                WriteArray(writer, rows);
            }
            writer.WriteEndObject();
        });

    private static void WriteArray(Utf8JsonWriter writer, IEnumerable<JsonElement> rows) {
        writer.WriteStartArray();
        foreach (var row in rows) {
            row.WriteTo(writer);
        }
        writer.WriteEndArray();
    }

    private static async Task WriteAsync(string file, Func<Stream, Task> write) {
        var partial = file + ".part";
        await using (var output = File.Create(partial)) {
            await write(output).ConfigureAwait(false);
        }
        File.Move(partial, file, overwrite: true);
    }
}
