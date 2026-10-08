using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

// HTTP and file code shared by the status list downloads: the HttpClient they use, a GET that tries
// again after a failure, and the writing and reading of a downloaded file. A file is written as
// <file>.part and renamed when it is complete, so a download that stops part way never leaves a file
// under the name that `--file` imports.
//
// The kept JSON files hold the rows as each source gave them, so an import reads them the same way as
// a download: NZTCS keeps {"assessments": [...], "species": [...]} (WriteJsonObjectAsync), SALVE
// keeps one array (WriteJsonArrayAsync), and the CITES Checklist keeps one gzip-compressed array
// (WriteGzipJsonArrayAsync, read back by ReadJsonArray), because its rows repeat the same long notes:
// about 140 MB of JSON, 3 MB compressed.

namespace BeastieBot3.StatusLists;

internal static class StatusListDownload {
    /// The waits before each new try of a failed request: 5 s, 15 s, 30 s, 1 min and 2 min, then the
    /// request fails.
    public static readonly TimeSpan[] RetryWaits = {
        TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2),
    };

    /// Answers worth asking again: 429, 408 and every 5xx.
    public static bool IsTransient(HttpStatusCode status) =>
        status is HttpStatusCode.TooManyRequests or HttpStatusCode.RequestTimeout || (int)status >= 500;

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

    /// GETs <paramref name="url"/> and returns the body. When the answer is 429, 408, a 5xx, no answer
    /// within the client's timeout or a network error, it tells <paramref name="onRetry"/> and tries
    /// again after each of RetryWaits (longer when the server's Retry-After asks for it, up to 10
    /// minutes); after the last it throws HttpRequestException. Any other failed answer throws at once.
    public static async Task<string> GetStringAsync(HttpClient http, string url, Action<string>? onRetry, CancellationToken cancellationToken) {
        for (var attempt = 0; ; attempt++) {
            string problem;
            TimeSpan? retryAfter = null;
            HttpStatusCode? status = null;
            try {
                using var response = await http.GetAsync(url, cancellationToken).ConfigureAwait(false);
                if (response.IsSuccessStatusCode) {
                    return await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                }
                status = response.StatusCode;
                problem = $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}";
                if (!IsTransient(response.StatusCode)) {
                    throw new HttpRequestException(problem, null, status);
                }
                retryAfter = response.Headers.RetryAfter?.Delta;
            } catch (HttpRequestException ex) when (ex.StatusCode is null) {
                problem = ex.Message;
            } catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested) {
                problem = $"no answer within {http.Timeout.TotalSeconds:0} s";
            }

            if (attempt >= RetryWaits.Length) {
                throw new HttpRequestException($"{problem} (tried {attempt + 1} times)", null, status);
            }
            var delay = retryAfter is { } after && after > RetryWaits[attempt] && after < TimeSpan.FromMinutes(10) ? after : RetryWaits[attempt];
            onRetry?.Invoke($"{problem}. Trying again in {delay.TotalSeconds:0} s.");
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    /// Downloads <paramref name="url"/> into <paramref name="file"/> as it is. When the answer is not a
    /// success it throws HttpRequestException with the status and the URL, and when the answer is a
    /// web page (Content-Type text/html, as a site under maintenance gives for every URL) it throws
    /// IOException, so the page is never saved under the file's name.
    public static async Task SaveAsync(HttpClient http, string url, string file, CancellationToken cancellationToken) {
        using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode) {
            throw new HttpRequestException($"HTTP {(int)response.StatusCode} {response.ReasonPhrase}: {url}", null, response.StatusCode);
        }
        if (string.Equals(response.Content.Headers.ContentType?.MediaType, "text/html", StringComparison.OrdinalIgnoreCase)) {
            throw new IOException($"The server answered with a web page (text/html), not the file: {url}");
        }
        await WriteAsync(file, output => response.Content.CopyToAsync(output, cancellationToken)).ConfigureAwait(false);
    }

    /// Writes the rows as one JSON array.
    public static Task WriteJsonArrayAsync(string file, IEnumerable<JsonElement> rows) =>
        WriteAsync(file, async output => {
            await using var writer = new Utf8JsonWriter(output);
            WriteArray(writer, rows);
        });

    /// Writes one gzip-compressed JSON array. <paramref name="produce"/> hands over the rows a batch at
    /// a time (a page of a download) through the action it is given, and each batch is written out
    /// before the next, so the whole download is never held in memory.
    public static Task WriteGzipJsonArrayAsync(string file, Func<Action<IEnumerable<JsonElement>>, Task> produce) =>
        WriteAsync(file, async output => {
            await using var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true);
            await using var writer = new Utf8JsonWriter(gzip);
            writer.WriteStartArray();
            await produce(rows => {
                foreach (var row in rows) {
                    row.WriteTo(writer);
                }
                writer.Flush();
            }).ConfigureAwait(false);
            writer.WriteEndArray();
        });

    /// The rows of a file holding one JSON array, gzip-compressed or not (told apart by the gzip
    /// signature), one at a time.
    public static IEnumerable<JsonElement> ReadJsonArray(string file) {
        using var input = File.OpenRead(file);
        var gzipped = input.ReadByte() == 0x1f && input.ReadByte() == 0x8b;
        input.Position = 0;
        using Stream stream = gzipped ? new GZipStream(input, CompressionMode.Decompress) : input;
        foreach (var row in JsonSerializer.DeserializeAsyncEnumerable<JsonElement>(stream).ToBlockingEnumerable()) {
            yield return row;
        }
    }

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
