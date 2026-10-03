using System.Text.Json;

// Asks doi.org's handle API whether a DOI exists: GET https://doi.org/api/handles/<doi>?type=URL.
//
//   HTTP 200, {"responseCode":1, "values":[{"type":"URL","data":{"value":"https://www.iucnredlist.org/species/22823/14871490"}}]}
//       the DOI exists and points to that page.
//   HTTP 404, {"responseCode":100}  no such DOI.
//   responseCode 200                the DOI exists but has no URL value.
//
// Anything else (another responseCode, or a body that is not the handle API's JSON) is Unexpected:
// the caller must not take it to mean the DOI is missing.

namespace BeastieBot3.Iucn.Doi;

internal enum DoiHandleStatus {
    Exists,
    NotFound,
    Unexpected,
}

internal sealed record DoiHandleResult(string Doi, DoiHandleStatus Status, int HttpStatus, int? ResponseCode, string? Url);

internal interface IDoiHandleLookup {
    Task<DoiHandleResult> LookupAsync(string doi, CancellationToken cancellationToken);
}

internal sealed class DoiHandleClient : IDoiHandleLookup {
    public const string DefaultBaseUrl = "https://doi.org/api/handles/";

    private readonly PoliteHttpGetter _getter;
    private readonly string _baseUrl;

    public DoiHandleClient(PoliteHttpGetter getter, string baseUrl = DefaultBaseUrl) {
        _getter = getter;
        _baseUrl = baseUrl.EndsWith('/') ? baseUrl : baseUrl + "/";
    }

    public async Task<DoiHandleResult> LookupAsync(string doi, CancellationToken cancellationToken) {
        var result = await _getter.GetAsync(UrlFor(doi), cancellationToken).ConfigureAwait(false);
        return Interpret(doi, result.Status, result.Body);
    }

    public string UrlFor(string doi) {
        // The prefix and suffix are separate path segments; each is escaped on its own.
        var slash = doi.IndexOf('/');
        var path = slash < 0
            ? Uri.EscapeDataString(doi)
            : Uri.EscapeDataString(doi[..slash]) + "/" + Uri.EscapeDataString(doi[(slash + 1)..]);
        return _baseUrl + path + "?type=URL";
    }

    /// Reads the handle API's answer. Pure.
    internal static DoiHandleResult Interpret(string doi, int httpStatus, string? body) {
        int? responseCode = null;
        string? url = null;
        if (!string.IsNullOrWhiteSpace(body)) {
            try {
                using var document = JsonDocument.Parse(body);
                var root = document.RootElement;
                if (root.ValueKind == JsonValueKind.Object) {
                    if (root.TryGetProperty("responseCode", out var code) && code.ValueKind == JsonValueKind.Number && code.TryGetInt32(out var n)) {
                        responseCode = n;
                    }
                    url = FirstUrl(root);
                }
            } catch (JsonException) {
                // Not the handle API's JSON: Unexpected below.
            }
        }
        var status = responseCode switch {
            1 when httpStatus == 200 => DoiHandleStatus.Exists,
            200 when httpStatus == 200 => DoiHandleStatus.Exists,
            100 when httpStatus is 404 or 200 => DoiHandleStatus.NotFound,
            _ => DoiHandleStatus.Unexpected,
        };
        return new DoiHandleResult(doi, status, httpStatus, responseCode, status == DoiHandleStatus.Exists ? url : null);
    }

    private static string? FirstUrl(JsonElement root) {
        if (!root.TryGetProperty("values", out var values) || values.ValueKind != JsonValueKind.Array) {
            return null;
        }
        foreach (var value in values.EnumerateArray()) {
            if (value.ValueKind != JsonValueKind.Object
                || !value.TryGetProperty("type", out var type) || type.ValueKind != JsonValueKind.String
                || !string.Equals(type.GetString(), "URL", StringComparison.OrdinalIgnoreCase)
                || !value.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Object
                || !data.TryGetProperty("value", out var text) || text.ValueKind != JsonValueKind.String) {
                continue;
            }
            return text.GetString();
        }
        return null;
    }
}
