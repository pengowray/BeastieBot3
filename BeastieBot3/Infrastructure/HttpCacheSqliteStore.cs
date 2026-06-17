using System;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Infrastructure;

/// <summary>
/// Base for the HTTP-backed cache stores (IUCN API, Wikidata, Wikipedia). Composes an
/// <see cref="ApiImportMetadataStore"/> over the same connection and forwards the
/// <c>BeginImport</c>/<c>CompleteImport*</c> request-tracking calls, so the concrete stores no
/// longer hand-forward them. Factories must run <see cref="EnsureImportSchema"/> before their own
/// <see cref="SqliteStore.EnsureSchema"/> so the <c>http_request_log</c> table the cache tables
/// reference already exists.
/// </summary>
internal abstract class HttpCacheSqliteStore : SqliteStore {
    protected readonly ApiImportMetadataStore _importStore;

    protected HttpCacheSqliteStore(SqliteConnection connection) : base(connection) {
        _importStore = new ApiImportMetadataStore(connection);
    }

    /// <summary>Creates the shared <c>http_request_log</c> table. Call from the factory before <c>EnsureSchema()</c>.</summary>
    protected void EnsureImportSchema() => _importStore.EnsureSchema();

    public long BeginImport(string url) => _importStore.BeginImport(url);

    public void CompleteImportSuccess(long importId, int httpStatus, long payloadBytes, TimeSpan duration) =>
        _importStore.CompleteImportSuccess(importId, httpStatus, payloadBytes, duration);

    public void CompleteImportFailure(long importId, string errorMessage, int? statusCode, TimeSpan duration) =>
        _importStore.CompleteImportFailure(importId, errorMessage, statusCode, duration);
}
