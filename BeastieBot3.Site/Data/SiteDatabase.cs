using System.Globalization;
using BeastieBot3.Shared.SiteData;
using BeastieBot3.Site.Web;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Data;

/// What the site knows about its database once it has opened it: the meta table and a few counts.
/// The database is never written while the site runs, so this is read once per database file.
public sealed class SiteSnapshot {
    public required IReadOnlyDictionary<string, string> Meta { get; init; }

    /// Taxa whose latest_global_assessment_id is set.
    public required long GlobalTaxonCount { get; init; }

    public string? Get(string key) => Meta.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;

    /// Red List version of the data ("2026-1").
    public string? IucnRelease => Get(SiteDbSchema.MetaKeys.IucnRelease);

    public long? AssessmentCount => Count(SiteDbSchema.MetaKeys.AssessmentCount);

    /// Assessments IUCN published with no geographic scope.
    public long? NoScopeAssessmentCount => Count(SiteDbSchema.MetaKeys.NoScopeAssessmentCount);

    private long? Count(string key) =>
        long.TryParse(Get(key), NumberStyles.None, CultureInfo.InvariantCulture, out var n) ? n : null;
}

/// The site database, opened read-only. The site serves pages only when the file opens and its
/// schema_version equals SiteDbSchema.Version; otherwise every page answers 503 and /healthz says why.
///
/// The file is checked again at most every 30 seconds, whether it was ready or not:
/// - not ready: it is opened and checked again, so a file that appears or is fixed later is picked
///   up without a restart;
/// - ready: its size and last write time are compared with the file that was checked. When the file
///   is gone or different (replaced by moving a new file into place), the connection pool is cleared,
///   because pooled connections keep reading the old file, the file is checked again, and every
///   output-cached response is evicted. A replaced file is then served without a restart; a removed
///   one or one with the wrong schema makes /healthz answer 503.
public sealed class SiteDatabase {
    internal static readonly TimeSpan RecheckInterval = TimeSpan.FromSeconds(30);

    private readonly ILogger<SiteDatabase> _logger;
    private readonly TimeProvider _time;
    private readonly IOutputCacheStore? _cacheStore;
    private readonly Lock _lock = new();
    private readonly string? _path;
    private readonly string? _connectionString;
    // The check opens its own connection outside the pool, so it never reads an old file through
    // a pooled handle.
    private readonly string? _checkConnectionString;

    private volatile SiteSnapshot? _snapshot;
    private FileIdentity? _checkedFile;
    private string _notReadyReason = "not checked yet";
    private volatile string _publicReason = "not checked yet";
    private long _nextCheckTicks;

    public SiteDatabase(IOptions<SiteOptions> options, ILogger<SiteDatabase> logger, TimeProvider time, IOutputCacheStore? cacheStore = null) {
        _logger = logger;
        _time = time;
        _cacheStore = cacheStore;
        _path = ExpandHome(options.Value.DatabasePath);
        if (_path is not null) {
            var builder = new SqliteConnectionStringBuilder {
                DataSource = _path,
                Mode = SqliteOpenMode.ReadOnly,
                Cache = SqliteCacheMode.Private,
                Pooling = true,
            };
            _connectionString = builder.ToString();
            builder.Pooling = false;
            _checkConnectionString = builder.ToString();
        }
    }

    /// The snapshot when the database is ready, otherwise null.
    public SiteSnapshot? Snapshot {
        get {
            EnsureChecked();
            return _snapshot;
        }
    }

    public bool IsReady => Snapshot is not null;

    /// Why the database is not ready, without file paths, for /healthz; empty when it is ready.
    public string PublicNotReadyReason {
        get {
            EnsureChecked();
            return _snapshot is null ? _publicReason : string.Empty;
        }
    }

    /// Checks the database now and logs the outcome. Called once at startup.
    public void CheckAtStartup() {
        lock (_lock) {
            Check();
            Volatile.Write(ref _nextCheckTicks, _time.GetUtcNow().UtcTicks + RecheckInterval.Ticks);
        }
    }

    /// A new read-only connection. Throws when the database is not ready.
    public SqliteConnection OpenConnection() {
        if (!IsReady || _connectionString is null) {
            throw new InvalidOperationException("The site database is not ready: " + _notReadyReason);
        }
        var connection = new SqliteConnection(_connectionString);
        connection.Open();
        return connection;
    }

    private void EnsureChecked() {
        if (_time.GetUtcNow().UtcTicks < Volatile.Read(ref _nextCheckTicks)) {
            return;
        }
        lock (_lock) {
            var now = _time.GetUtcNow().UtcTicks;
            if (now < _nextCheckTicks) {
                return;
            }
            Volatile.Write(ref _nextCheckTicks, now + RecheckInterval.Ticks);
            if (_snapshot is null) {
                Check();
                return;
            }
            var current = FileIdentity.Of(_path!);
            if (current == _checkedFile) {
                return;
            }
            _logger.LogWarning("The site database file {Path} was {Change} while the site was running. Checking it again",
                _path, current is null ? "removed" : "replaced");
            _snapshot = null;
            ClearPool();
            Check();
            EvictCachedResponses();
        }
    }

    private void Check() {
        if (_path is null || _checkConnectionString is null) {
            NotReady("Site:DatabasePath is not set", "database path not configured");
            return;
        }
        // Read before the file is opened: if the file is replaced in between, the next check sees
        // a different file and checks again.
        var file = FileIdentity.Of(_path);
        if (file is null) {
            NotReady($"the file {_path} does not exist", "database file not found");
            return;
        }
        try {
            using var connection = new SqliteConnection(_checkConnectionString);
            connection.Open();
            var meta = ReadMeta(connection);
            if (!meta.TryGetValue(SiteDbSchema.MetaKeys.SchemaVersion, out var version)) {
                NotReady($"{_path} has no schema_version in its meta table", "database has no schema version");
                return;
            }
            var expected = SiteDbSchema.Version.ToString(CultureInfo.InvariantCulture);
            if (version.Trim() != expected) {
                NotReady($"{_path} has schema_version {version}, and this build of the site needs {expected}. Rebuild it with `site build-db` from the same commit as the site",
                    $"database schema version {version.Trim()}, this build of the site needs {expected}");
                return;
            }
            using var count = connection.CreateCommand();
            count.CommandText = "SELECT COUNT(*) FROM taxon WHERE latest_global_assessment_id IS NOT NULL";
            var globalTaxa = Convert.ToInt64(count.ExecuteScalar(), CultureInfo.InvariantCulture);

            _checkedFile = file;
            _snapshot = new SiteSnapshot { Meta = meta, GlobalTaxonCount = globalTaxa };
            _logger.LogInformation("Site database ready: {Path}, IUCN Red List version {Release}, schema version {Version}, {Taxa} taxa with a global assessment",
                _path, _snapshot.IucnRelease ?? "(not set)", version, globalTaxa);
        } catch (Exception ex) when (ex is SqliteException or IOException or InvalidOperationException or FormatException) {
            NotReady($"{_path} could not be read: {ex.Message}", "database cannot be read");
        }
    }

    private void NotReady(string reason, string publicReason) {
        _notReadyReason = reason;
        _publicReason = publicReason;
        _logger.LogError("Site database not ready, so every page answers 503 until it is fixed: {Reason}", reason);
    }

    private void ClearPool() {
        if (_connectionString is not null) {
            using var connection = new SqliteConnection(_connectionString);
            SqliteConnection.ClearPool(connection);
        }
    }

    // Cached pages were made from the old file. A request that was already running on the old file
    // can still store its response after this; it expires with the cache's lifetime (1 hour).
    private void EvictCachedResponses() {
        if (_cacheStore is null) {
            return;
        }
        try {
            var eviction = _cacheStore.EvictByTagAsync(SiteCachePolicies.DatabaseTag, CancellationToken.None);
            if (!eviction.IsCompletedSuccessfully) {
                _ = eviction.AsTask().ContinueWith(
                    task => _logger.LogError(task.Exception, "Could not clear the output cache after the database file changed"),
                    CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
            }
        } catch (Exception ex) {
            _logger.LogError(ex, "Could not clear the output cache after the database file changed");
        }
    }

    private static Dictionary<string, string> ReadMeta(SqliteConnection connection) {
        var meta = new Dictionary<string, string>(StringComparer.Ordinal);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT key, value FROM meta";
        using var reader = command.ExecuteReader();
        while (reader.Read()) {
            meta[reader.GetString(0)] = reader.GetString(1);
        }
        return meta;
    }

    internal static string? ExpandHome(string? path) {
        if (string.IsNullOrWhiteSpace(path)) {
            return null;
        }
        path = path.Trim();
        if (path == "~" || path.StartsWith("~/", StringComparison.Ordinal)) {
            var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            return Path.Combine(home, path.Length > 2 ? path[2..] : string.Empty);
        }
        return path;
    }

    // Which file is at the path: a file moved into place has a different size or last write time.
    private sealed record FileIdentity(long Length, DateTime LastWriteUtc) {
        public static FileIdentity? Of(string path) {
            var info = new FileInfo(path);
            return info.Exists ? new FileIdentity(info.Length, info.LastWriteTimeUtc) : null;
        }
    }
}
