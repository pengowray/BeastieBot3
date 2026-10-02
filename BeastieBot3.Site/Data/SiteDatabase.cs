using System.Globalization;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace BeastieBot3.Site.Data;

/// What the site knows about its database once it has opened it: the meta table and a few counts.
/// The database is never written while the site runs, so this is read once.
public sealed class SiteSnapshot {
    public required IReadOnlyDictionary<string, string> Meta { get; init; }

    /// Taxa whose latest_global_assessment_id is set.
    public required long GlobalTaxonCount { get; init; }

    public string? Get(string key) => Meta.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : null;

    /// Red List version of the data ("2026-1").
    public string? IucnRelease => Get(SiteDbSchema.MetaKeys.IucnRelease);

    public long? AssessmentCount =>
        long.TryParse(Get(SiteDbSchema.MetaKeys.AssessmentCount), NumberStyles.None, CultureInfo.InvariantCulture, out var n) ? n : null;
}

/// The site database, opened read-only. The site serves pages only when the file opens and its
/// schema_version equals SiteDbSchema.Version; otherwise every page answers 503 and /healthz says why.
/// The file is replaced by moving a new one into place and restarting the service, so a ready
/// database is never checked again. A database that is not ready is checked again at most every
/// 30 seconds, so a file that appears later is picked up without a restart.
public sealed class SiteDatabase {
    private static readonly TimeSpan RecheckInterval = TimeSpan.FromSeconds(30);

    private readonly ILogger<SiteDatabase> _logger;
    private readonly Lock _lock = new();
    private readonly string? _path;
    private readonly string? _connectionString;
    private SiteSnapshot? _snapshot;
    private string _notReadyReason = "not checked yet";
    private DateTime _lastCheckUtc = DateTime.MinValue;

    public SiteDatabase(IOptions<SiteOptions> options, ILogger<SiteDatabase> logger) {
        _logger = logger;
        _path = ExpandHome(options.Value.DatabasePath);
        if (_path is not null) {
            _connectionString = new SqliteConnectionStringBuilder {
                DataSource = _path,
                Mode = SqliteOpenMode.ReadOnly,
                Cache = SqliteCacheMode.Private,
                Pooling = true,
            }.ToString();
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

    /// Why the database is not ready (for logs and /healthz); empty when it is.
    public string NotReadyReason {
        get {
            EnsureChecked();
            return _snapshot is null ? _notReadyReason : string.Empty;
        }
    }

    /// Checks the database now and logs the outcome. Called once at startup.
    public void CheckAtStartup() {
        lock (_lock) {
            Check();
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
        if (_snapshot is not null) {
            return;
        }
        lock (_lock) {
            if (_snapshot is null && DateTime.UtcNow - _lastCheckUtc >= RecheckInterval) {
                Check();
            }
        }
    }

    private void Check() {
        _lastCheckUtc = DateTime.UtcNow;
        if (_path is null || _connectionString is null) {
            NotReady("Site:DatabasePath is not set");
            return;
        }
        if (!File.Exists(_path)) {
            NotReady($"the file {_path} does not exist");
            return;
        }
        try {
            using var connection = new SqliteConnection(_connectionString);
            connection.Open();
            var meta = ReadMeta(connection);
            if (!meta.TryGetValue(SiteDbSchema.MetaKeys.SchemaVersion, out var version)) {
                NotReady($"{_path} has no schema_version in its meta table");
                return;
            }
            if (version.Trim() != SiteDbSchema.Version.ToString(CultureInfo.InvariantCulture)) {
                NotReady($"{_path} has schema_version {version}, and this build of the site needs {SiteDbSchema.Version}. Rebuild it with `site build-db` from the same commit as the site");
                return;
            }
            using var count = connection.CreateCommand();
            count.CommandText = "SELECT COUNT(*) FROM taxon WHERE latest_global_assessment_id IS NOT NULL";
            var globalTaxa = Convert.ToInt64(count.ExecuteScalar(), CultureInfo.InvariantCulture);

            _snapshot = new SiteSnapshot { Meta = meta, GlobalTaxonCount = globalTaxa };
            _logger.LogInformation("Site database ready: {Path}, IUCN Red List version {Release}, schema version {Version}, {Taxa} taxa with a global assessment",
                _path, _snapshot.IucnRelease ?? "(not set)", version, globalTaxa);
        } catch (Exception ex) when (ex is SqliteException or IOException or InvalidOperationException or FormatException) {
            NotReady($"{_path} could not be read: {ex.Message}");
        }
    }

    private void NotReady(string reason) {
        _notReadyReason = reason;
        _logger.LogError("Site database not ready, so every page answers 503 until it is fixed: {Reason}", reason);
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
}
