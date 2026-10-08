using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using BeastieBot3.Col;
using BeastieBot3.Configuration;
using BeastieBot3.Infrastructure;
using BeastieBot3.Iucn.Gbif;
using BeastieBot3.Shared.SiteData;
using Microsoft.Data.Sqlite;

// What the public species site workflow needs to light its steps: which Red List release GBIF's
// newest checklist zip is from, the site database's meta table, and when each input of
// `site build-db` last changed.
//
// Every read here is small (a meta table, two MAX() over indexed columns, a few file times), so it
// runs on the poll directly. The checklist's eml.xml is the first exception: reading it means opening
// the zip, so its result is kept until the file's size or modification time changes. The DOI step's
// count is the second: it takes about 17 seconds, so SiteDoiCountReader makes it in the background.
//
// File times of a SQLite database: the newest of the main file and its -wal file, but the -wal
// file only when it has content. A database in WAL mode can hold its latest writes in the -wal
// file for days (the SPRAT database has a 4 KB main file beside a 2.8 MB -wal), and opening a
// database read-write touches an empty -wal file without changing anything. The -shm file is
// touched by every reader, so it is never looked at.

namespace BeastieBot3.Web.Flows;

/// <summary>One input of `site build-db` and when it last changed (UTC).</summary>
public sealed record SiteInputChange(string Name, DateTime ChangedAtUtc);

public sealed record PublicSiteState {
    // --- The IUCN Red List database (the CSV release, Datastore:IUCN_sqlite_from_cvs) ---
    public bool IucnExists { get; init; }
    /// The Red List release the database holds ("2026-1"). Null when it holds none or several.
    public string? IucnRelease { get; init; }

    // --- GBIF's IUCN checklist: the newest zip in Datasets:GBIF_IUCN_dir, the one site build-db reads ---
    public string? GbifDir { get; init; }
    public string? GbifZipName { get; init; }
    /// The Red List release named in the checklist's metadata ("2026-1"). Null when it names none.
    public string? GbifRelease { get; init; }
    /// The checklist's publication date as its metadata writes it ("2026-07-28").
    public string? GbifPublished { get; init; }
    /// Why the zip's metadata could not be read, when it could not.
    public string? GbifReadError { get; init; }

    // --- The DOI cache (`iucn resolve-dois`, Datastore:IUCN_doi_cache_sqlite) ---
    public string? DoiCachePath { get; init; }
    public bool DoiCacheExists { get; init; }
    /// What the DOI cache has checked of the latest global assessments that need it. Null until
    /// the background count has finished once, or when it could not be made (SiteDoiCountReader).
    public SiteDoiCount? DoiCount { get; init; }

    // --- The pages and redirects of the groups' titles (`wikipedia fetch-group-titles`, in the Wikipedia cache) ---
    public string? WikipediaCachePath { get; init; }
    /// The counts the last run stored; null when it has never run (or the cache cannot be read).
    public GroupTitleRunState? GroupTitles { get; init; }
    /// When the IUCN Red List database file last changed, to compare with the time the last run saw.
    public DateTime? IucnFileChangedAtUtc { get; init; }

    // --- The site database (`site build-db`, Datastore:site_sqlite) ---
    public string? SitePath { get; init; }
    public bool SiteExists { get; init; }
    /// Why the meta table could not be read, when the file is there but could not be read.
    public string? SiteReadError { get; init; }
    public int? SiteSchemaVersion { get; init; }
    public DateTime? SiteBuiltAtUtc { get; init; }
    public string? SiteIucnRelease { get; init; }
    public long? SiteTaxonCount { get; init; }

    // --- `wikidata sweep-taxa`'s progress, from wikidata_sync_state in the Wikidata cache ---
    public bool WikidataCacheExists { get; init; }
    /// When the last full pass finished; null when none has.
    public DateTime? SweepCompletedUtc { get; init; }
    /// When the pass under way started; null when none is under way.
    public DateTime? SweepPassStartedUtc { get; init; }
    /// The last item the pass under way stored (Q-number).
    public long SweepCursor { get; init; }
    /// When the state was read, for the age of the last pass.
    public DateTime ReadAtUtc { get; init; } = DateTime.UtcNow;

    // --- The status lists store (`statuses natureserve-fetch`, `statuses ecos-import`, `statuses nztcs-import`) ---
    public string? StatusListsPath { get; init; }
    /// The NatureServe, ECOS and NZTCS rows of status_source: when each last finished, and how many rows
    /// the store holds. Null when the source has never finished.
    public StatusListSourceState? NatureServe { get; init; }
    public StatusListSourceState? Ecos { get; init; }
    public StatusListSourceState? Nztcs { get; init; }
    /// When the NatureServe download under way started; null when none is under way.
    public DateTime? NatureServePassStartedUtc { get; init; }
    /// How many records the download under way has stored, and how many NatureServe said it has.
    public long NatureServePassStored { get; init; }
    public long? NatureServePassTotal { get; init; }

    /// When each input of `site build-db` last changed, in the order the build reads them.
    /// Inputs that do not exist are left out, as the build leaves them out.
    public IReadOnlyList<SiteInputChange> Inputs { get; init; } = Array.Empty<SiteInputChange>();
}

/// <summary>When a status list source last finished downloading, and how many rows the store holds.</summary>
public sealed record StatusListSourceState(DateTime FetchedAtUtc, long Rows);

/// <summary>
/// What the last `wikipedia fetch-group-titles` run left to do, and the time it saw on the IUCN Red
/// List database file (the groups come from that database).
/// </summary>
public sealed record GroupTitleRunState(DateTime FinishedAtUtc, long PagesToDownload, long RedirectListsToDownload, long Articles,
    DateTime? IucnChangedAtUtc);

/// <summary>The files the public site workflow reads, resolved from paths.ini.</summary>
public sealed record PublicSitePaths {
    public string? IucnDatabase { get; init; }
    public string? ApiCache { get; init; }
    public string? GbifDir { get; init; }
    public string? DoiCache { get; init; }
    public string? CommonNames { get; init; }
    public string? WikidataCache { get; init; }
    public string? WikipediaCache { get; init; }
    public string? ColPlacement { get; init; }
    public string? SpratDatabase { get; init; }
    public string? StatusLists { get; init; }
    public string? SiteDatabase { get; init; }

    // The same defaults `site build-db` uses when it is given no options.
    public static PublicSitePaths From(PathsService paths) {
        var col = Full(Try(paths.GetColSqlitePath));
        return new PublicSitePaths {
            IucnDatabase = Full(Try(paths.GetIucnDatabasePath)),
            ApiCache = Full(Try(paths.GetIucnApiCachePath)),
            GbifDir = Full(Try(paths.GetGbifIucnDir)),
            DoiCache = Full(Try(paths.GetIucnDoiCachePath)),
            CommonNames = Full(Try(paths.GetCommonNameStorePath)),
            WikidataCache = Full(Try(paths.GetWikidataCachePath)),
            WikipediaCache = Full(Try(paths.GetWikipediaCachePath)),
            ColPlacement = col is null ? null : TaxonPlacementStore.SidecarPath(col),
            SpratDatabase = Full(Try(paths.GetSpratDatabasePath)),
            StatusLists = Full(Try(paths.GetStatusListsPath)),
            SiteDatabase = Full(Try(paths.GetSiteDatabasePath)),
        };
    }

    private static string? Try(Func<string?> get) {
        try { return get(); } catch { return null; }
    }

    private static string? Full(string? path) {
        if (string.IsNullOrWhiteSpace(path)) return null;
        try { return Path.GetFullPath(path); } catch { return null; }
    }
}

public static class PublicSiteStateReader {
    // Names of the inputs as the web UI's data source chips name them, where there is a chip.
    public const string IucnInput = "IUCN Red List database";
    public const string ApiCacheInput = "IUCN API cache";
    public const string GbifInput = "GBIF checklist";
    public const string DoiCacheInput = "DOI cache";
    public const string CommonNamesInput = "Common names store";
    public const string WikidataInput = "Wikidata cache";
    public const string WikipediaInput = "Wikipedia cache";
    public const string ColPlacementInput = "Catalogue of Life placement";
    public const string SpratInput = "SPRAT (EPBC) database";
    public const string StatusListsInput = "Status lists store";

    /// The state for the workflow page, with the DOI step's count. That count comes from a
    /// background task (SiteDoiCountReader), so this overload is for the poll only; tests read
    /// through Read(PublicSitePaths), which starts no background work.
    public static PublicSiteState Read(PathsService paths) {
        var p = PublicSitePaths.From(paths);
        var state = Read(p);
        return state with { DoiCount = SiteDoiCountReader.Read(p, state) };
    }

    /// Never throws: whatever cannot be read is left empty, with the reason where there is one.
    public static PublicSiteState Read(PublicSitePaths p) {
        var inputs = new List<SiteInputChange>();
        void Add(string name, DateTime? changedAt) {
            if (changedAt is { } at) inputs.Add(new SiteInputChange(name, at));
        }

        var iucn = ReadIucn(p.IucnDatabase);
        Add(IucnInput, iucn.ImportedAt);
        Add(ApiCacheInput, ReadNewestDownload(p.ApiCache));

        var gbifZip = Try(() => GbifIucnChecklistFiles.FindNewest(p.GbifDir));
        var gbif = gbifZip is null ? null : ReadGbif(gbifZip);
        Add(GbifInput, gbifZip is null ? null : FileTime(gbifZip));

        Add(DoiCacheInput, SqliteChangedAt(p.DoiCache));
        Add(CommonNamesInput, SqliteChangedAt(p.CommonNames));
        Add(WikidataInput, SqliteChangedAt(p.WikidataCache));
        Add(WikipediaInput, SqliteChangedAt(p.WikipediaCache));
        Add(ColPlacementInput, SqliteChangedAt(p.ColPlacement));
        Add(SpratInput, SqliteChangedAt(p.SpratDatabase));
        Add(StatusListsInput, SqliteChangedAt(p.StatusLists));

        var state = new PublicSiteState {
            IucnExists = iucn.Exists,
            IucnRelease = iucn.Release,
            GbifDir = p.GbifDir,
            GbifZipName = gbifZip is null ? null : Path.GetFileName(gbifZip),
            GbifRelease = gbif?.Release,
            GbifPublished = gbif?.Published,
            GbifReadError = gbif?.Error,
            DoiCachePath = p.DoiCache,
            DoiCacheExists = Exists(p.DoiCache),
            WikipediaCachePath = p.WikipediaCache,
            GroupTitles = ReadGroupTitles(p.WikipediaCache),
            IucnFileChangedAtUtc = SqliteChangedAt(p.IucnDatabase),
            SitePath = p.SiteDatabase,
            SiteExists = Exists(p.SiteDatabase),
            Inputs = inputs,
        };
        state = ReadSweep(state, p.WikidataCache);
        state = ReadStatusLists(state, p.StatusLists);
        return state.SiteExists ? ReadSite(state, p.SiteDatabase!) : state;
    }

    // ---- `wikidata sweep-taxa` ----

    // Three keys of the sync table, read without WikidataCacheStore.Open, which would run its schema
    // work (and a column migration) on every poll.
    private static PublicSiteState ReadSweep(PublicSiteState state, string? path) {
        if (!Exists(path)) return state;
        state = state with { WikidataCacheExists = true };
        try {
            using var conn = OpenReadOnly(path!);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT key, value FROM wikidata_sync_state WHERE key IN (@completed, @started, @cursor)";
            cmd.Parameters.AddWithValue("@completed", Wikidata.WikidataCacheStore.TaxonSweepCompletedKey);
            cmd.Parameters.AddWithValue("@started", Wikidata.WikidataCacheStore.TaxonSweepStartedKey);
            cmd.Parameters.AddWithValue("@cursor", Wikidata.WikidataCacheStore.TaxonSweepCursorKey);
            cmd.CommandTimeout = 5;
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) {
                var value = reader.IsDBNull(1) ? null : reader.GetString(1);
                state = reader.GetString(0) switch {
                    Wikidata.WikidataCacheStore.TaxonSweepCompletedKey => state with { SweepCompletedUtc = StoredUtc.Parse(value) },
                    Wikidata.WikidataCacheStore.TaxonSweepStartedKey => state with { SweepPassStartedUtc = StoredUtc.Parse(value) },
                    _ => state with { SweepCursor = long.TryParse(value, out var cursor) ? cursor : 0 },
                };
            }
        } catch (Exception) {
            // No sync table yet: the sweep has never run.
        }
        return state;
    }

    // ---- the status lists store ----

    // Two source rows, two keys of the sync table and, while a NatureServe download is under way, a
    // count over the indexed fetched_at column. Read without StatusListStore.Open, which would run
    // its schema work on every poll.
    private static PublicSiteState ReadStatusLists(PublicSiteState state, string? path) {
        state = state with { StatusListsPath = path };
        if (!Exists(path)) return state;
        try {
            using var conn = OpenReadOnly(path!);
            using (var cmd = conn.CreateCommand()) {
                cmd.CommandText = "SELECT source, fetched_at, row_count FROM status_source WHERE source IN (@natureserve, @ecos, @nztcs)";
                cmd.Parameters.AddWithValue("@natureserve", StatusLists.StatusSources.NatureServe);
                cmd.Parameters.AddWithValue("@ecos", StatusLists.StatusSources.Ecos);
                cmd.Parameters.AddWithValue("@nztcs", StatusLists.StatusSources.Nztcs);
                cmd.CommandTimeout = 5;
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) {
                    if (reader.IsDBNull(1) || StoredUtc.Parse(reader.GetString(1)) is not { } fetched) continue;
                    var source = new StatusListSourceState(fetched, reader.IsDBNull(2) ? 0 : reader.GetInt64(2));
                    state = reader.GetString(0) switch {
                        StatusLists.StatusSources.NatureServe => state with { NatureServe = source },
                        StatusLists.StatusSources.Ecos => state with { Ecos = source },
                        _ => state with { Nztcs = source },
                    };
                }
            }
            using (var cmd = conn.CreateCommand()) {
                cmd.CommandText = "SELECT key, value FROM status_sync_state WHERE key IN (@started, @total)";
                cmd.Parameters.AddWithValue("@started", StatusLists.NatureServePassKeys.Started);
                cmd.Parameters.AddWithValue("@total", StatusLists.NatureServePassKeys.Total);
                cmd.CommandTimeout = 5;
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) {
                    var value = reader.IsDBNull(1) ? null : reader.GetString(1);
                    state = reader.GetString(0) == StatusLists.NatureServePassKeys.Started
                        ? state with { NatureServePassStartedUtc = StoredUtc.Parse(value) }
                        : state with { NatureServePassTotal = long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var total) ? total : null };
                }
            }
            if (state.NatureServePassStartedUtc is { } started) {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM natureserve_species WHERE fetched_at >= @since";
                cmd.Parameters.AddWithValue("@since", StatusLists.StatusListStore.Stamp(started));
                cmd.CommandTimeout = 5;
                state = state with { NatureServePassStored = Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture) };
            }
        } catch (Exception) {
            // No status tables yet: neither command has finished on this store.
        }
        return state;
    }

    // ---- the IUCN Red List database ----

    private sealed record IucnInfo(bool Exists, string? Release, DateTime? ImportedAt);

    // Completed imports only, as IucnReleaseStateReader reads them. The import time is when the
    // last zip finished importing, which says more than the file's time: `iucn import` also
    // recreates its views on every run.
    private static IucnInfo ReadIucn(string? path) {
        if (!Exists(path)) return new IucnInfo(false, null, null);
        try {
            using var conn = OpenReadOnly(path!);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT redlist_version, ended_at FROM import_metadata WHERE ended_at IS NOT NULL";
            cmd.CommandTimeout = 5;
            var releases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            DateTime? newest = null;
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) {
                if (!reader.IsDBNull(0) && reader.GetString(0).Trim() is { Length: > 0 } version
                    && !string.Equals(version, "unknown", StringComparison.OrdinalIgnoreCase)) {
                    releases.Add(version);
                }
                if (!reader.IsDBNull(1) && StoredUtc.Parse(reader.GetString(1)) is { } ended
                    && (newest is null || ended > newest)) {
                    newest = ended;
                }
            }
            return new IucnInfo(true, releases.Count == 1 ? releases.First() : null, newest);
        } catch (Exception) {
            // No import_metadata table, or a file that is not a database: nothing to compare against.
            return new IucnInfo(true, null, null);
        }
    }

    // ---- the IUCN API cache ----

    // The newest download, not the file's time: the cache is also written by refresh sessions and
    // failed-request records, which change nothing the site reads. Both columns are indexed.
    private static DateTime? ReadNewestDownload(string? path) {
        if (!Exists(path)) return null;
        try {
            using var conn = OpenReadOnly(path!);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT (SELECT MAX(downloaded_at) FROM taxa), (SELECT MAX(downloaded_at) FROM assessments)";
            cmd.CommandTimeout = 5;
            using var reader = cmd.ExecuteReader();
            if (!reader.Read()) return null;
            DateTime? newest = null;
            for (var i = 0; i < 2; i++) {
                if (!reader.IsDBNull(i) && StoredUtc.Parse(reader.GetString(i)) is { } at && (newest is null || at > newest)) {
                    newest = at;
                }
            }
            return newest;
        } catch (Exception) {
            return null;
        }
    }

    // ---- the groups' titles in the Wikipedia cache ----

    // One small key/value table that `wikipedia fetch-group-titles` writes at the end of each run.
    private static GroupTitleRunState? ReadGroupTitles(string? path) {
        if (!Exists(path)) return null;
        try {
            using var conn = OpenReadOnly(path!);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT key, value FROM wiki_group_title_status";
            cmd.CommandTimeout = 5;
            var values = new Dictionary<string, string>(StringComparer.Ordinal);
            using (var reader = cmd.ExecuteReader()) {
                while (reader.Read()) {
                    values[reader.GetString(0)] = reader.GetString(1);
                }
            }
            long Count(string key) => values.TryGetValue(key, out var v) && long.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : 0;
            if (!values.TryGetValue(Wikipedia.GroupTitleStatusKeys.FinishedAt, out var finished) || StoredUtc.Parse(finished) is not { } finishedAt) {
                return null;
            }
            var iucnChanged = values.TryGetValue(Wikipedia.GroupTitleStatusKeys.IucnChangedAt, out var changed) ? StoredUtc.Parse(changed) : null;
            return new GroupTitleRunState(finishedAt, Count(Wikipedia.GroupTitleStatusKeys.PagesToDownload),
                Count(Wikipedia.GroupTitleStatusKeys.RedirectListsToDownload), Count(Wikipedia.GroupTitleStatusKeys.Articles), iucnChanged);
        } catch (Exception) {
            // No such table yet: the command has never run on this cache.
            return null;
        }
    }

    // ---- GBIF's checklist ----

    private sealed record GbifInfo(string Path, long Length, DateTime ModifiedUtc, string? Release, string? Published, string? Error);

    private static GbifInfo? _gbifCache;

    // eml.xml is a few kilobytes inside a ~21 MB zip; opening the zip reads its central directory.
    // Kept until the file changes, because this runs on every poll of the workflow page.
    private static GbifInfo? ReadGbif(string zipPath) {
        FileInfo file;
        try {
            file = new FileInfo(zipPath);
            if (!file.Exists) return null;
        } catch {
            return null;
        }
        var cached = _gbifCache;
        if (cached is not null && cached.Path == zipPath && cached.Length == file.Length && cached.ModifiedUtc == file.LastWriteTimeUtc) {
            return cached;
        }
        GbifInfo info;
        try {
            var dataset = GbifIucnChecklistReader.ReadSummary(zipPath).Dataset;
            info = new GbifInfo(zipPath, file.Length, file.LastWriteTimeUtc, dataset.RedListVersion, dataset.PubDate, null);
        } catch (Exception ex) {
            // Not a zip, not a checklist, unreadable XML or no permission: all say the same thing here.
            info = new GbifInfo(zipPath, file.Length, file.LastWriteTimeUtc, null, null, ex.Message);
        }
        _gbifCache = info;
        return info;
    }

    // ---- the site database ----

    private static PublicSiteState ReadSite(PublicSiteState state, string path) {
        try {
            using var conn = OpenReadOnly(path);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT key, value FROM meta";
            cmd.CommandTimeout = 5;
            var meta = new Dictionary<string, string>(StringComparer.Ordinal);
            using (var reader = cmd.ExecuteReader()) {
                while (reader.Read()) {
                    if (!reader.IsDBNull(0) && !reader.IsDBNull(1)) meta[reader.GetString(0)] = reader.GetString(1);
                }
            }
            string? Get(string key) => meta.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v) ? v.Trim() : null;
            return state with {
                SiteSchemaVersion = int.TryParse(Get(SiteDbSchema.MetaKeys.SchemaVersion), NumberStyles.Integer, CultureInfo.InvariantCulture, out var version) ? version : null,
                SiteBuiltAtUtc = StoredUtc.Parse(Get(SiteDbSchema.MetaKeys.BuiltAtUtc)),
                SiteIucnRelease = Get(SiteDbSchema.MetaKeys.IucnRelease),
                SiteTaxonCount = long.TryParse(Get(SiteDbSchema.MetaKeys.TaxonCount), NumberStyles.Integer, CultureInfo.InvariantCulture, out var taxa) ? taxa : null,
            };
        } catch (Exception ex) {
            return state with { SiteReadError = ex.Message };
        }
    }

    // ---- files ----

    /// <summary>
    /// When a SQLite database last changed: the newer of the main file's time and its -wal file's
    /// time, counting the -wal file only when it is not empty. Null when the database does not exist.
    /// </summary>
    internal static DateTime? SqliteChangedAt(string? path) {
        var main = FileTime(path);
        if (main is null) return null;
        try {
            var wal = new FileInfo(path + "-wal");
            if (wal.Exists && wal.Length > 0 && wal.LastWriteTimeUtc > main.Value) return wal.LastWriteTimeUtc;
        } catch {
            // an unreadable -wal file leaves the main file's time
        }
        return main;
    }

    private static DateTime? FileTime(string? path) {
        if (string.IsNullOrWhiteSpace(path)) return null;
        try {
            return File.Exists(path) ? File.GetLastWriteTimeUtc(path) : null;
        } catch {
            return null;
        }
    }

    private static bool Exists(string? path) {
        if (string.IsNullOrWhiteSpace(path)) return false;
        try { return File.Exists(path); } catch { return false; }
    }

    private static T? Try<T>(Func<T?> get) where T : class {
        try { return get(); } catch { return null; }
    }

    // Read-only, no pooling, no schema work: this runs on a poll against databases other commands
    // are writing.
    private static SqliteConnection OpenReadOnly(string path) {
        var conn = new SqliteConnection(new SqliteConnectionStringBuilder {
            DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false,
        }.ConnectionString);
        conn.Open();
        return conn;
    }
}
