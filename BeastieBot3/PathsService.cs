using System;
using System.Collections.Generic;
using System.IO;

namespace BeastieBot3 {
    // Provides typed access to paths defined in an INI file, backed by IniPathReader.
    public sealed class PathsService {
        private readonly IniPathReader _reader;

        public string SourceFilePath => _reader.SourceFilePath;

        public PathsService(string? iniFile = null, string? baseDirectory = null) {
            _reader = new IniPathReader(iniFile, baseDirectory);
        }

        // Get all flattened key/value pairs (normalized paths)
        public IReadOnlyDictionary<string, string> GetAll() => _reader.GetAll();

        // Specific strongly-typed helpers
        public string? GetColDir() => _reader.Get("Datasets:COL_dir");

        public string? GetIucnCvsDir() => _reader.Get("Datasets:IUCN_CVS_dir");

        public string? GetDatastoreDir() => _reader.Get("Datastore:datastore_dir");

        public string? GetMainDatabasePath() => _reader.Get("Datastore:MainDB");

        public string? GetColSqlitePath() => _reader.Get("Datastore:COL_sqlite");

        public string? GetIucnDatabasePath() => _reader.Get("Datastore:IUCN_sqlite_from_cvs");

        public string? GetIucnApiCachePath() => _reader.Get("Datastore:IUCN_api_cache_sqlite");

        public string? GetWikidataCachePath() =>
            _reader.Get("Datastore:wikidata_cache_sqlite") ?? _reader.Get("wikidata_cache_sqlite");

        public string ResolveIucnDatabasePath(string? overridePath) {
            var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
                ? overridePath
                : GetIucnDatabasePath();

            if (string.IsNullOrWhiteSpace(configuredPath)) {
                throw new InvalidOperationException($"IUCN SQLite database path is not configured. Set Datastore:IUCN_sqlite_from_cvs or pass --database.\n[using ini-file: '{_reader.SourceFilePath}']");
            }

            try {
                return Path.GetFullPath(configuredPath);
            }
            catch (Exception ex) {
                throw new InvalidOperationException($"Failed to resolve database path {configuredPath}: {ex.Message}", ex);
            }
        }

        public string ResolveIucnApiCachePath(string? overridePath) {
            var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
                ? overridePath
                : GetIucnApiCachePath();

            if (string.IsNullOrWhiteSpace(configuredPath)) {
                throw new InvalidOperationException($"IUCN API cache path is not configured. Set Datastore:IUCN_api_cache_sqlite or pass --cache.");
            }

            try {
                return Path.GetFullPath(configuredPath);
            }
            catch (Exception ex) {
                throw new InvalidOperationException($"Failed to resolve API cache path {configuredPath}: {ex.Message}", ex);
            }
        }

        public string ResolveWikidataCachePath(string? overridePath) {
            var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
                ? overridePath
                : GetWikidataCachePath();

            if (string.IsNullOrWhiteSpace(configuredPath)) {
                throw new InvalidOperationException("Wikidata cache path is not configured. Set Datastore:wikidata_cache_sqlite or pass --cache.");
            }

            try {
                return Path.GetFullPath(configuredPath);
            }
            catch (Exception ex) {
                throw new InvalidOperationException($"Failed to resolve Wikidata cache path {configuredPath}: {ex.Message}", ex);
            }
        }
    }
}
