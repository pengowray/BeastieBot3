using System;
using System.Collections.Generic;
using System.IO;

namespace BeastieBot3 {
    // Provides typed access to paths defined in an INI file, backed by IniPathReader.
    public sealed class PathsService {
        private readonly IniPathReader _reader;

        public string SourceFilePath => _reader.SourceFilePath;
        public string BaseDirectory => _reader.BaseDirectory;

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

        public string? GetWikipediaCachePath() =>
            _reader.Get("Datastore:enwiki_cache_sqlite") ?? _reader.Get("enwiki_cache_sqlite");

        public string? GetReportOutputDirectory() =>
            _reader.Get("Reports:output_dir")
            ?? _reader.Get("Datastore:reports_dir")
            ?? _reader.Get("reports_dir")
            ?? _reader.Get("reports_output_dir");

        public string? GetDataAnalysisDirectory() =>
            _reader.Get("Datastore:data_analysis_dir")
            ?? _reader.Get("data_analysis_dir");

        public string? GetWikipediaOutputDirectory() =>
            _reader.Get("Datastore:wikipedia_output_dir")
            ?? _reader.Get("wikipedia_output_dir");

        public string? GetCommonNameStorePath() =>
            _reader.Get("Datastore:common_names_sqlite") ?? _reader.Get("common_names_sqlite");

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

        public string ResolveWikipediaCachePath(string? overridePath) {
            var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
                ? overridePath
                : GetWikipediaCachePath();

            if (string.IsNullOrWhiteSpace(configuredPath)) {
                throw new InvalidOperationException("Wikipedia cache path is not configured. Set Datastore:enwiki_cache_sqlite or pass --cache.");
            }

            try {
                return Path.GetFullPath(configuredPath);
            }
            catch (Exception ex) {
                throw new InvalidOperationException($"Failed to resolve Wikipedia cache path {configuredPath}: {ex.Message}", ex);
            }
        }

        public string ResolveCommonNameStorePath(string? overridePath) {
            var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
                ? overridePath
                : GetCommonNameStorePath();

            if (string.IsNullOrWhiteSpace(configuredPath)) {
                // Default to datastore directory if not explicitly configured
                var datastoreDir = GetDatastoreDir();
                if (!string.IsNullOrWhiteSpace(datastoreDir)) {
                    configuredPath = Path.Combine(datastoreDir, "common_names.sqlite");
                } else {
                    throw new InvalidOperationException("Common name store path is not configured. Set Datastore:common_names_sqlite or pass --database.");
                }
            }

            try {
                return Path.GetFullPath(configuredPath);
            }
            catch (Exception ex) {
                throw new InvalidOperationException($"Failed to resolve common name store path {configuredPath}: {ex.Message}", ex);
            }
        }
    }
}
