using System;
using System.Collections.Generic;
using System.IO;

// Typed facade over IniPathReader for application-wide path configuration.
// Wraps paths.ini access with methods like GetIucnDatabasePath(), GetColDatabasePath(),
// GetWikidataCachePath(). INI sections: [Datastore] for .sqlite files,
// [Dirs] for folders (iucn_csv_folder, output_reports, col_dpfiles).
// Used by nearly all commands for consistent path resolution.

namespace BeastieBot3.Configuration;
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

        public string? GetColSqlitePath() => _reader.Get("Datastore:COL_sqlite");

        public string? GetIucnDatabasePath() => _reader.Get("Datastore:IUCN_sqlite_from_cvs");

        public string? GetIucnApiCachePath() => _reader.Get("Datastore:IUCN_api_cache_sqlite");

        // Australian SPRAT (EPBC) dataset: source report CSV and derived SQLite.
        public string? GetSpratCsvPath() => _reader.Get("Datasets:SPRAT_csv");

        public string? GetSpratDatabasePath() => _reader.Get("Datastore:SPRAT_sqlite");

        // Derived CSV-shaped projection of the API cache (built by `iucn api project-view`).
        public string? GetIucnApiProjectedPath() => _reader.Get("Datastore:IUCN_api_projected_sqlite");

        public string? GetWikidataCachePath() =>
            _reader.Get("Datastore:wikidata_cache_sqlite") ?? _reader.Get("wikidata_cache_sqlite");

        public string? GetWikipediaCachePath() =>
            _reader.Get("Datastore:enwiki_cache_sqlite") ?? _reader.Get("enwiki_cache_sqlite");

        public string? GetReportOutputDirectory() =>
            _reader.Get("Datastore:reports_dir")
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

        /// <summary>
        /// Explicit path to the SOURCE rules/ directory (the editable copy in the repo, not the
        /// build-output copy the app reads at runtime). Used by the web rule editor's draft/apply
        /// flow. Set [Dirs] rules_source_dir (or [Datastore] rules_source_dir) in paths.ini. When
        /// unset, the editor falls back to walking up for BeastieBot3.csproj — see RulesPaths.
        /// </summary>
        public string? GetRulesSourceDir() =>
            _reader.Get("Dirs:rules_source_dir") ?? _reader.Get("Datastore:rules_source_dir") ?? _reader.Get("rules_source_dir");

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

        public string ResolveSpratDatabasePath(string? overridePath) {
            var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
                ? overridePath
                : GetSpratDatabasePath();

            if (string.IsNullOrWhiteSpace(configuredPath)) {
                // Default to the datastore directory if not explicitly configured.
                var datastoreDir = GetDatastoreDir();
                if (!string.IsNullOrWhiteSpace(datastoreDir)) {
                    configuredPath = Path.Combine(datastoreDir, "sprat.sqlite");
                } else {
                    throw new InvalidOperationException($"SPRAT SQLite database path is not configured. Set Datastore:SPRAT_sqlite or pass --database.\n[using ini-file: '{_reader.SourceFilePath}']");
                }
            }

            try {
                return Path.GetFullPath(configuredPath);
            }
            catch (Exception ex) {
                throw new InvalidOperationException($"Failed to resolve SPRAT database path {configuredPath}: {ex.Message}", ex);
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

        /// <summary>
        /// Resolve the CSV-shaped API projection database path. Falls back to a file next to the
        /// API cache (iucn_api_projected.sqlite), then the datastore dir, so `iucn api project-view`
        /// works without an explicit INI entry. The file may not exist yet — callers that READ it
        /// (e.g. generate-lists --dataset api) must check File.Exists and tell the user to build it.
        /// </summary>
        public string ResolveIucnApiProjectedPath(string? overridePath) {
            var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
                ? overridePath
                : GetIucnApiProjectedPath();

            if (string.IsNullOrWhiteSpace(configuredPath)) {
                var cachePath = GetIucnApiCachePath();
                if (!string.IsNullOrWhiteSpace(cachePath)) {
                    var dir = Path.GetDirectoryName(Path.GetFullPath(cachePath));
                    if (!string.IsNullOrWhiteSpace(dir)) {
                        configuredPath = Path.Combine(dir, "iucn_api_projected.sqlite");
                    }
                }
                if (string.IsNullOrWhiteSpace(configuredPath)) {
                    var datastoreDir = GetDatastoreDir();
                    configuredPath = !string.IsNullOrWhiteSpace(datastoreDir)
                        ? Path.Combine(datastoreDir, "iucn_api_projected.sqlite")
                        : throw new InvalidOperationException("IUCN API projection path is not configured. Set Datastore:IUCN_api_projected_sqlite or pass --output.");
                }
            }

            try {
                return Path.GetFullPath(configuredPath);
            }
            catch (Exception ex) {
                throw new InvalidOperationException($"Failed to resolve API projection path {configuredPath}: {ex.Message}", ex);
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