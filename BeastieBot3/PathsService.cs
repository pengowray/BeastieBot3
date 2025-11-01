using System.Collections.Generic;

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

        public string? GetIucnDatabasePath() => _reader.Get("Datastore:IUCN_DB");
    }
}
