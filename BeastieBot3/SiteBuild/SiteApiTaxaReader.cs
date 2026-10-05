using System.Globalization;
using System.Net;
using System.Text.Json;
using BeastieBot3.Infrastructure;
using Microsoft.Data.Sqlite;

// Reads the cached /api/v4/taxa records (iucn_api_cache.sqlite, table taxa) for `site build-db`:
// each taxon's assessment headers (its history, and which ones are latest), its IUCN common names
// in every language, its IUCN synonyms, and the species the API names for an infraspecific taxon.
//
// A species or infraspecific taxon has its own record (root_sis_id = taxon id). A subpopulation
// usually has none; the record of the taxon it belongs to lists it under subpopulation_taxa[] with
// its names but not its assessments (taxa_lookup maps the subpopulation id to that record). Four
// Zea mays subpopulations do have their own record, which is preferred.
//
// A record whose id is not in the CSV (an old or merged id, or a taxon IUCN no longer assesses, such
// as the Amur leopard, 15957) becomes a taxon too (NotInRelease): its name, ranks, authority and
// kind come from the record's taxon object (kind from its species / infrarank / subpopulation flags),
// and its species from species_taxa. None of its assessments is latest (SiteAssessmentPass).

namespace BeastieBot3.SiteBuild;

/// One assessment header of a taxon record, with the fields the history rows need.
internal sealed record ApiAssessmentHeader(
    long AssessmentId,
    long? TaxonId,
    bool Latest,
    int? YearPublished,
    string? AssessmentDate,
    string? Category,
    string? Criteria,
    bool PossiblyExtinct,
    bool PossiblyExtinctInTheWild,
    string? Scope);

/// What one taxon's own record gives.
internal sealed class ApiTaxonRecord {
    public required IReadOnlyList<IucnAssessmentHeader> Headers { get; init; }
    public required IReadOnlyList<ApiAssessmentHeader> Assessments { get; init; }
}

internal sealed class SiteApiTaxaReader {
    private readonly Dictionary<long, SiteTaxon> _taxa;
    private readonly SiteBuildStats _stats;
    // Names of a subpopulation from the record it belongs to, used when it has no record of its own.
    private readonly Dictionary<long, (List<IucnCommonName> Names, List<SiteSynonym> Synonyms, long ParentRoot)> _subpopulationNames = new();

    public Dictionary<long, ApiTaxonRecord> Records { get; } = new();

    /// For a subpopulation listed in another taxon's record: that record's taxon id.
    public Dictionary<long, long> SubpopulationParents { get; } = new();

    /// Taxa made from records whose id is not in the CSV export; they are also added to the taxa
    /// dictionary the reader was given.
    public List<SiteTaxon> NotInRelease { get; } = new();

    public SiteApiTaxaReader(Dictionary<long, SiteTaxon> taxa, SiteBuildStats stats) {
        _taxa = taxa;
        _stats = stats;
    }

    public void Read(SqliteConnection cache, bool readAll, CancellationToken cancellationToken) {
        if (readAll) {
            using var command = cache.CreateCommand();
            command.CommandText = "SELECT root_sis_id, json FROM taxa";
            command.CommandTimeout = 0;
            using var reader = command.ExecuteReader();
            ProgressConsole.Run("Reading IUCN API taxon records", 0, progress => {
                while (reader.Read()) {
                    cancellationToken.ThrowIfCancellationRequested();
                    progress.Increment();
                    Process(reader.GetInt64(0), reader.GetFieldValue<byte[]>(1));
                }
            });
        } else {
            // A limited build looks up only the records it needs.
            var rowIds = new SortedSet<long>();
            using (var own = cache.CreateCommand()) {
                own.CommandText = "SELECT id FROM taxa WHERE root_sis_id = @id";
                var id = own.Parameters.Add("@id", SqliteType.Integer);
                using var lookup = cache.CreateCommand();
                lookup.CommandText = "SELECT taxa_id FROM taxa_lookup WHERE sis_id = @id AND scope = 'subpopulation'";
                var lookupId = lookup.Parameters.Add("@id", SqliteType.Integer);
                foreach (var taxon in _taxa.Values) {
                    id.Value = taxon.TaxonId;
                    if (own.ExecuteScalar() is long rowId) {
                        rowIds.Add(rowId);
                    }
                    if (taxon.Kind == SiteTaxonKind.Subpopulation) {
                        lookupId.Value = taxon.TaxonId;
                        using var reader = lookup.ExecuteReader();
                        while (reader.Read()) {
                            rowIds.Add(reader.GetInt64(0));
                        }
                    }
                }
            }
            // The records not in the CSV whose ids are in the same range as the taxa read.
            if (_taxa.Count > 0) {
                using var range = cache.CreateCommand();
                range.CommandText = "SELECT id FROM taxa WHERE root_sis_id <= @max";
                range.Parameters.AddWithValue("@max", _taxa.Keys.Max());
                using var reader = range.ExecuteReader();
                while (reader.Read()) {
                    rowIds.Add(reader.GetInt64(0));
                }
            }
            using var command = cache.CreateCommand();
            command.CommandText = "SELECT root_sis_id, json FROM taxa WHERE id = @id";
            var rowParameter = command.Parameters.Add("@id", SqliteType.Integer);
            foreach (var rowId in rowIds) {
                cancellationToken.ThrowIfCancellationRequested();
                rowParameter.Value = rowId;
                using var reader = command.ExecuteReader();
                if (reader.Read()) {
                    Process(reader.GetInt64(0), reader.GetFieldValue<byte[]>(1));
                }
            }
        }

        // Subpopulations without a record of their own take their names from the record that lists them.
        foreach (var (subpopulationId, found) in _subpopulationNames) {
            if (!_taxa.TryGetValue(subpopulationId, out var taxon)) {
                continue;
            }
            SubpopulationParents[subpopulationId] = found.ParentRoot;
            if (!Records.ContainsKey(subpopulationId)) {
                taxon.IucnCommonNames = found.Names;
                taxon.IucnSynonyms = found.Synonyms;
            }
        }
    }

    private void Process(long rootSisId, byte[] json) {
        _stats.ApiTaxaRowsRead++;
        JsonDocument document;
        try {
            document = JsonDocument.Parse(json);
        } catch (JsonException) {
            _stats.ApiTaxaRowsUnreadable++;
            return;
        }
        using (document) {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object) {
                _stats.ApiTaxaRowsUnreadable++;
                return;
            }
            var taxonElement = root.TryGetProperty("taxon", out var t) && t.ValueKind == JsonValueKind.Object ? t : (JsonElement?)null;

            if (!_taxa.TryGetValue(rootSisId, out var taxon)) {
                taxon = taxonElement is { } own ? NotInReleaseTaxon(rootSisId, own) : null;
                if (taxon is null) {
                    _stats.NotInReleaseRecordsUnusable++;
                } else {
                    _taxa[rootSisId] = taxon;
                    NotInRelease.Add(taxon);
                    _stats.Count(_stats.NotInReleaseByKind, taxon.Kind);
                }
            }
            if (taxon is not null) {
                Records[rootSisId] = new ApiTaxonRecord {
                    Headers = IucnTaxaHeaders.Read(root),
                    Assessments = ReadHeaders(root),
                };
                if (taxonElement is { } te) {
                    taxon.IucnCommonNames = ReadCommonNames(te);
                    taxon.IucnSynonyms = ReadSynonyms(te, _stats);
                    taxon.ApiSpeciesId = FirstSpeciesId(te);
                }
            }

            if (taxonElement is { } owner && owner.TryGetProperty("subpopulation_taxa", out var subpopulations)
                && subpopulations.ValueKind == JsonValueKind.Array) {
                foreach (var subpopulation in subpopulations.EnumerateArray()) {
                    if (subpopulation.ValueKind != JsonValueKind.Object || ReadLong(subpopulation, "sis_id") is not { } id
                        || !_taxa.TryGetValue(id, out var listed) || listed.Kind != SiteTaxonKind.Subpopulation
                        || _subpopulationNames.ContainsKey(id)) {
                        continue;
                    }
                    _subpopulationNames[id] = (ReadCommonNames(subpopulation), ReadSynonyms(subpopulation, _stats), rootSisId);
                }
            }
        }
    }

    // A taxon row from the record's taxon object, for a record whose id is not in the CSV; null when
    // the record names no taxon.
    private static SiteTaxon? NotInReleaseTaxon(long rootSisId, JsonElement taxon) {
        var scientificName = SiteBuildRules.CleanName(ReadString(taxon, "scientific_name"));
        if (scientificName.Length == 0) {
            return null;
        }
        var subpopulation = SiteBuildRules.NullIfBlank(ReadString(taxon, "subpopulation_name"));
        var authority = SiteBuildRules.NullIfBlank(ReadString(taxon, "authority"));
        return new SiteTaxon {
            TaxonId = rootSisId,
            ScientificName = scientificName,
            Kind = SiteBuildRules.KindFromApiFlags(IsTrue(taxon, "infrarank"), IsTrue(taxon, "subpopulation") || subpopulation is not null,
                scientificName),
            Kingdom = SiteBuildRules.NullIfBlank(ReadString(taxon, "kingdom_name")),
            Phylum = SiteBuildRules.NullIfBlank(ReadString(taxon, "phylum_name")),
            ClassName = SiteBuildRules.NullIfBlank(ReadString(taxon, "class_name")),
            OrderName = SiteBuildRules.NullIfBlank(ReadString(taxon, "order_name")),
            Family = SiteBuildRules.NullIfBlank(ReadString(taxon, "family_name")),
            Genus = SiteBuildRules.NullIfBlank(ReadString(taxon, "genus_name")),
            SpeciesEpithet = SiteBuildRules.NullIfBlank(ReadString(taxon, "species_name")),
            InfraRank = SiteBuildRules.InfraRankMarker(scientificName),
            InfraName = SiteBuildRules.NullIfBlank(ReadString(taxon, "infra_name")),
            SubpopulationName = subpopulation,
            Authority = authority is null ? null : SiteBuildRules.NullIfBlank(WebUtility.HtmlDecode(authority)),
            InRelease = false,
        };
    }

    // ------------------------------------------------------------ headers

    private static List<ApiAssessmentHeader> ReadHeaders(JsonElement root) {
        var headers = new List<ApiAssessmentHeader>();
        if (!root.TryGetProperty("assessments", out var assessments) || assessments.ValueKind != JsonValueKind.Array) {
            return headers;
        }
        foreach (var header in assessments.EnumerateArray()) {
            if (header.ValueKind != JsonValueKind.Object || ReadLong(header, "assessment_id") is not { } id) {
                continue;
            }
            headers.Add(new ApiAssessmentHeader(
                id,
                ReadLong(header, "sis_taxon_id"),
                IsTrue(header, "latest"),
                SiteBuildRules.Year(ReadString(header, "year_published")),
                SiteBuildRules.UtcDate(ReadString(header, "assessment_date")),
                SiteBuildRules.NullIfBlank(ReadString(header, "red_list_category_code")),
                SiteBuildRules.NullIfBlank(ReadString(header, "criteria")),
                IsTrue(header, "possibly_extinct"),
                IsTrue(header, "possibly_extinct_in_the_wild"),
                SiteBuildRules.ScopeFromApi(ReadScopes(header))));
        }
        return headers;
    }

    /// scopes[] as (code, English description).
    internal static List<(string? Code, string? Description)> ReadScopes(JsonElement element) {
        var scopes = new List<(string?, string?)>();
        if (!element.TryGetProperty("scopes", out var array) || array.ValueKind != JsonValueKind.Array) {
            return scopes;
        }
        foreach (var scope in array.EnumerateArray()) {
            if (scope.ValueKind != JsonValueKind.Object) {
                continue;
            }
            string? description = null;
            if (scope.TryGetProperty("description", out var d) && d.ValueKind == JsonValueKind.Object) {
                description = ReadString(d, "en");
            }
            scopes.Add((ReadString(scope, "code"), description));
        }
        return scopes;
    }

    // ------------------------------------------------------------ names

    private static List<IucnCommonName> ReadCommonNames(JsonElement taxon) {
        var names = new List<IucnCommonName>();
        if (!taxon.TryGetProperty("common_names", out var array) || array.ValueKind != JsonValueKind.Array) {
            return names;
        }
        foreach (var entry in array.EnumerateArray()) {
            if (entry.ValueKind != JsonValueKind.Object) {
                continue;
            }
            var name = SiteBuildRules.CleanName(ReadString(entry, "name"));
            if (name.Length == 0) {
                continue;
            }
            names.Add(new IucnCommonName(name, IucnLanguageCodes.Normalise(ReadString(entry, "language")), IsTrue(entry, "main")));
        }
        // The main name first, so it gets the lower name id and is shown first.
        return names.OrderByDescending(n => n.IsMain).ToList();
    }

    private static List<SiteSynonym> ReadSynonyms(JsonElement taxon, SiteBuildStats stats) {
        var names = new List<SiteSynonym>();
        if (!taxon.TryGetProperty("synonyms", out var array) || array.ValueKind != JsonValueKind.Array) {
            return names;
        }
        foreach (var entry in array.EnumerateArray()) {
            if (entry.ValueKind != JsonValueKind.Object) {
                continue;
            }
            var genus = ReadString(entry, "genus_name");
            var species = ReadString(entry, "species_name");
            var infraName = ReadString(entry, "infra_name");
            var fullName = ReadString(entry, "name");
            var name = SiteBuildRules.IucnSynonymName(genus, species, ReadString(entry, "infra_type"),
                infraName, ReadString(entry, "subpopulation_name"), fullName);
            if (name is null) {
                continue;
            }
            if (string.IsNullOrWhiteSpace(genus) || string.IsNullOrWhiteSpace(species)) {
                stats.SynonymsBuiltFromFullName++;
            }
            var authority = SiteBuildRules.IucnSynonymAuthority(name, fullName, ReadString(entry, "species_author"),
                ReadString(entry, "infrarank_author"), !string.IsNullOrWhiteSpace(infraName));
            names.Add(new SiteSynonym(name, authority));
        }
        return names;
    }

    private static long? FirstSpeciesId(JsonElement taxon) {
        if (!taxon.TryGetProperty("species_taxa", out var array) || array.ValueKind != JsonValueKind.Array) {
            return null;
        }
        foreach (var entry in array.EnumerateArray()) {
            if (entry.ValueKind == JsonValueKind.Object && ReadLong(entry, "sis_id") is { } id) {
                return id;
            }
        }
        return null;
    }

    // ------------------------------------------------------------ JSON helpers

    internal static bool IsTrue(JsonElement element, string property) =>
        element.TryGetProperty(property, out var value)
        && (value.ValueKind == JsonValueKind.True
            || (value.ValueKind == JsonValueKind.String && bool.TryParse(value.GetString(), out var parsed) && parsed));

    internal static long? ReadLong(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) {
            return null;
        }
        return value.ValueKind switch {
            JsonValueKind.Number when value.TryGetInt64(out var n) => n,
            JsonValueKind.String when long.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) => n,
            _ => null,
        };
    }

    internal static string? ReadString(JsonElement element, string property) {
        if (!element.TryGetProperty(property, out var value)) {
            return null;
        }
        return value.ValueKind switch {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            _ => null,
        };
    }
}
