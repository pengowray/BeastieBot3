using System.IO.Compression;
using System.Text.RegularExpressions;
using BeastieBot3.Taxonomy;

// Reads a red list's Darwin Core Archive into the rows `statuses red-lists-import` stores:
//   - one RedListTaxonRow per Distribution row with a status, joined to its core taxon. A taxon can
//     have several (Ecuador's birds: one for the mainland and one for the Galápagos Islands), so
//     each has a sequence number;
//   - one RedListSynonymRow per core taxon that is a synonym (its acceptedNameUsageID names another
//     taxon) of a taxon with a status, for name matching. Misapplied names are left out: they are
//     names used for another taxon, not names of this one.
//
// Left out, and counted in RedListSkips: Distribution rows with no status (empty, or the text
// "NULL"), rows whose status is NE (not evaluated; Denmark's national checklist gives NE to 43,000
// taxa, genera and families included), and rows whose taxon is not in the core.
//
// Nothing narrative is read: no occurrenceRemarks, taxonRemarks or descriptions.

namespace BeastieBot3.StatusLists;

/// One status of one taxon, as the archive gives it. ThreatStatus is verbatim; IucnCode is
/// RedListCategories.ToIucnCode of it; StatusLabel is the dataset's label for it, if any.
/// CanonicalName: the archive's canonicalName, else computed (RedListArchiveReader.CanonicalName).
internal sealed record RedListTaxonRow(
    string TaxonId,
    int Seq,
    string ScientificName,
    string? CanonicalName,
    string? Authorship,
    string? Rank,
    string? TaxonomicStatus,
    string? AcceptedTaxonId,
    string? AcceptedName,
    string? Kingdom,
    string? Phylum,
    string? Class,
    string? Order,
    string? Family,
    string? Genus,
    string ThreatStatus,
    string? IucnCode,
    string? StatusLabel,
    string? CountryCode,
    string? Locality,
    string? LocationId,
    string? EstablishmentMeans,
    string? OccurrenceStatus,
    string? EventDate,
    string? Source,
    string? Url);

/// A synonym the archive gives, pointing at a taxon with a status.
internal sealed record RedListSynonymRow(
    string TaxonId,
    string Name,
    string? CanonicalName,
    string? Authorship,
    string? TaxonomicStatus,
    string AcceptedTaxonId);

internal sealed record RedListSkips(int NoStatus, int NotEvaluated, int NoTaxon, int Misapplied);

internal sealed record RedListParse(IReadOnlyList<RedListTaxonRow> Taxa, IReadOnlyList<RedListSynonymRow> Synonyms, RedListSkips Skipped) {
    public int TaxaWithStatus => Taxa.Select(t => t.TaxonId).Distinct(StringComparer.Ordinal).Count();
}

internal static partial class RedListArchiveReader {
    public static RedListParse Read(string zipPath, RedListDataset dataset) {
        using var zip = ZipFile.OpenRead(zipPath);
        return Read(zip, dataset);
    }

    public static RedListParse Read(ZipArchive zip, RedListDataset dataset) {
        var meta = DwcArchive.ReadMeta(zip);
        if (!string.Equals(meta.Core.RowType, "Taxon", StringComparison.OrdinalIgnoreCase)) {
            throw new InvalidDataException($"The archive's core is {meta.Core.RowType}, not Taxon.");
        }
        var distribution = meta.Extension("Distribution")
                           ?? throw new InvalidDataException("The archive has no Distribution extension.");
        if (!distribution.Fields.ContainsKey("threatStatus") && !distribution.Defaults.ContainsKey("threatStatus")) {
            throw new InvalidDataException("The archive's Distribution extension has no threatStatus.");
        }
        var core = DwcArchive.ReadRows(zip, meta.Core).Select(CoreTaxon.From).OfType<CoreTaxon>().ToList();
        return Build(core, DwcArchive.ReadRows(zip, distribution), dataset);
    }

    internal static RedListParse Build(IReadOnlyList<CoreTaxon> core, IEnumerable<DwcRow> distribution, RedListDataset dataset) {
        var byId = new Dictionary<string, CoreTaxon>(StringComparer.Ordinal);
        var idOfTaxonId = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var taxon in core) {
            byId.TryAdd(taxon.Id, taxon);
            if (taxon.TaxonId is { } taxonId) {
                idOfTaxonId.TryAdd(taxonId, taxon.Id);
            }
        }
        // acceptedNameUsageID holds a taxonID, which is usually also the row's id.
        string? Accepted(CoreTaxon taxon) {
            if (taxon.AcceptedId is not { } accepted) {
                return null;
            }
            var id = byId.ContainsKey(accepted) ? accepted : idOfTaxonId.GetValueOrDefault(accepted, accepted);
            return id == taxon.Id ? null : id;
        }

        var taxa = new List<RedListTaxonRow>();
        var seq = new Dictionary<string, int>(StringComparer.Ordinal);
        int noStatus = 0, notEvaluated = 0, noTaxon = 0;
        foreach (var row in distribution) {
            var status = row["threatStatus"];
            if (status is null || status.Equals("NULL", StringComparison.OrdinalIgnoreCase)) {
                noStatus++;
                continue;
            }
            var code = RedListCategories.ToIucnCode(status);
            if (code == "NE") {
                notEvaluated++;
                continue;
            }
            if (row.Id is not { } id || !byId.TryGetValue(id, out var taxon)) {
                noTaxon++;
                continue;
            }
            var n = seq.GetValueOrDefault(id);
            seq[id] = n + 1;
            taxa.Add(new RedListTaxonRow(id, n, taxon.ScientificName, taxon.CanonicalName, taxon.Authorship, taxon.Rank, taxon.TaxonomicStatus,
                Accepted(taxon), taxon.AcceptedName, taxon.Kingdom ?? dataset.Kingdom, taxon.Phylum, taxon.Class, taxon.Order, taxon.Family,
                taxon.Genus, status, code, RedListCategories.Label(dataset.Categories, status), row["countryCode"], row["locality"],
                row["locationID"], row["establishmentMeans"], row["occurrenceStatus"], row["eventDate"] ?? row["temporal"], row["source"],
                taxon.Url ?? WholeUrl(row["source"])));
        }

        var withStatus = new HashSet<string>(taxa.Select(t => t.TaxonId), StringComparer.Ordinal);
        var synonyms = new List<RedListSynonymRow>();
        var misapplied = 0;
        foreach (var taxon in core) {
            if (Accepted(taxon) is not { } accepted || !withStatus.Contains(accepted)) {
                continue;
            }
            if (taxon.TaxonomicStatus?.Contains("misapplied", StringComparison.OrdinalIgnoreCase) == true) {
                misapplied++;
                continue;
            }
            synonyms.Add(new RedListSynonymRow(taxon.Id, taxon.ScientificName, taxon.CanonicalName, taxon.Authorship, taxon.TaxonomicStatus, accepted));
        }
        return new RedListParse(taxa, synonyms, new RedListSkips(noStatus, notEvaluated, noTaxon, misapplied));
    }

    /// The name without its authority: the scientific name less the authorship column when the name
    /// ends with it, then BareScientificName.Strip. Null when the result could be mistaken for
    /// another taxon's name: a hybrid formula or nothotaxon ("Elytrigia repens × Hordeum
    /// secalinum"), an unranked or group-rank row (Sweden's "Phocoena phocoena (Baltic
    /// population)", Denmark's "Sphagnum sect. Sphagnum"), or a species or infraspecific row that
    /// strips to a single word.
    internal static string? CanonicalName(string scientificName, string? authorship, string? rank) {
        var name = scientificName.Trim();
        if (authorship is { Length: > 0 } author && name.Length > author.Length && name.EndsWith(author, StringComparison.Ordinal)) {
            name = name[..^author.Length].Trim();
        }
        if (name.Contains('×') || name.Contains(" x ", StringComparison.Ordinal) || name.StartsWith("x ", StringComparison.Ordinal)) {
            // A nothospecies ("Mentha × gracilis") keeps its name; a formula of two parents does not.
            return Nothospecies().Match(name) is { Success: true } hybrid
                ? $"{hybrid.Groups["genus"].Value} × {hybrid.Groups["epithet"].Value}"
                : null;
        }
        var level = RankLevel(rank);
        if (level == RankKind.Other) {
            return null;
        }
        var bare = BareScientificName.Strip(name);
        var words = bare.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length;
        // With no rank, a name whose second word is lower case is a species or below.
        var nameWords = name.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var looksLikeSpecies = level is RankKind.Species or RankKind.Infraspecific
                               || level == RankKind.Unknown && nameWords.Length > 1 && char.IsLower(nameWords[1][0]);
        return words switch {
            0 => null,
            1 when looksLikeSpecies => null,
            > 1 when level == RankKind.Genus => null,
            _ => bare,
        };
    }

    // "Mentha × gracilis", "Salix x rubens Schrank": a genus, the hybrid sign and one epithet, and no
    // second hybrid sign after it.
    [GeneratedRegex(@"^(?<genus>[A-Z][a-z-]+)\s*(?:×|x(?=\s))\s*(?<epithet>[a-z-]{2,})(?![^×]*×)(?!\s+[a-z])")]
    private static partial Regex Nothospecies();

    internal enum RankKind { Unknown, Genus, Species, Infraspecific, Other }

    // The ranks seen in the archives, in their languages: species (Especie, Art), below species
    // (Subespecie, Underart, Varietet, varietas, infraspecificname) and genus. Any other named rank
    // (section, family, unranked, hybrid) is Other.
    internal static RankKind RankLevel(string? rank) {
        if (string.IsNullOrWhiteSpace(rank)) {
            return RankKind.Unknown;
        }
        return rank.Trim().ToLowerInvariant() switch {
            "genus" or "género" or "genero" or "slekt" or "släkte" or "slægt" => RankKind.Genus,
            "species" or "especie" or "espèce" or "art" or "speciesaggregate" or "aggregate" => RankKind.Species,
            "subspecies" or "subespecie" or "underart" or "sous-espèce" or "variety" or "varietas" or "varietet" or "variedad" or "var."
                or "form" or "forma" or "f." or "infraspecificname" or "infraspecific" or "subsp." or "ssp." => RankKind.Infraspecific,
            _ => RankKind.Other,
        };
    }

    // A value that is a URL and nothing else ("https://arter.dk//taxa/57859"); a citation that ends
    // with a URL is not one.
    internal static string? WholeUrl(string? value) =>
        value is { Length: > 0 } text && !text.Contains(' ')
        && (text.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || text.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        && Uri.TryCreate(text, UriKind.Absolute, out _)
            ? text
            : null;

    /// The core taxon fields the store keeps. Url: the taxon's page at the publisher, from
    /// references or bibliographicCitation when either is a URL.
    internal sealed record CoreTaxon(
        string Id,
        string? TaxonId,
        string ScientificName,
        string? CanonicalName,
        string? Authorship,
        string? Rank,
        string? TaxonomicStatus,
        string? AcceptedId,
        string? AcceptedName,
        string? Kingdom,
        string? Phylum,
        string? Class,
        string? Order,
        string? Family,
        string? Genus,
        string? Url) {
        public static CoreTaxon? From(DwcRow row) {
            var id = row.Id ?? row["taxonID"];
            var name = row["scientificName"];
            if (id is null || name is null) {
                return null;
            }
            var authorship = row["scientificNameAuthorship"];
            var rank = row["taxonRank"];
            return new CoreTaxon(id, row["taxonID"], name, row["canonicalName"] ?? RedListArchiveReader.CanonicalName(name, authorship, rank), authorship, rank,
                row["taxonomicStatus"], row["acceptedNameUsageID"], row["acceptedNameUsage"], row["kingdom"], row["phylum"], row["class"],
                row["order"], row["family"], row["genus"], WholeUrl(row["references"]) ?? WholeUrl(row["bibliographicCitation"]));
        }
    }
}
