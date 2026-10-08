using Microsoft.Data.Sqlite;

// The status lists store's CITES tables (`statuses cites-import`): cites_taxon, cites_listing,
// cites_note and cites_synonym. A long note (a full note, the text of a #4 annotation) is stored once
// in cites_note: the same texts are repeated on tens of thousands of listings (about 90 MB of the
// 140 MB download).

namespace BeastieBot3.StatusLists;

internal sealed partial class StatusListStore {
    public long CountCitesTaxa() => Scalar("SELECT COUNT(*) FROM cites_taxon");

    public long CountCitesListings() => Scalar("SELECT COUNT(*) FROM cites_listing");

    /// Replaces every CITES taxon, listing, note and synonym, and the CITES source row, in one
    /// transaction.
    public void ReplaceCites(IReadOnlyList<CitesTaxon> taxa, DateTime importedAtUtc, StatusSourceInfo source) {
        using var tx = _connection.BeginTransaction();
        Execute(tx, "DELETE FROM cites_synonym");
        Execute(tx, "DELETE FROM cites_listing");
        Execute(tx, "DELETE FROM cites_note");
        Execute(tx, "DELETE FROM cites_taxon");

        using var insertTaxon = _connection.CreateCommand();
        insertTaxon.Transaction = tx;
        insertTaxon.CommandText = """
            INSERT INTO cites_taxon(taxon_concept_id, full_name, author_year, taxon_rank, cites_accepted, kingdom, phylum, taxclass, taxorder,
                family, genus, current_listing, url, imported_at)
            VALUES (@id, @name, @author, @rank, @accepted, @kingdom, @phylum, @class, @order, @family, @genus, @listing, @url, @imported)
            ON CONFLICT(taxon_concept_id) DO NOTHING
            """;
        var taxonNames = new[] { "@id", "@name", "@author", "@rank", "@accepted", "@kingdom", "@phylum", "@class", "@order", "@family", "@genus",
            "@listing", "@url" };
        var t = taxonNames.ToDictionary(n => n, n => AddParameter(insertTaxon, n), StringComparer.Ordinal);
        insertTaxon.Parameters.AddWithValue("@imported", Stamp(importedAtUtc));

        using var insertListing = _connection.CreateCommand();
        insertListing.Transaction = tx;
        insertListing.CommandText = """
            INSERT INTO cites_listing(taxon_concept_id, listing_change_id, appendix, party_iso_code, party_name, effective_on, short_note,
                full_note_id, annotation_symbol, annotation_note_id, inherited_rank, inherited_name, inherited_from_id, inherited_short_note,
                inherited_full_note_id, nomenclature_note)
            VALUES (@taxon, @change, @appendix, @party, @party_name, @effective, @short, @full, @symbol, @annotation, @inherited_rank,
                @inherited_name, @inherited_from, @inherited_short, @inherited_full, @nomenclature)
            ON CONFLICT(taxon_concept_id, listing_change_id) DO NOTHING
            """;
        var listingNames = new[] { "@taxon", "@change", "@appendix", "@party", "@party_name", "@effective", "@short", "@full", "@symbol",
            "@annotation", "@inherited_rank", "@inherited_name", "@inherited_from", "@inherited_short", "@inherited_full", "@nomenclature" };
        var l = listingNames.ToDictionary(n => n, n => AddParameter(insertListing, n), StringComparer.Ordinal);

        using var insertNote = _connection.CreateCommand();
        insertNote.Transaction = tx;
        insertNote.CommandText = "INSERT INTO cites_note(note_id, html) VALUES (@id, @html)";
        var noteId = insertNote.Parameters.Add("@id", SqliteType.Integer);
        var noteHtml = insertNote.Parameters.Add("@html", SqliteType.Text);
        var notes = new Dictionary<string, long>(StringComparer.Ordinal);
        object Note(string? html) {
            if (html is null) {
                return DBNull.Value;
            }
            if (!notes.TryGetValue(html, out var id)) {
                id = notes.Count + 1;
                notes[html] = id;
                noteId.Value = id;
                noteHtml.Value = html;
                insertNote.ExecuteNonQuery();
            }
            return id;
        }

        using var insertSynonym = _connection.CreateCommand();
        insertSynonym.Transaction = tx;
        insertSynonym.CommandText = """
            INSERT INTO cites_synonym(taxon_concept_id, name_with_author, name, author) VALUES (@taxon, @raw, @name, @author)
            ON CONFLICT DO NOTHING
            """;
        var synonymTaxon = insertSynonym.Parameters.Add("@taxon", SqliteType.Integer);
        var synonymRaw = insertSynonym.Parameters.Add("@raw", SqliteType.Text);
        var synonymName = insertSynonym.Parameters.Add("@name", SqliteType.Text);
        var synonymAuthor = insertSynonym.Parameters.Add("@author", SqliteType.Text);

        static object Value(object? v) => v ?? DBNull.Value;
        foreach (var taxon in taxa) {
            t["@id"].Value = taxon.TaxonConceptId;
            t["@name"].Value = taxon.FullName;
            t["@author"].Value = Value(taxon.AuthorYear);
            t["@rank"].Value = taxon.Rank;
            t["@accepted"].Value = taxon.CitesAccepted ? 1 : 0;
            t["@kingdom"].Value = Value(taxon.Kingdom);
            t["@phylum"].Value = Value(taxon.Phylum);
            t["@class"].Value = Value(taxon.TaxClass);
            t["@order"].Value = Value(taxon.TaxOrder);
            t["@family"].Value = Value(taxon.Family);
            t["@genus"].Value = Value(taxon.Genus);
            t["@listing"].Value = Value(taxon.CurrentListing);
            t["@url"].Value = CitesChecklist.SpeciesPlusUrl(taxon.TaxonConceptId);
            if (insertTaxon.ExecuteNonQuery() == 0) {
                // A second row with the same id: the first one is kept, with its listings and synonyms.
                continue;
            }

            foreach (var listing in taxon.Listings) {
                l["@taxon"].Value = taxon.TaxonConceptId;
                l["@change"].Value = listing.ListingChangeId;
                l["@appendix"].Value = listing.Appendix;
                l["@party"].Value = Value(listing.PartyIsoCode);
                l["@party_name"].Value = Value(listing.PartyName);
                l["@effective"].Value = Value(listing.EffectiveOn);
                l["@short"].Value = Value(listing.ShortNote);
                l["@full"].Value = Note(listing.FullNote);
                l["@symbol"].Value = Value(listing.AnnotationSymbol);
                l["@annotation"].Value = Note(listing.AnnotationNote);
                l["@inherited_rank"].Value = Value(listing.InheritedRank);
                l["@inherited_name"].Value = Value(listing.InheritedName);
                l["@inherited_from"].Value = Value(listing.InheritedFromId);
                l["@inherited_short"].Value = Value(listing.InheritedShortNote);
                l["@inherited_full"].Value = Note(listing.InheritedFullNote);
                l["@nomenclature"].Value = Value(listing.NomenclatureNote);
                insertListing.ExecuteNonQuery();
            }

            synonymTaxon.Value = taxon.TaxonConceptId;
            foreach (var synonym in taxon.Synonyms) {
                synonymRaw.Value = synonym.NameWithAuthor;
                synonymName.Value = synonym.Name;
                synonymAuthor.Value = Value(synonym.Author);
                insertSynonym.ExecuteNonQuery();
            }
        }
        UpsertSource(source, tx);
        tx.Commit();
    }
}
