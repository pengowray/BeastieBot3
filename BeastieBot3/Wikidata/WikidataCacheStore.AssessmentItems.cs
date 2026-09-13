using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;

// The wikidata_iucn_assessment_items table of the Wikidata cache: IUCN assessment publication
// items found on Wikidata (see WikidataAssessmentItems.cs). Kept in its own file so the table,
// its upsert and the test seam don't churn the main store file.

namespace BeastieBot3.Wikidata;

internal sealed partial class WikidataCacheStore {
    // Another process (serve, a cache run) may hold the database, so writes go in short
    // transactions of this many rows rather than one long one.
    private const int AssessmentItemWriteBatch = 250;

    /// Test seam: build the schema on a connection the caller owns (see SqliteStore).
    internal static WikidataCacheStore OpenFromConnection(SqliteConnection connection) {
        EnableForeignKeys(connection);
        var store = new WikidataCacheStore(connection);
        store.EnsureImportSchema();
        store.EnsureSchema();
        return store;
    }

    private void EnsureAssessmentItemSchema() {
        using var command = _connection.CreateCommand();
        command.CommandText = WikidataAssessmentItemTable.Ddl;
        command.ExecuteNonQuery();
    }

    /// Inserts or replaces each item's row. first_seen_at is kept from the existing row.
    public void UpsertAssessmentItems(IReadOnlyList<WikidataAssessmentItemRow> rows) {
        foreach (var chunk in rows.Chunk(AssessmentItemWriteBatch)) {
            using var tx = _connection.BeginTransaction();
            using var command = _connection.CreateCommand();
            command.Transaction = tx;
            command.CommandText =
                """
INSERT INTO wikidata_iucn_assessment_items(
    qid, qid_numeric, doi, all_dois, taxon_id, assessment_id, doi_release, doi_language, id_source,
    title, label_en, instance_of, main_subjects, published_in, publication_date, publication_year,
    author_item_count, author_string_count, urls, found_by, source_endpoint, modified_at,
    fetched_at, first_seen_at)
VALUES (
    @qid, @qidNumeric, @doi, @allDois, @taxon, @assessment, @release, @language, @idSource,
    @title, @label, @instanceOf, @mainSubjects, @publishedIn, @date, @year,
    @authorItems, @authorStrings, @urls, @foundBy, @endpoint, @modified,
    @fetched, @fetched)
ON CONFLICT(qid) DO UPDATE SET
    qid_numeric = excluded.qid_numeric,
    doi = excluded.doi,
    all_dois = excluded.all_dois,
    taxon_id = excluded.taxon_id,
    assessment_id = excluded.assessment_id,
    doi_release = excluded.doi_release,
    doi_language = excluded.doi_language,
    id_source = excluded.id_source,
    title = excluded.title,
    label_en = excluded.label_en,
    instance_of = excluded.instance_of,
    main_subjects = excluded.main_subjects,
    published_in = excluded.published_in,
    publication_date = excluded.publication_date,
    publication_year = excluded.publication_year,
    author_item_count = excluded.author_item_count,
    author_string_count = excluded.author_string_count,
    urls = excluded.urls,
    found_by = excluded.found_by,
    source_endpoint = excluded.source_endpoint,
    modified_at = excluded.modified_at,
    fetched_at = excluded.fetched_at
""";
            var qid = command.Parameters.Add("@qid", SqliteType.Text);
            var qidNumeric = command.Parameters.Add("@qidNumeric", SqliteType.Integer);
            var doi = command.Parameters.Add("@doi", SqliteType.Text);
            var allDois = command.Parameters.Add("@allDois", SqliteType.Text);
            var taxon = command.Parameters.Add("@taxon", SqliteType.Integer);
            var assessment = command.Parameters.Add("@assessment", SqliteType.Integer);
            var release = command.Parameters.Add("@release", SqliteType.Text);
            var language = command.Parameters.Add("@language", SqliteType.Text);
            var idSource = command.Parameters.Add("@idSource", SqliteType.Text);
            var title = command.Parameters.Add("@title", SqliteType.Text);
            var label = command.Parameters.Add("@label", SqliteType.Text);
            var instanceOf = command.Parameters.Add("@instanceOf", SqliteType.Text);
            var mainSubjects = command.Parameters.Add("@mainSubjects", SqliteType.Text);
            var publishedIn = command.Parameters.Add("@publishedIn", SqliteType.Text);
            var date = command.Parameters.Add("@date", SqliteType.Text);
            var year = command.Parameters.Add("@year", SqliteType.Integer);
            var authorItems = command.Parameters.Add("@authorItems", SqliteType.Integer);
            var authorStrings = command.Parameters.Add("@authorStrings", SqliteType.Integer);
            var urls = command.Parameters.Add("@urls", SqliteType.Text);
            var foundBy = command.Parameters.Add("@foundBy", SqliteType.Text);
            var endpoint = command.Parameters.Add("@endpoint", SqliteType.Text);
            var modified = command.Parameters.Add("@modified", SqliteType.Text);
            var fetched = command.Parameters.Add("@fetched", SqliteType.Text);

            foreach (var row in chunk) {
                qid.Value = row.Qid;
                qidNumeric.Value = long.TryParse(row.Qid.AsSpan(1), out var numeric) ? numeric : 0L;
                doi.Value = Db(row.Doi);
                allDois.Value = Db(WikidataAssessmentItemTable.JoinList(row.AllDois));
                taxon.Value = row.TaxonId is { } t ? t : DBNull.Value;
                assessment.Value = row.AssessmentId is { } a ? a : DBNull.Value;
                release.Value = Db(row.DoiRelease);
                language.Value = Db(row.DoiLanguage);
                idSource.Value = Db(row.IdSource);
                title.Value = Db(row.Title);
                label.Value = Db(row.LabelEn);
                instanceOf.Value = Db(WikidataAssessmentItemTable.JoinList(row.InstanceOf));
                mainSubjects.Value = Db(WikidataAssessmentItemTable.JoinList(row.MainSubjects));
                publishedIn.Value = Db(WikidataAssessmentItemTable.JoinList(row.PublishedIn));
                date.Value = Db(row.PublicationDate);
                year.Value = row.PublicationYear is { } y ? y : DBNull.Value;
                authorItems.Value = row.AuthorItemCount;
                authorStrings.Value = row.AuthorStringCount;
                urls.Value = Db(WikidataAssessmentItemTable.JoinList(row.Urls));
                foundBy.Value = Db(WikidataAssessmentItemTable.JoinList(row.FoundBy));
                endpoint.Value = Db(row.SourceEndpoint);
                modified.Value = Db(row.ModifiedAt);
                fetched.Value = row.FetchedAtUtc.ToUniversalTime().ToString("O");
                command.ExecuteNonQuery();
            }

            tx.Commit();
        }
    }

    public IReadOnlyList<WikidataAssessmentItemRow> ReadAssessmentItems() =>
        WikidataAssessmentItemTable.ReadAll(_connection);

    private static object Db(string? value) => value is null ? DBNull.Value : value;
}
