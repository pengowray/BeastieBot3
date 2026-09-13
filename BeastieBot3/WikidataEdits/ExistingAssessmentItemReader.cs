using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Data.Sqlite;
using BeastieBot3.Wikidata;

// Reads the IUCN assessment items already on Wikidata from the Wikidata cache, for the status
// dry run. Opens the database read-only and does no schema work, so it is safe while serve or a
// cache run holds the file. The table is filled by `wikidata iucn-assessment-items`.

namespace BeastieBot3.WikidataEdits;

internal sealed record ExistingAssessmentItemSet(
    bool TableFound,
    IReadOnlyList<WikidataAssessmentItemRow> Rows) {

    /// Every stored item, including taxon items that carry an assessment DOI by mistake (see
    /// PublicationItems for the ones a reference can cite).
    public IReadOnlyList<ExistingAssessmentItem> All =>
        Rows.Select(r => r.ToExistingAssessmentItem()).ToList();

    /// Items that are publications, not taxa. Q1272830 (a petrel) carries an assessment DOI on the
    /// taxon item itself; its DOI is taken but the item is not something to cite.
    public IReadOnlyList<ExistingAssessmentItem> PublicationItems =>
        Rows.Where(r => !r.IsTaxonItem).Select(r => r.ToExistingAssessmentItem()).ToList();

    /// Publication items by assessment id. More than one item per id is possible (duplicates on Wikidata).
    public ILookup<long, ExistingAssessmentItem> ByAssessmentId =>
        PublicationItems.Where(i => i.AssessmentId is not null).ToLookup(i => i.AssessmentId!.Value);
}

internal static class ExistingAssessmentItemReader {
    /// Empty (TableFound false) when the cache file or the table doesn't exist yet.
    public static ExistingAssessmentItemSet Read(string wikidataCachePath) {
        if (string.IsNullOrWhiteSpace(wikidataCachePath) || !File.Exists(wikidataCachePath)) {
            return new ExistingAssessmentItemSet(false, Array.Empty<WikidataAssessmentItemRow>());
        }

        var builder = new SqliteConnectionStringBuilder {
            DataSource = wikidataCachePath,
            Mode = SqliteOpenMode.ReadOnly,
            Pooling = false,
        };
        using var connection = new SqliteConnection(builder.ConnectionString);
        connection.Open();
        return Read(connection);
    }

    public static ExistingAssessmentItemSet Read(SqliteConnection connection) =>
        WikidataAssessmentItemTable.Exists(connection)
            ? new ExistingAssessmentItemSet(true, WikidataAssessmentItemTable.ReadAll(connection))
            : new ExistingAssessmentItemSet(false, Array.Empty<WikidataAssessmentItemRow>());
}
