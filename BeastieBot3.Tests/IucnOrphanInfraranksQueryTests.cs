using System.Collections.Generic;
using BeastieBot3.Iucn;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// The orphan subspecies/variety query (iucn report-orphan-infraranks and the audit site's
// orphan-infraranks page). It reads the species-rank pairs once through a materialised CTE; these
// tests pin that it returns exactly the rows of the correlated NOT EXISTS it replaced, including
// blank and NULL ranks, subpopulation-only parents and NULL genus or species names.
public class IucnOrphanInfraranksQueryTests {
    // The query before the CTE, kept here as the reference.
    private const string CorrelatedQuery = """
        SELECT i.taxonId
        FROM view_assessments_html_taxonomy_html i
        WHERE i.infraType IS NOT NULL AND TRIM(i.infraType) <> ''
          AND NOT EXISTS (
            SELECT 1 FROM view_assessments_html_taxonomy_html p
            WHERE p.genusName = i.genusName
              AND p.speciesName = i.speciesName
              AND (p.infraType IS NULL OR TRIM(p.infraType) = '')
              AND (p.subpopulationName IS NULL OR TRIM(p.subpopulationName) = '')
          )
        ORDER BY i.kingdomName, i.className, i.orderName, i.familyName, i.scientificName, i.taxonId
        """;

    private static SqliteConnection Fixture() {
        var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE view_assessments_html_taxonomy_html (taxonId INTEGER, scientificName TEXT,
                kingdomName TEXT, className TEXT, orderName TEXT, familyName TEXT,
                genusName TEXT, speciesName TEXT, infraType TEXT, subpopulationName TEXT);
            INSERT INTO view_assessments_html_taxonomy_html VALUES
                -- Aus bus: assessed species (NULL ranks), so its subspecies is not an orphan.
                (1, 'Aus bus', 'ANIMALIA', 'AVES', 'O', 'F', 'Aus', 'bus', NULL, NULL),
                (2, 'Aus bus alpha', 'ANIMALIA', 'AVES', 'O', 'F', 'Aus', 'bus', 'ssp.', NULL),
                -- Cus dus: species row with blank ranks, so its variety is not an orphan.
                (3, 'Cus dus', 'PLANTAE', 'MAGNOLIOPSIDA', 'O', 'F', 'Cus', 'dus', ' ', ''),
                (4, 'Cus dus var. beta', 'PLANTAE', 'MAGNOLIOPSIDA', 'O', 'F', 'Cus', 'dus', 'var.', NULL),
                -- Eus fus: only a subpopulation of the species is assessed, so both infraranks are orphans.
                (5, 'Eus fus', 'ANIMALIA', 'MAMMALIA', 'O', 'F', 'Eus', 'fus', NULL, 'Baltic Sea'),
                (6, 'Eus fus gamma', 'ANIMALIA', 'MAMMALIA', 'O', 'F', 'Eus', 'fus', 'ssp.', NULL),
                (7, 'Eus fus delta', 'ANIMALIA', 'MAMMALIA', 'O', 'F', 'Eus', 'fus', 'subsp.', NULL),
                -- Gus hus: no species row at all; the subspecies also has a regional row.
                (8, 'Gus hus epsilon', 'ANIMALIA', 'AVES', 'O', 'F', 'Gus', 'hus', 'ssp.', NULL),
                (8, 'Gus hus epsilon', 'ANIMALIA', 'AVES', 'O', 'F', 'Gus', 'hus', 'ssp.', 'Europe'),
                -- A species row with no genus name matches nothing, so a subspecies with no genus is an orphan.
                (9, 'Ius jus', 'ANIMALIA', 'AVES', 'O', 'F', NULL, 'jus', NULL, NULL),
                (10, 'Ius jus zeta', 'ANIMALIA', 'AVES', 'O', 'F', NULL, 'jus', 'ssp.', NULL),
                -- Names compare exactly: a species row 'Kus lus' does not cover 'kus lus'.
                (11, 'Kus lus', 'ANIMALIA', 'AVES', 'O', 'F', 'Kus', 'lus', NULL, NULL),
                (12, 'kus lus eta', 'ANIMALIA', 'AVES', 'O', 'F', 'kus', 'lus', 'ssp.', NULL),
                -- Blank infraType is a species row, not an infrarank.
                (13, 'Mus nus', 'ANIMALIA', 'AVES', 'O', 'F', 'Mus', 'nus', '', NULL);
            """;
        command.ExecuteNonQuery();
        return connection;
    }

    private static List<long> TaxonIds(SqliteConnection connection, string sql) {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var ids = new List<long>();
        while (reader.Read()) {
            ids.Add(reader.GetInt64(0));
        }
        return ids;
    }

    [Fact]
    public void OrphanQuery_FindsInfraranksWithNoSpeciesRow() {
        using var connection = Fixture();
        var ids = TaxonIds(connection, IucnOrphanInfraranksReportCommand.OrphanQuery("i.taxonId"));
        // Ordered by kingdom, class, order, family, scientific name, taxon id.
        Assert.Equal(new long[] { 8, 8, 10, 12, 7, 6 }, ids);
    }

    [Fact]
    public void OrphanQuery_ReturnsTheSameRowsAsTheCorrelatedQuery() {
        using var connection = Fixture();
        Assert.Equal(TaxonIds(connection, CorrelatedQuery),
            TaxonIds(connection, IucnOrphanInfraranksReportCommand.OrphanQuery("i.taxonId")));
    }

    [Fact]
    public void OrphanQuery_AcceptsALimit() {
        using var connection = Fixture();
        var ids = TaxonIds(connection, IucnOrphanInfraranksReportCommand.OrphanQuery("i.taxonId") + "\nLIMIT 2");
        Assert.Equal(new long[] { 8, 8 }, ids);
    }
}
