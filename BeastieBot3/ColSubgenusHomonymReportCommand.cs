using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

// test example report; doesn't work / runs too slow; probably the LOWER commands.

public sealed class ColSubgenusHomonymReportCommand : Command<CommonSettings>
{
    // Canonicalize genus and subgenus names and join on the cleaned value to locate potential homonyms.
    private const string ReportSql = @"
WITH subg AS (
    SELECT
        ID,
        scientificName,
        authorship,
        status,
        kingdom,
        nameRemarks,
        remarks,
        parentID,
        basionymID,
        LOWER(
            COALESCE(
                infragenericEpithet,
                subgenus,
                uninomial,
                genericName
            )
        ) AS canonical_name
    FROM nameusage
    WHERE LOWER(rank) = 'subgenus'
), genus AS (
    SELECT
        ID,
        scientificName,
        authorship,
        status,
        kingdom,
        nameRemarks,
        remarks,
        parentID,
        basionymID,
        LOWER(
            COALESCE(
                uninomial,
                genus
            )
        ) AS canonical_name
    FROM nameusage
    WHERE LOWER(rank) = 'genus'
)
SELECT
    s.canonical_name AS shared_name,
    s.ID AS subgenus_id,
    s.scientificName AS subgenus_name,
    s.authorship AS subgenus_authorship,
    s.status AS subgenus_status,
    s.kingdom AS subgenus_kingdom,
    s.parentID AS subgenus_parent_id,
    s.basionymID AS subgenus_basionym_id,
    s.nameRemarks AS subgenus_name_remarks,
    s.remarks AS subgenus_remarks,
    g.ID AS genus_id,
    g.scientificName AS genus_name,
    g.authorship AS genus_authorship,
    g.status AS genus_status,
    g.kingdom AS genus_kingdom,
    g.parentID AS genus_parent_id,
    g.basionymID AS genus_basionym_id,
    g.nameRemarks AS genus_name_remarks,
    g.remarks AS genus_remarks
FROM subg s
JOIN genus g ON s.canonical_name = g.canonical_name
WHERE s.canonical_name IS NOT NULL
  AND g.canonical_name IS NOT NULL
  AND COALESCE(s.ID, '') <> COALESCE(g.ID, '')
ORDER BY shared_name, subgenus_name, genus_name;";

    public override int Execute(CommandContext context, CommonSettings settings, CancellationToken cancellationToken)
    {
        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        var dbPath = paths.GetColSqlitePath();
        if (string.IsNullOrWhiteSpace(dbPath))
        {
            AnsiConsole.MarkupLine("[red]COL_sqlite path is not configured. Set [bold]Datastore:COL_sqlite[/] in paths.ini.[/]");
            return -1;
        }

        if (!File.Exists(dbPath))
        {
            AnsiConsole.MarkupLine($"[red]COL SQLite database not found at:[/] {Markup.Escape(dbPath)}");
            return -2;
        }

        AnsiConsole.MarkupLine($"[grey]Using settings from:[/] {Markup.Escape(paths.SourceFilePath)}");
        AnsiConsole.MarkupLine($"[grey]Reading Catalogue of Life data from:[/] {Markup.Escape(dbPath)}");

        var rows = new List<ReportRow>();

        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = ReportSql;

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            cancellationToken.ThrowIfCancellationRequested();
            rows.Add(new ReportRow(
                SharedName: ReadString(reader, "shared_name"),
                SubgenusId: ReadString(reader, "subgenus_id"),
                SubgenusName: ReadString(reader, "subgenus_name"),
                SubgenusAuthorship: ReadString(reader, "subgenus_authorship"),
                SubgenusStatus: ReadString(reader, "subgenus_status"),
                SubgenusKingdom: ReadString(reader, "subgenus_kingdom"),
                SubgenusParentId: ReadString(reader, "subgenus_parent_id"),
                SubgenusBasionymId: ReadString(reader, "subgenus_basionym_id"),
                SubgenusNameRemarks: ReadString(reader, "subgenus_name_remarks"),
                SubgenusRemarks: ReadString(reader, "subgenus_remarks"),
                GenusId: ReadString(reader, "genus_id"),
                GenusName: ReadString(reader, "genus_name"),
                GenusAuthorship: ReadString(reader, "genus_authorship"),
                GenusStatus: ReadString(reader, "genus_status"),
                GenusKingdom: ReadString(reader, "genus_kingdom"),
                GenusParentId: ReadString(reader, "genus_parent_id"),
                GenusBasionymId: ReadString(reader, "genus_basionym_id"),
                GenusNameRemarks: ReadString(reader, "genus_name_remarks"),
                GenusRemarks: ReadString(reader, "genus_remarks")
            ));
        }

        if (rows.Count == 0)
        {
            AnsiConsole.MarkupLine("[green]No subgenus entries share their name with a genus.[/]");
            return 0;
        }

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Shared Name");
        table.AddColumn("Subgenus");
        table.AddColumn("Genus");

        foreach (var row in rows)
        {
            var sharedName = FormatValue(row.SharedName);
            var subgenusBlock = BuildEntityBlock(
                ("ID", row.SubgenusId),
                ("Scientific", row.SubgenusName),
                ("Authorship", row.SubgenusAuthorship),
                ("Status", row.SubgenusStatus),
                ("Kingdom", row.SubgenusKingdom),
                ("Parent", row.SubgenusParentId),
                ("Basionym", row.SubgenusBasionymId),
                ("Name remarks", row.SubgenusNameRemarks),
                ("Remarks", row.SubgenusRemarks)
            );
            var genusBlock = BuildEntityBlock(
                ("ID", row.GenusId),
                ("Scientific", row.GenusName),
                ("Authorship", row.GenusAuthorship),
                ("Status", row.GenusStatus),
                ("Kingdom", row.GenusKingdom),
                ("Parent", row.GenusParentId),
                ("Basionym", row.GenusBasionymId),
                ("Name remarks", row.GenusNameRemarks),
                ("Remarks", row.GenusRemarks)
            );

            table.AddRow(sharedName, subgenusBlock, genusBlock);
        }

        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine($"[grey]Potential homonyms found:[/] {rows.Count}");

        return 0;
    }

    private static string? ReadString(SqliteDataReader reader, string columnName)
    {
        var ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static string FormatValue(string? value) => string.IsNullOrWhiteSpace(value) ? "-" : Markup.Escape(value.Trim());

    private static string BuildEntityBlock(params (string Label, string? Value)[] fields)
    {
        var sb = new StringBuilder();
        foreach (var (label, value) in fields)
        {
            var trimmed = value?.Trim();
            if (string.IsNullOrWhiteSpace(trimmed))
            {
                continue;
            }

            sb.Append(label)
                .Append(": ")
                .Append(trimmed)
                .AppendLine();
        }

        if (sb.Length == 0)
        {
            return "-";
        }

        return Markup.Escape(sb.ToString().TrimEnd());
    }

    private sealed record ReportRow(
        string? SharedName,
        string? SubgenusId,
        string? SubgenusName,
        string? SubgenusAuthorship,
        string? SubgenusStatus,
        string? SubgenusKingdom,
        string? SubgenusParentId,
        string? SubgenusBasionymId,
        string? SubgenusNameRemarks,
        string? SubgenusRemarks,
        string? GenusId,
        string? GenusName,
        string? GenusAuthorship,
        string? GenusStatus,
        string? GenusKingdom,
        string? GenusParentId,
        string? GenusBasionymId,
        string? GenusNameRemarks,
        string? GenusRemarks
    );
}
