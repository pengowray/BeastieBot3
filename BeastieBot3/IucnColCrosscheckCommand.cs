using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Data.Sqlite;
using Spectre.Console;
using Spectre.Console.Cli;

namespace BeastieBot3;

public sealed class IucnColCrosscheckCommand : Command<IucnColCrosscheckCommand.Settings> {
    public sealed class Settings : CommandSettings {
        [CommandOption("-s|--settings-dir <DIR>")]
        [Description("Directory containing settings files like paths.ini. Defaults to the app base directory.")]
        public string? SettingsDir { get; init; }

        [CommandOption("--ini-file <FILE>")]
        [Description("INI filename to read. Defaults to paths.ini.")]
        public string? IniFile { get; init; }

        [CommandOption("--iucn-database <PATH>")]
        [Description("Explicit IUCN SQLite database path. Overrides Datastore:IUCN_sqlite_from_cvs.")]
        public string? IucnDatabasePath { get; init; }

        [CommandOption("--col-database <PATH>")]
        [Description("Explicit Catalogue of Life SQLite database path. Overrides Datastore:COL_sqlite.")]
        public string? ColDatabasePath { get; init; }

        [CommandOption("--limit <ROWS>")]
        [Description("Maximum number of IUCN rows to inspect (0 = all).")]
        public long Limit { get; init; }

        [CommandOption("--max-samples <COUNT>")]
        [Description("Maximum number of examples to display per finding category.")]
        public int MaxSamples { get; init; } = 10;
    }

    public override int Execute(CommandContext context, Settings settings, CancellationToken cancellationToken) {
        if (settings.MaxSamples <= 0) {
            AnsiConsole.MarkupLine("[red]--max-samples must be greater than zero.[/]");
            return -1;
        }

        var baseDir = settings.SettingsDir ?? AppContext.BaseDirectory;
        var iniFile = settings.IniFile ?? "paths.ini";
        var paths = new PathsService(iniFile, baseDir);

        string iucnDatabasePath;
        try {
            iucnDatabasePath = paths.ResolveIucnDatabasePath(settings.IucnDatabasePath);
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return -2;
        }

        string colDatabasePath;
        try {
            colDatabasePath = ResolveColDatabasePath(paths, settings.ColDatabasePath);
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return -3;
        }

        if (!File.Exists(iucnDatabasePath)) {
            AnsiConsole.MarkupLine($"[red]IUCN SQLite database not found at:[/] {Markup.Escape(iucnDatabasePath)}");
            return -4;
        }

        if (!File.Exists(colDatabasePath)) {
            AnsiConsole.MarkupLine($"[red]Catalogue of Life SQLite database not found at:[/] {Markup.Escape(colDatabasePath)}");
            return -5;
        }

        var iucnConnectionString = new SqliteConnectionStringBuilder {
            DataSource = iucnDatabasePath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        var colConnectionString = new SqliteConnectionStringBuilder {
            DataSource = colDatabasePath,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString();

        using var iucnConnection = new SqliteConnection(iucnConnectionString);
        using var colConnection = new SqliteConnection(colConnectionString);
        iucnConnection.Open();
        colConnection.Open();

        var iucnRepository = new IucnTaxonomyRepository(iucnConnection);
        if (!iucnRepository.ObjectExists("view_assessments_html_taxonomy_html", "view")) {
            AnsiConsole.MarkupLine("[red]view_assessments_html_taxonomy_html not found. Re-run the IUCN importer to create the joined view.[/]");
            return -6;
        }

        var colRepository = new ColNameUsageRepository(colConnection);
        if (!colRepository.ObjectExists("nameusage", "table")) {
            AnsiConsole.MarkupLine("[red]nameusage table not found in the COL database.[/]");
            return -7;
        }

        if (!colRepository.SupportsAcceptedNameLookup) {
            var message = colRepository.UsesParentIdFallback
                ? "[yellow]COL dataset lacks acceptedNameUsageID; falling back to parentID for context only.[/]"
                : "[yellow]COL dataset lacks acceptedNameUsageID; accepted-name lookups will be skipped.[/]";
            AnsiConsole.MarkupLine(message);
        }

        var analysis = new IucnColCrosscheckAnalysis(settings.MaxSamples);

        try {
            foreach (var row in iucnRepository.ReadRows(settings.Limit, cancellationToken)) {
                cancellationToken.ThrowIfCancellationRequested();
                analysis.RegisterRow();

                if (!IsSpeciesCandidate(row)) {
                    analysis.RegisterSkipped();
                    continue;
                }

                var composition = ScientificNameComposer.Compose(row);
                if (composition.Classification != NameClassification.SpeciesOrHigher || composition.HasInfra) {
                    analysis.RegisterSkipped();
                    continue;
                }

                analysis.RegisterEvaluated();

                var displayName = DetermineDisplayName(row, composition);
                var normalizedName = NormalizeText(displayName);
                if (normalizedName is null) {
                    var missingSample = CreateSample(row, displayName, row.Authority, NormalizeText(row.Authority), null, null, null, null, null, null, 0, "No scientific name available for lookup.");
                    analysis.RegisterMissing(missingSample);
                    continue;
                }

                var matches = colRepository.FindByScientificName(normalizedName, cancellationToken);
                if (matches.Count == 0) {
                    matches = colRepository.FindByGenusSpecies(row.GenusName, row.SpeciesName, cancellationToken);
                }

                if (matches.Count == 0) {
                    var missingSample = CreateSample(row, displayName, row.Authority, NormalizeText(row.Authority), null, null, null, null, null, null, 0, "Not found in Catalogue of Life.");
                    analysis.RegisterMissing(missingSample);
                    continue;
                }

                analysis.RegisterFound();

                var selected = SelectPreferredMatch(matches);
                var acceptedEntry = GetAcceptedEntry(selected, colRepository, cancellationToken);

                var acceptedId = acceptedEntry?.Id ?? selected.AcceptedNameUsageId;
                var acceptedScientificName = acceptedEntry?.ScientificName;
                var acceptedAuthority = acceptedEntry?.Authorship;
                var normalizedIucnAuthority = NormalizeText(row.Authority);
                var normalizedColAuthority = NormalizeText(selected.Authorship);
                var normalizedAcceptedAuthority = NormalizeText(acceptedAuthority);

                var baseSample = CreateSample(
                    row,
                    displayName,
                    row.Authority,
                    normalizedIucnAuthority,
                    selected,
                    normalizedColAuthority,
                    acceptedId,
                    acceptedScientificName,
                    acceptedAuthority,
                    normalizedAcceptedAuthority,
                    matches.Count,
                    string.Empty
                );

                if (IsSynonymStatus(selected.Status)) {
                    var synonymDetail = BuildSynonymDetail(baseSample);
                    analysis.RegisterSynonym(baseSample with { Detail = synonymDetail });
                }

                if (AuthoritiesEqual(normalizedIucnAuthority, normalizedColAuthority)) {
                    analysis.RegisterAuthorityMatch();
                } else {
                    var mismatchDetail = BuildAuthorityMismatchDetail(baseSample);
                    analysis.RegisterAuthorityMismatch(baseSample with { Detail = mismatchDetail });
                }

                if (matches.Count > 1) {
                    var multipleDetail = BuildMultipleMatchesDetail(matches);
                    analysis.RegisterMultipleMatches(baseSample with { Detail = multipleDetail });
                }
            }
        } catch (OperationCanceledException) {
            throw;
        } catch (Exception ex) {
            AnsiConsole.MarkupLine($"[red]Crosscheck failed:[/] {Markup.Escape(ex.Message)}");
            return -8;
        }

        RenderResults(analysis, iucnDatabasePath, colDatabasePath, settings.Limit);
        return 0;
    }

    private static string ResolveColDatabasePath(PathsService paths, string? overridePath) {
        var configuredPath = !string.IsNullOrWhiteSpace(overridePath)
            ? overridePath
            : paths.GetColSqlitePath();

        if (string.IsNullOrWhiteSpace(configuredPath)) {
            throw new InvalidOperationException("Catalogue of Life SQLite path is not configured. Set Datastore:COL_sqlite or pass --col-database.");
        }

        return Path.GetFullPath(configuredPath);
    }

    private static bool IsSpeciesCandidate(IucnTaxonomyRow row) {
        if (!string.IsNullOrWhiteSpace(row.SubpopulationName)) {
            return false;
        }

        if (!string.IsNullOrWhiteSpace(row.InfraType)) {
            return false;
        }

        if (string.IsNullOrWhiteSpace(row.GenusName) || string.IsNullOrWhiteSpace(row.SpeciesName)) {
            return false;
        }

        return true;
    }

    private static string DetermineDisplayName(IucnTaxonomyRow row, ScientificNameComposition composition) {
        if (!string.IsNullOrWhiteSpace(composition.BaseName)) {
            return composition.BaseName;
        }

        if (!string.IsNullOrWhiteSpace(row.ScientificNameAssessments)) {
            return row.ScientificNameAssessments.Trim();
        }

        if (!string.IsNullOrWhiteSpace(row.ScientificNameTaxonomy)) {
            return row.ScientificNameTaxonomy.Trim();
        }

        return $"{row.GenusName} {row.SpeciesName}".Trim();
    }

    private static ColNameUsageEntry? GetAcceptedEntry(ColNameUsageEntry entry, ColNameUsageRepository repository, CancellationToken cancellationToken) {
        if (!repository.SupportsAcceptedNameLookup) {
            return null;
        }

        if (string.IsNullOrWhiteSpace(entry.AcceptedNameUsageId)) {
            return null;
        }

        return repository.GetById(entry.AcceptedNameUsageId, cancellationToken);
    }

    private static ColNameUsageEntry SelectPreferredMatch(IReadOnlyList<ColNameUsageEntry> matches) {
        foreach (var candidate in matches) {
            if (IsAcceptedStatus(candidate.Status)) {
                return candidate;
            }
        }

        foreach (var candidate in matches) {
            if (string.IsNullOrWhiteSpace(candidate.Status)) {
                return candidate;
            }
        }

        return matches[0];
    }

    private static bool IsAcceptedStatus(string? status) {
        if (string.IsNullOrWhiteSpace(status)) {
            return false;
        }

        var normalized = status.Trim().ToLowerInvariant();
        if (normalized.Contains("synonym", StringComparison.Ordinal)) {
            return false;
        }

        if (normalized.Contains("misapplied", StringComparison.Ordinal)) {
            return false;
        }

        return normalized.Contains("accepted", StringComparison.Ordinal) || normalized.Contains("valid", StringComparison.Ordinal);
    }

    private static bool IsSynonymStatus(string? status) {
        if (string.IsNullOrWhiteSpace(status)) {
            return false;
        }

        return status.Contains("synonym", StringComparison.OrdinalIgnoreCase);
    }

    private static string? NormalizeText(string? value) {
        if (value is null) {
            return null;
        }

        var trimmed = value.Trim();
        if (trimmed.Length == 0) {
            return null;
        }

        var builder = new StringBuilder(trimmed.Length);
        var previousWasSpace = false;

        foreach (var ch in trimmed) {
            var normalized = ch switch {
                '\u00A0' => ' ',
                '\u2007' => ' ',
                '\u202F' => ' ',
                '\u2009' => ' ',
                '\r' => ' ',
                '\n' => ' ',
                '\t' => ' ',
                _ => ch
            };

            if (char.IsWhiteSpace(normalized)) {
                if (!previousWasSpace) {
                    builder.Append(' ');
                    previousWasSpace = true;
                }
            } else {
                builder.Append(normalized);
                previousWasSpace = false;
            }
        }

        var result = builder.ToString().Trim();
        return result.Length == 0 ? null : result;
    }

    private static bool AuthoritiesEqual(string? a, string? b) {
        if (string.IsNullOrWhiteSpace(a) && string.IsNullOrWhiteSpace(b)) {
            return true;
        }

        if (string.IsNullOrWhiteSpace(a) || string.IsNullOrWhiteSpace(b)) {
            return false;
        }

        return string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
    }

    private static string BuildSynonymDetail(CrosscheckSample sample) {
        var status = string.IsNullOrWhiteSpace(sample.ColStatus) ? "unknown" : sample.ColStatus.Trim();
        if (!string.IsNullOrWhiteSpace(sample.AcceptedScientificName)) {
            var authority = string.IsNullOrWhiteSpace(sample.AcceptedAuthority) ? "(no authority)" : sample.AcceptedAuthority.Trim();
            var idLabel = string.IsNullOrWhiteSpace(sample.AcceptedId) ? "(id unavailable)" : sample.AcceptedId;
            return $"Synonym in COL (status={status}) → accepted {sample.AcceptedScientificName} [{authority}] (ID: {idLabel})";
        }

        if (!string.IsNullOrWhiteSpace(sample.AcceptedId)) {
            return $"Synonym in COL (status={status}) → accepted ID {sample.AcceptedId} not resolved";
        }

        return $"Synonym in COL (status={status}).";
    }

    private static string BuildAuthorityMismatchDetail(CrosscheckSample sample) {
        var iucn = sample.NormalizedIucnAuthority ?? "(none)";
        var col = sample.NormalizedColAuthority ?? "(none)";
        return $"Authority mismatch after normalization (IUCN={iucn}; COL={col}).";
    }

    private static string BuildMultipleMatchesDetail(IReadOnlyList<ColNameUsageEntry> matches) {
        var parts = matches
            .Select(match => {
                var status = string.IsNullOrWhiteSpace(match.Status) ? "?" : match.Status.Trim();
                return $"{match.Id}:{status}";
            });
        return $"Multiple COL candidates ({matches.Count}): {string.Join(", ", parts)}";
    }

    private static CrosscheckSample CreateSample(
        IucnTaxonomyRow row,
        string scientificName,
        string? iucnAuthority,
        string? normalizedIucnAuthority,
        ColNameUsageEntry? colEntry,
        string? normalizedColAuthority,
        string? acceptedId,
        string? acceptedScientificName,
        string? acceptedAuthority,
        string? normalizedAcceptedAuthority,
        int matchCount,
        string detail
    ) {
        var sortKey = BuildSortKey(row, scientificName);
        return new CrosscheckSample(
            sortKey,
            row.KingdomName,
            row.PhylumName,
            row.ClassName,
            row.OrderName,
            row.FamilyName,
            row.GenusName,
            row.SpeciesName,
            scientificName,
            iucnAuthority,
            normalizedIucnAuthority,
            colEntry?.Id,
            colEntry?.ScientificName,
            colEntry?.Status,
            colEntry?.Authorship,
            normalizedColAuthority,
            acceptedId,
            acceptedScientificName,
            acceptedAuthority,
            normalizedAcceptedAuthority,
            matchCount,
            detail,
            row.AssessmentId,
            row.InternalTaxonId,
            row.RedlistVersion
        );
    }

    private static string BuildSortKey(IucnTaxonomyRow row, string scientificName) {
        var parts = new[] {
            row.KingdomName,
            row.PhylumName,
            row.ClassName,
            row.OrderName,
            row.FamilyName,
            row.GenusName,
            row.SpeciesName,
            scientificName
        };

        return string.Join("|", parts.Select(p => (p ?? string.Empty).ToUpperInvariant()));
    }

    private static void RenderResults(IucnColCrosscheckAnalysis analysis, string iucnDatabasePath, string colDatabasePath, long limit) {
        AnsiConsole.MarkupLine("[bold]IUCN vs Catalogue of Life Crosscheck[/]");
        AnsiConsole.MarkupLine($"[grey]IUCN database:[/] {Markup.Escape(Path.GetFileName(iucnDatabasePath))}");
        AnsiConsole.MarkupLine($"[grey]COL database:[/] {Markup.Escape(Path.GetFileName(colDatabasePath))}");
        if (limit > 0) {
            AnsiConsole.MarkupLine($"[grey]Row limit:[/] {limit:N0}");
        }
        AnsiConsole.MarkupLine($"[grey]Rows scanned:[/] {analysis.TotalRows:N0}");
        AnsiConsole.MarkupLine($"[grey]Species evaluated:[/] {analysis.EvaluatedSpecies:N0}");
        if (analysis.SkippedNonSpecies > 0) {
            AnsiConsole.MarkupLine($"[grey]Skipped (non-global or infraspecific):[/] {analysis.SkippedNonSpecies:N0}");
        }

        AnsiConsole.MarkupLine(string.Empty);
        AnsiConsole.MarkupLine("[bold]Counts[/]");
        AnsiConsole.MarkupLine($"- Found in COL: {analysis.FoundCount:N0}");
        AnsiConsole.MarkupLine($"- Missing in COL: {analysis.MissingCount:N0}");
        AnsiConsole.MarkupLine($"- Synonyms in COL: {analysis.SynonymCount:N0}");
        AnsiConsole.MarkupLine($"- Authority matches: {analysis.AuthorityMatchCount:N0}");
        AnsiConsole.MarkupLine($"- Authority mismatches: {analysis.AuthorityMismatchCount:N0}");
        if (analysis.MultipleMatchesCount > 0) {
            AnsiConsole.MarkupLine($"- Multiple COL candidates: {analysis.MultipleMatchesCount:N0}");
        }

        AnsiConsole.MarkupLine(string.Empty);
        PrintSamples("Missing in COL", analysis.GetSamples(CrosscheckSampleKind.Missing));
        PrintSamples("Synonyms in COL", analysis.GetSamples(CrosscheckSampleKind.Synonym));
        PrintSamples("Authority mismatches", analysis.GetSamples(CrosscheckSampleKind.AuthorityMismatch));
        PrintSamples("Multiple COL candidates", analysis.GetSamples(CrosscheckSampleKind.MultipleMatches));
    }

    private static void PrintSamples(string title, IReadOnlyList<CrosscheckSample> samples) {
        if (samples.Count == 0) {
            return;
        }

        AnsiConsole.MarkupLine($"[grey]{Markup.Escape(title)}[/]");
        var ordered = samples
            .OrderBy(sample => sample.SortKey, StringComparer.Ordinal)
            .ThenBy(sample => sample.IucnScientificName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var sample in ordered) {
            var builder = new StringBuilder();
            AppendField(builder, "classification", BuildClassificationString(sample));
            AppendField(builder, "iucnScientificName", sample.IucnScientificName);
            AppendField(builder, "iucnAuthority", FormatAuthority(sample.IucnAuthority, sample.NormalizedIucnAuthority));
            AppendField(builder, "colScientificName", sample.ColScientificName ?? "(not found)");
            AppendField(builder, "colId", sample.ColId ?? "(none)");
            AppendField(builder, "colStatus", sample.ColStatus ?? "(none)");
            AppendField(builder, "colAuthority", FormatAuthority(sample.ColAuthority, sample.NormalizedColAuthority));

            if (!string.IsNullOrWhiteSpace(sample.AcceptedScientificName) || !string.IsNullOrWhiteSpace(sample.AcceptedId)) {
                var acceptedLabel = sample.AcceptedScientificName ?? "(unknown)";
                AppendField(builder, "colAccepted", acceptedLabel);
                AppendField(builder, "colAcceptedAuthority", FormatAuthority(sample.AcceptedAuthority, sample.NormalizedAcceptedAuthority));
                AppendField(builder, "colAcceptedId", sample.AcceptedId ?? "(none)");
            }

            AppendField(builder, "matches", sample.MatchCount.ToString());
            AppendField(builder, "detail", sample.Detail);
            AppendField(builder, "assessmentId", sample.AssessmentId);
            AppendField(builder, "internalTaxonId", sample.InternalTaxonId);
            AppendField(builder, "redlistVersion", sample.RedlistVersion);
            AppendField(builder, "url", BuildIucnUrl(sample.InternalTaxonId, sample.AssessmentId));
            builder.AppendLine();
            AnsiConsole.Markup(builder.ToString());
        }
    }

    private static void AppendField(StringBuilder builder, string label, string? value) {
        builder.Append("[grey]").Append(Markup.Escape(label)).AppendLine("[/]");
        if (value is null) {
            builder.AppendLine("  (null)");
            return;
        }

        if (value.Length == 0) {
            builder.AppendLine("  (empty)");
            return;
        }

        var lines = value.Split('\n');
        foreach (var line in lines) {
            builder.Append("  ").AppendLine(Markup.Escape(line.Length == 0 ? "(empty)" : line));
        }
    }

    private static string BuildClassificationString(CrosscheckSample sample) {
        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(sample.Kingdom)) parts.Add(sample.Kingdom);
        if (!string.IsNullOrWhiteSpace(sample.Phylum)) parts.Add(sample.Phylum);
        if (!string.IsNullOrWhiteSpace(sample.Class)) parts.Add(sample.Class);
        if (!string.IsNullOrWhiteSpace(sample.Order)) parts.Add(sample.Order);
        if (!string.IsNullOrWhiteSpace(sample.Family)) parts.Add(sample.Family);
        if (!string.IsNullOrWhiteSpace(sample.Genus)) parts.Add(sample.Genus);
        if (!string.IsNullOrWhiteSpace(sample.Species)) parts.Add(sample.Species);
        return parts.Count == 0 ? "(unknown)" : string.Join(" > ", parts);
    }

    private static string FormatAuthority(string? raw, string? normalized) {
        if (string.IsNullOrWhiteSpace(raw)) {
            return normalized is null ? "(none)" : $"(none) → normalized: {normalized}";
        }

        if (string.IsNullOrWhiteSpace(normalized)) {
            return raw;
        }

        if (string.Equals(raw.Trim(), normalized, StringComparison.Ordinal)) {
            return raw;
        }

        return $"{raw} (normalized: {normalized})";
    }

    private static string BuildIucnUrl(string internalTaxonId, string assessmentId) {
        if (string.IsNullOrWhiteSpace(internalTaxonId) || string.IsNullOrWhiteSpace(assessmentId)) {
            return "(unavailable)";
        }

        var taxon = internalTaxonId.Trim();
        var assess = assessmentId.Trim();
        return $"https://www.iucnredlist.org/species/{taxon}/{assess}";
    }
}

internal sealed class IucnColCrosscheckAnalysis {
    private readonly int _maxSamples;
    private readonly List<CrosscheckSample> _missingSamples = new();
    private readonly List<CrosscheckSample> _synonymSamples = new();
    private readonly List<CrosscheckSample> _authorityMismatchSamples = new();
    private readonly List<CrosscheckSample> _multipleMatchSamples = new();

    public IucnColCrosscheckAnalysis(int maxSamples) {
        _maxSamples = Math.Max(1, maxSamples);
    }

    public long TotalRows { get; private set; }
    public long SkippedNonSpecies { get; private set; }
    public long EvaluatedSpecies { get; private set; }
    public long MissingCount { get; private set; }
    public long FoundCount { get; private set; }
    public long SynonymCount { get; private set; }
    public long AuthorityMatchCount { get; private set; }
    public long AuthorityMismatchCount { get; private set; }
    public long MultipleMatchesCount { get; private set; }

    public void RegisterRow() => TotalRows++;

    public void RegisterSkipped() => SkippedNonSpecies++;

    public void RegisterEvaluated() => EvaluatedSpecies++;

    public void RegisterMissing(CrosscheckSample sample) {
        MissingCount++;
        AddSample(_missingSamples, sample);
    }

    public void RegisterFound() => FoundCount++;

    public void RegisterSynonym(CrosscheckSample sample) {
        SynonymCount++;
        AddSample(_synonymSamples, sample);
    }

    public void RegisterAuthorityMatch() => AuthorityMatchCount++;

    public void RegisterAuthorityMismatch(CrosscheckSample sample) {
        AuthorityMismatchCount++;
        AddSample(_authorityMismatchSamples, sample);
    }

    public void RegisterMultipleMatches(CrosscheckSample sample) {
        MultipleMatchesCount++;
        AddSample(_multipleMatchSamples, sample);
    }

    public IReadOnlyList<CrosscheckSample> GetSamples(CrosscheckSampleKind kind) => kind switch {
        CrosscheckSampleKind.Missing => _missingSamples,
        CrosscheckSampleKind.Synonym => _synonymSamples,
        CrosscheckSampleKind.AuthorityMismatch => _authorityMismatchSamples,
        CrosscheckSampleKind.MultipleMatches => _multipleMatchSamples,
        _ => Array.Empty<CrosscheckSample>()
    };

    private void AddSample(List<CrosscheckSample> list, CrosscheckSample sample) {
        if (list.Count < _maxSamples) {
            list.Add(sample);
        }
    }
}

internal enum CrosscheckSampleKind {
    Missing,
    Synonym,
    AuthorityMismatch,
    MultipleMatches
}

internal sealed record CrosscheckSample(
    string SortKey,
    string Kingdom,
    string? Phylum,
    string? Class,
    string? Order,
    string? Family,
    string Genus,
    string Species,
    string IucnScientificName,
    string? IucnAuthority,
    string? NormalizedIucnAuthority,
    string? ColId,
    string? ColScientificName,
    string? ColStatus,
    string? ColAuthority,
    string? NormalizedColAuthority,
    string? AcceptedId,
    string? AcceptedScientificName,
    string? AcceptedAuthority,
    string? NormalizedAcceptedAuthority,
    int MatchCount,
    string Detail,
    string AssessmentId,
    string InternalTaxonId,
    string RedlistVersion
);
