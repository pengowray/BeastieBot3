using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

// SALVE (Sistema de Avaliação do Risco de Extinção da Biodiversidade), ICMBio's system for the
// national assessments of the extinction risk of Brazil's fauna. Its public search answers
// GET /salve-api/public/search with one row per species or subspecies and its current assessment.
// The search needs a filter (with none it answers 500), so the download asks for every category id
// of the selectOptions list (EX 127 to NA 136). Pages are at most 500 rows (paginationPageSize),
// numbered from 1 (paginationPageNumber); with a filter the answer gives no total, so the download
// reads pages until one is short. 15,409 rows on 2026-10-08, in 31 pages.
//
// ICMBio's data policy for the assessments (Instrução Normativa 05/2017) makes the results public
// once the category is validated, and asks users to cite the source. Precise localities can be
// restricted; nothing about places is stored here.

namespace BeastieBot3.StatusLists;

/// One current SALVE assessment. Category: the code (CR, EN ... RE, NA). PossiblyExtinct: SALVE's
/// flag on a CR assessment. AssessedOn: yyyy-MM-dd, the end of the assessment.
internal sealed record SalveAssessment(
    string FichaId,
    string ScientificName,
    string? Authority,
    string? CommonName,
    string? Group,
    string Category,
    bool PossiblyExtinct,
    string? Criteria,
    string? AssessedOn,
    string? Doi,
    string? TaxonLevel,
    bool Published);

internal static partial class SalveApi {
    public const string SiteUrl = "https://salve.icmbio.gov.br/";
    public const string SearchUrl = "https://salve.icmbio.gov.br/salve-api/public/search";
    public const int PageSize = 500;

    /// The category ids of SALVE's selectOptions: EX, EW, RE, CR, EN, VU, NT, LC, DD, NA.
    public const string CategoryIds = "127,128,129,130,131,132,133,134,135,136";

    public const string Title = "SALVE, Sistema de Avaliação do Risco de Extinção da Biodiversidade, ICMBio";
    public const string Licence = "Public, with the source cited (ICMBio Instrução Normativa 05/2017)";

    public static string Citation(DateTime accessedUtc) =>
        "ICMBio, " + accessedUtc.Year.ToString(CultureInfo.InvariantCulture)
        + ". Sistema de Avaliação do Risco de Extinção da Biodiversidade – SALVE. Disponível em: https://salve.icmbio.gov.br/. Acesso em: "
        + $"{accessedUtc.Day:00} de {PortugueseMonths[accessedUtc.Month - 1]} de {accessedUtc.Year}.";

    // SALVE's own citation abbreviates the month as Portuguese does ("08 de out. de 2026").
    private static readonly string[] PortugueseMonths = ["jan.", "fev.", "mar.", "abr.", "maio", "jun.", "jul.", "ago.", "set.", "out.", "nov.", "dez."];

    public static string PageUrl(int page) =>
        $"{SearchUrl}?categoriaIds={Uri.EscapeDataString(CategoryIds)}&paginationPageSize={PageSize}&paginationPageNumber={page}";

    /// The assessment's page: its DOI, else the PDF of its sheet.
    public static string AssessmentUrl(string fichaId, string? doi) =>
        doi is { Length: > 0 } ? "https://doi.org/" + doi : "https://salve.icmbio.gov.br/salve-api/public/fichaPdf/" + fichaId;

    /// The rows of one page, as JSON elements.
    public static IReadOnlyList<JsonElement> ReadPage(string json) {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array
            ? data.EnumerateArray().Select(e => e.Clone()).ToList()
            : [];
    }

    /// A row as an assessment; null for a row with no id, name or category, or one SALVE marks excluded.
    public static SalveAssessment? ReadAssessment(JsonElement row) {
        if (Text(row, "id_ficha") is not { } id || Text(row, "cd_categoria_final") is not { } category
            || (row.TryGetProperty("st_excluida", out var excluded) && excluded.ValueKind == JsonValueKind.True)) {
            return null;
        }
        var nameHtml = Text(row, "nm_cientifico_atual") ?? Text(row, "nm_cientifico");
        if (nameHtml is null || NameFromHtml(nameHtml) is not { } name) {
            return null;
        }
        var assessed = Text(row, "dt_fim_avaliacao") is { } end
            && DateTime.TryParse(end, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var date)
            ? date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : null;
        return new SalveAssessment(id, name.Name, name.Authority, Text(row, "no_comum"), Text(row, "ds_grupo_salve"),
            category.ToUpperInvariant(), Text(row, "st_possivelmente_extinta") == "S", Text(row, "ds_criterio_aval_iucn_final"), assessed,
            Text(row, "ds_doi"), Text(row, "co_nivel_taxonomico"), Text(row, "cd_situacao_ficha") == "PUBLICADA");
    }

    /// "<i>Aaptos glutinans</i>&nbsp;<span>Moraes, 2011</span>": the italic part is the name, the
    /// span the authority.
    public static (string Name, string? Authority)? NameFromHtml(string html) {
        var italic = string.Join(' ', Italic().Matches(html).Select(m => NztcsApi.PlainText(m.Groups[1].Value)).Where(t => t.Length > 0));
        if (italic.Length == 0) {
            return null;
        }
        var authority = Span().Match(html) is { Success: true } span ? NztcsApi.PlainText(span.Groups[1].Value) : null;
        return (italic, string.IsNullOrEmpty(authority) ? null : authority);
    }

    private static string? Text(JsonElement row, string property) =>
        row.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(value.GetString())
            ? value.GetString()!.Trim()
            : null;

    [GeneratedRegex("<i>(.*?)</i>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Italic();

    [GeneratedRegex("<span>(.*?)</span>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Span();
}
