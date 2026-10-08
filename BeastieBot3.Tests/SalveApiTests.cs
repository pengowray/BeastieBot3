using System.Text.Json;
using BeastieBot3.StatusLists;
using BeastieBot3.Web.Flows;

namespace BeastieBot3.Tests;

// `statuses salve-import`: reading SALVE's search rows, and the workflow light. The rows are cut down
// from SALVE's answers (October 2026).
public sealed class SalveApiTests {
    private static JsonElement Row(string json) => JsonDocument.Parse(json).RootElement.Clone();

    [Fact]
    public void Row_is_read_with_its_name_category_date_and_doi() {
        var row = Row("""
            {"no_comum":"Esponja-aglutinadora","ds_grupo_salve":"Invertebrados Marinhos","cd_categoria_final":"CR",
             "st_possivelmente_extinta":"S","ds_criterio_aval_iucn_final":"B1ab(iii)","cd_situacao_ficha":"PUBLICADA",
             "co_nivel_taxonomico":"ESPECIE","st_excluida":false,"ds_doi":"10.37002/salve.ficha.32866.2","id_ficha":"355867",
             "dt_fim_avaliacao":"2022-08-19T06:00:00Z","nm_cientifico":"<i>Aaptos glutinans</i>&nbsp;<span>Moraes, 2011</span>",
             "nm_cientifico_atual":"<i>Aaptos glutinans</i>&nbsp;<span>Moraes, 2011</span>"}
            """);
        Assert.Equal(new SalveAssessment("355867", "Aaptos glutinans", "Moraes, 2011", "Esponja-aglutinadora", "Invertebrados Marinhos", "CR",
            true, "B1ab(iii)", "2022-08-19", "10.37002/salve.ficha.32866.2", "ESPECIE", true), SalveApi.ReadAssessment(row));
    }

    [Fact]
    public void Excluded_rows_and_rows_without_a_name_are_left_out() {
        Assert.Null(SalveApi.ReadAssessment(Row("""{"id_ficha":"1","cd_categoria_final":"LC","st_excluida":true,"nm_cientifico":"<i>A b</i>"}""")));
        Assert.Null(SalveApi.ReadAssessment(Row("""{"id_ficha":"1","cd_categoria_final":"LC","nm_cientifico":"Ab"}""")));
    }

    [Fact]
    public void The_current_name_is_used() {
        var row = SalveApi.ReadAssessment(Row("""
            {"id_ficha":"9","cd_categoria_final":"LC","nm_cientifico":"<i>Old name</i>","nm_cientifico_atual":"<i>Alouatta guariba clamitans</i>&nbsp;<span>Cabrera, 1940</span>"}
            """))!;
        Assert.Equal(("Alouatta guariba clamitans", "Cabrera, 1940"), (row.ScientificName, row.Authority));
        Assert.False(row.Published);
    }

    [Fact]
    public void Links_go_to_the_doi_else_the_pdf() {
        Assert.Equal("https://doi.org/10.37002/salve.ficha.32866.2", SalveApi.AssessmentUrl("355867", "10.37002/salve.ficha.32866.2"));
        Assert.Equal("https://salve.icmbio.gov.br/salve-api/public/fichaPdf/355867", SalveApi.AssessmentUrl("355867", null));
    }

    [Fact]
    public void Citation_is_salves_own_form() =>
        Assert.Equal("ICMBio, 2026. Sistema de Avaliação do Risco de Extinção da Biodiversidade – SALVE. Disponível em: https://salve.icmbio.gov.br/. Acesso em: 08 de out. de 2026.",
            SalveApi.Citation(new DateTime(2026, 10, 8, 3, 0, 0, DateTimeKind.Utc)));

    [Fact]
    public void Light() {
        var now = new DateTime(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);
        var site = new PublicSiteState { StatusListsPath = "/data/status_lists.sqlite", ReadAtUtc = now };
        Assert.Equal("Not downloaded yet.", PublicSiteProbes.SalveStep(site).Detail);
        var done = PublicSiteProbes.SalveStep(site with { Salve = new StatusListSourceState(now.AddDays(-1), 15409) });
        Assert.Equal(("ok", "15,409 assessments, downloaded 2026-10-07."), (done.Status, done.Detail));
    }
}
