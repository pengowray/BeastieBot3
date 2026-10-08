using System.Text.Json;
using BeastieBot3.StatusLists;
using Microsoft.Data.Sqlite;

namespace BeastieBot3.Tests;

// StatusListImport, the run that `statuses ecos-import`, `nztcs-import` and `salve-import` share, with
// --file: a kept file is stored, a file with no rows leaves the store as it was, and a missing file
// stops the run before the store is opened. Also the kept JSON files' two formats.
public sealed class StatusListImportTests : IDisposable {
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "bb3-status-import-" + Guid.NewGuid().ToString("N"));

    private string StorePath => Path.Combine(_dir, "status_lists.sqlite");

    public StatusListImportTests() {
        Directory.CreateDirectory(_dir);
        // No datastore folder, so a run without --file could not download.
        File.WriteAllText(Path.Combine(_dir, "paths.ini"), "[Datastore]\n");
    }

    public void Dispose() {
        SqliteConnection.ClearAllPools();
        try {
            Directory.Delete(_dir, recursive: true);
        } catch (IOException) {
        }
    }

    // Two rows cut down from SALVE's answers (October 2026), as a kept file holds them.
    private const string SalveRows = """
        [{"no_comum":"Esponja-aglutinadora","ds_grupo_salve":"Invertebrados Marinhos","cd_categoria_final":"CR",
          "st_possivelmente_extinta":"S","ds_criterio_aval_iucn_final":"B1ab(iii)","cd_situacao_ficha":"PUBLICADA",
          "co_nivel_taxonomico":"ESPECIE","st_excluida":false,"ds_doi":"10.37002/salve.ficha.32866.2","id_ficha":"355867",
          "dt_fim_avaliacao":"2022-08-19T06:00:00Z","nm_cientifico":"<i>Aaptos glutinans</i>&nbsp;<span>Moraes, 2011</span>",
          "nm_cientifico_atual":"<i>Aaptos glutinans</i>&nbsp;<span>Moraes, 2011</span>"},
         {"id_ficha":"9","cd_categoria_final":"LC","nm_cientifico":"<i>Old name</i>",
          "nm_cientifico_atual":"<i>Alouatta guariba clamitans</i>&nbsp;<span>Cabrera, 1940</span>"}]
        """;

    private string Write(string name, string text) {
        var path = Path.Combine(_dir, name);
        File.WriteAllText(path, text);
        return path;
    }

    private Task<int> ImportSalve(string file) => StatusListImport.RunAsync(new SalveImportCommand.Settings {
        File = file,
        StorePath = StorePath,
        IniFile = Path.Combine(_dir, "paths.ini"),
        SettingsDir = _dir,
    }, SalveImportCommand.Spec(), CancellationToken.None);

    // The SALVE rows and every status_source row, fetched times included.
    private (long Rows, string Sources) Snapshot() {
        using var store = StatusListStore.OpenReadOnly(StorePath)!;
        return (store.CountSalve(), string.Join("\n", store.Sources()));
    }

    [Fact]
    public async Task Kept_file_is_stored_with_its_source_row() {
        Assert.Equal(0, await ImportSalve(Write("salve-2026-10-08.json", SalveRows)));

        using var store = StatusListStore.OpenReadOnly(StorePath)!;
        Assert.Equal(2, store.CountSalve());
        var source = Assert.Single(store.Sources());
        Assert.Equal((StatusSources.Salve, SalveApi.Title, "salve-2026-10-08.json", 2L),
            (source.Source, source.Title, source.Version, source.RowCount));
        Assert.Equal(SalveApi.Citation(source.FetchedAtUtc), source.Citation);
    }

    [Fact]
    public async Task File_with_no_rows_leaves_the_store_as_it_was() {
        Assert.Equal(0, await ImportSalve(Write("salve-2026-10-08.json", SalveRows)));
        var before = Snapshot();

        Assert.NotEqual(0, await ImportSalve(Write("salve-2026-10-09.json", "[]")));
        Assert.Equal(before, Snapshot());
    }

    [Fact]
    public async Task Missing_file_stops_before_the_store_is_opened() {
        Assert.Equal(-1, await ImportSalve(Path.Combine(_dir, "salve-missing.json")));
        Assert.False(File.Exists(StorePath));
    }

    [Fact]
    public async Task Kept_json_files_keep_their_formats() {
        static JsonElement Row(string json) => JsonDocument.Parse(json).RootElement.Clone();
        var array = Path.Combine(_dir, "salve.json");
        var obj = Path.Combine(_dir, "nztcs.json");

        await StatusListDownload.WriteJsonArrayAsync(array, new[] { Row("""{"id_ficha":"1"}"""), Row("""{"id_ficha":"2"}""") });
        await StatusListDownload.WriteJsonObjectAsync(obj, ("assessments", new[] { Row("""{"id":1}""") }), ("species", new[] { Row("""{"id":2}""") }));

        Assert.Equal("""[{"id_ficha":"1"},{"id_ficha":"2"}]""", File.ReadAllText(array));
        Assert.Equal("""{"assessments":[{"id":1}],"species":[{"id":2}]}""", File.ReadAllText(obj));
        Assert.False(File.Exists(array + ".part"));
        Assert.False(File.Exists(obj + ".part"));
    }
}
