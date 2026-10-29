using System;
using System.Threading;
using System.Threading.Tasks;
using BeastieBot3.Configuration;
using BeastieBot3.Wikipedia;
using Xunit;

namespace BeastieBot3.Tests;

// A title with "|" ("Template:Taxonomy/Icteridae|Agelaioides", queued from a parent= field before
// the template parser cut the name at "|") was sent as two titles; the answer had no query object
// for it, the download failed on every run, and `wikipedia update` stopped at its last step. The
// endpoint here accepts no connections, so the test passes only if no request is sent.
public sealed class WikipediaInvalidTitleTests {
    [Fact]
    public async Task TitleWithPipeIsInvalidWithoutARequest() {
        var config = new WikipediaConfiguration(new Uri("http://127.0.0.1:9/w/api.php"), new Uri("http://127.0.0.1:9/rest/"),
            "BeastieBot3 tests", TimeSpan.FromSeconds(2), TimeSpan.Zero, TimeSpan.Zero);
        using var client = new WikipediaApiClient(config);

        var result = await client.QueryPageAsync("Template:Taxonomy/Icteridae|Agelaioides", CancellationToken.None);

        Assert.False(result.Exists);
        Assert.Equal("title contains |", result.MissingReason);
    }
}
