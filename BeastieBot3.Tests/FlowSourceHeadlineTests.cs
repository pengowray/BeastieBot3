using BeastieBot3.Web.Flows;
using BeastieBot3.Web.Status;
using Xunit;

namespace BeastieBot3.Tests;

// The one-line summary on a workflow step's data source chip. When the chosen metric has no value
// the chip said "{label}: n/a" and dropped the reason the status service had recorded, so a cache
// whose table is not created yet looked the same as one that could not be read.
public class FlowSourceHeadlineTests {
    private static DataSourceStatus Source(bool exists = true, string? error = null, params MetricResult[] metrics) => new() {
        Id = "wikidata-cache",
        Name = "Wikidata cache",
        Kind = "sqlite",
        Exists = exists,
        Error = error,
        Metrics = metrics,
    };

    private static MetricResult Metric(string label, long? value = null, string? note = null, string? error = null) =>
        new() { Label = label, Value = value, Note = note, Error = error };

    [Fact]
    public void A_counted_metric_shows_its_count() {
        Assert.Equal("181,294 entities cached", FlowEvaluator.SummariseHeadline(Source(metrics: new[] {
            Metric("entities cached", 181_294), Metric("pending download", 312),
        })));
    }

    [Fact]
    public void A_zero_count_is_still_shown_when_nothing_is_above_zero() {
        Assert.Equal("0 entities cached", FlowEvaluator.SummariseHeadline(Source(metrics: Metric("entities cached", 0))));
    }

    [Fact]
    public void A_missing_file_says_missing() {
        Assert.Equal("missing", FlowEvaluator.SummariseHeadline(Source(exists: false)));
    }

    [Fact]
    public void A_metric_without_a_value_shows_its_note() {
        Assert.Equal("entities cached: table not yet created", FlowEvaluator.SummariseHeadline(Source(metrics:
            Metric("entities cached", note: "table not yet created"))));
    }

    [Fact]
    public void A_metric_that_failed_shows_the_error_without_its_final_full_stop() {
        Assert.Equal("entities cached: count failed (SQLite Error 5: 'database is locked')",
            FlowEvaluator.SummariseHeadline(Source(metrics:
                Metric("entities cached", error: "SQLite Error 5: 'database is locked'."))));
    }

    [Fact]
    public void A_long_error_is_cut_to_fit_the_chip() {
        var headline = FlowEvaluator.SummariseHeadline(Source(metrics:
            Metric("entities cached", error: new string('x', 200))))!;
        Assert.StartsWith("entities cached: count failed (xxx", headline);
        Assert.EndsWith("…)", headline);
        Assert.True(headline.Length < 120, headline);
    }

    [Fact]
    public void A_file_that_could_not_be_opened_says_so() {
        Assert.Equal("read failed (SQLite Error 26: 'file is not a database')",
            FlowEvaluator.SummariseHeadline(Source(error: "SQLite Error 26: 'file is not a database'.")));
    }

    [Fact]
    public void A_source_with_no_metrics_and_no_error_has_no_headline() {
        Assert.Null(FlowEvaluator.SummariseHeadline(Source()));
    }
}
