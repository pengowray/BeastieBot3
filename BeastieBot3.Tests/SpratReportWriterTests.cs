using System.Collections.Generic;
using BeastieBot3.Sprat;

namespace BeastieBot3.Tests;

// Smoke-pins the structure of the two SPRAT reports: the applied-modernizations audit and the
// recommendations report (its four sections, fixed-elsewhere column, and present-only flag-orders).
public class SpratReportWriterTests {
    private static ModernizationLog SampleLog() {
        var log = new ModernizationLog();
        log.Record(new ModernizationChange("mammals", 1, "Antechinus sp.", "order",
            "Polyprotodonta", "Dasyuromorphia", "structural", null));
        log.Record(new ModernizationChange("mammals", 2, "Crocidura trichura", "order",
            "Insectivora", "Eulipotyphla", "obsolete", null, FixedElsewhere: true, Note: "abandoned order"));
        return log;
    }

    [Fact]
    public void AppliedReport_AggregatesByRename() {
        var report = SpratReportWriter.BuildAppliedReport(SampleLog(), "v1", "2026-06-26");
        Assert.Contains("taxonomy modernizations applied", report);
        Assert.Contains("| Insectivora | Eulipotyphla | obsolete | 1 |", report);
        Assert.Contains("| Polyprotodonta | Dasyuromorphia | structural | 1 |", report);
    }

    [Fact]
    public void RecommendationsReport_HasFourSections_AndOnlyPresentFlagOrders() {
        var flagOrders = new List<FlagOrder> {
            new("Pulmonata", "(informal clade)", "obsolete snail grouping"),
            new("Ardeiformes", "Pelecaniformes", "non-standard"),   // not present in data → omitted
        };
        var orderCounts = new Dictionary<string, int> { ["Pulmonata"] = 88 };
        var statuses = new[] { new StatusFinding("WA", "Other protected fauna", "Arctocephalus forsteri") };
        var redlinks = new List<DescriptiveNameFinding> {
            new("mammals", "Bettongia lesueur Barrow and Boodie Islands subspecies", "Bettongia lesueur"),
        };

        var report = SpratReportWriter.BuildRecommendationsReport(
            SampleLog(), flagOrders, orderCounts, statuses, redlinks, "v1", "2026-06-26");

        Assert.Contains("## 1. Order names auto-modernized", report);
        Assert.Contains("## 2. Order names needing review", report);
        Assert.Contains("## 3. Non-standard status values", report);
        Assert.Contains("## 4. Descriptive names that don't link", report);
        // fixed-elsewhere column reflects the change metadata
        Assert.Contains("| Insectivora | Eulipotyphla | obsolete | 1 | yes | abandoned order |", report);
        // only flag-orders present in the data are listed
        Assert.Contains("| Pulmonata | (informal clade) | 88 |", report);
        Assert.DoesNotContain("Ardeiformes", report);
        Assert.Contains("| WA | Other protected fauna |", report);
        Assert.Contains("| Bettongia lesueur Barrow and Boodie Islands subspecies | Bettongia lesueur | mammals |", report);
    }
}
