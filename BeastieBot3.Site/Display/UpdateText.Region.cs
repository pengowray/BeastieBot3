namespace BeastieBot3.Site.Display;

// The status update page's choice of an IUCN region (Europe, Mediterranean, ...) whose latest
// assessments the text is compared with instead of the global ones.

public static partial class UpdateText {
    public const string RegionLabel = "Assessments to compare with";
    public const string RegionGlobalOption = "Global";
    public static string RegionOption(string region, int taxa) =>
        $"{region} ({SiteFormat.Number(taxa)} {(taxa == 1 ? "taxon" : "taxa")})";
    public const string RegionHelp =
        "A regional assessment gives a taxon's status in that region only. With a region chosen, statuses and the comparison with the group use each taxon's latest assessment for that region. Taxoboxes always get the global status.";
    public static string RegionResultNote(string region) =>
        $"Compared with the latest assessments for {region}. Taxoboxes are compared with the latest global assessments.";
    public static string NotInRegionHeading(int n, string region) => n == 1
        ? $"1 listed taxon has no assessment for {region}"
        : $"{SiteFormat.Number(n)} listed taxa have no assessment for {region}";

    /// The release in the edit summary when a region was chosen: "IUCN Red List 2026-1, Europe assessments: ...".
    public static string EditSummaryRegionVersion(string version, string region) => $"{version}, {region} assessments";
}
