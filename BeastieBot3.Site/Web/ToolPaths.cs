namespace BeastieBot3.Site.Web;

/// The addresses of the tools, which share a row of tabs (_ToolTabs): cite an assessment, update the
/// statuses in a Wikipedia page, make a species list. Tab is the key of the tab a page belongs to.
public static class ToolPaths {
    public const string Tools = "/tools";
    public const string Cite = "/cite";
    public const string UpdateStatuses = "/update-statuses";
    public const string ListMaker = "/species-list-maker";

    public enum Tab { None, Cite, Update, List }
}
