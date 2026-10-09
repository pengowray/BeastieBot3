namespace BeastieBot3.Site.Display;

// The list page of a group and the links to it.

public static partial class GroupText {
    /// The list link in the Tools section of a group page ("Wikipedia list of family Felidae").
    public static string ToolListLink(string group) => $"Wikipedia list of {group}";
    public const string ToolListHelp = "The group's taxa as a bulleted list or species tables, with headings and categories to choose.";

    public static string ListPageTitle(string group) => $"Wikipedia list of {group}";
    public const string ListPageLabel = "Wikipedia list";
    public const string BackToGroupPage = "Back to the group page";
}
