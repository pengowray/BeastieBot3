namespace BeastieBot3.Site.Display;

// The list page of a group and the links to it.

public static partial class GroupText {
    /// "the family Ursidae", or the name alone for a group whose rank is not shown ("Cetacea").
    public static string GroupInSentence(Data.GroupRow group) =>
        group.ShowRank && group.Rank != "unranked" ? $"{The} {group.Rank} {group.Name}" : group.Name;

    /// The group's list page: "Species list of the family Ursidae" (link, browser title).
    public static string ToolListLink(Data.GroupRow group) => ToolListLinkBefore + GroupInSentence(group);

    /// The help line under the list link: "The taxa in the family as a bulleted list ...".
    public static string ToolListHelp(Data.GroupRow group) =>
        (group.ShowRank && group.Rank != "unranked" ? $"The taxa in the {group.Rank}" : "The taxa in this group")
        + " as a bulleted list or species tables, with a choice of headings, IUCN categories and references.";

    // The tabs under a group's name (PageTabs.ForGroup).
    public const string GroupTabsLabel = "Pages for this group";
    public const string TabGroupPage = "Group page";
    public const string TabList = "Species list";

    /// The list link: "Species list of the family Ursidae" (with _GroupInSentence).
    public const string ToolListLinkBefore = "Species list of ";
    /// The word before a group's rank inside a sentence ("the family Ursidae").
    public const string The = "the";
}
