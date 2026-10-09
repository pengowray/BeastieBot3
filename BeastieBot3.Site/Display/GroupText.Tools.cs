namespace BeastieBot3.Site.Display;

// The list page of a group and the links to it.

public static partial class GroupText {
    /// "the family Ursidae", or the name alone for a group whose rank is not shown ("Cetacea").
    public static string GroupInSentence(Data.GroupRow group) =>
        group.ShowRank && group.Rank != "unranked" ? $"{The} {group.Rank} {group.Name}" : group.Name;

    /// The group's list page: "Wikipedia list of the family Ursidae" (menu item, link, browser title).
    public static string ToolListLink(Data.GroupRow group) => ToolListLinkBefore + GroupInSentence(group);

    /// The help line under the list link: "The taxa in the family as a bulleted list ...".
    public static string ToolListHelp(Data.GroupRow group) =>
        (group.ShowRank && group.Rank != "unranked" ? $"The taxa in the {group.Rank}" : "The taxa in this group")
        + " as a bulleted list or species tables, with a choice of headings, IUCN categories and references.";

    public const string ListPageLabel = "Wikipedia list";
    /// The link back to the group page: "Group page for the family Ursidae" (with _GroupInSentence).
    public const string GroupPageForBefore = "Group page for ";
    /// The list link: "Wikipedia list of the family Ursidae" (with _GroupInSentence).
    public const string ToolListLinkBefore = "Wikipedia list of ";
    /// The word before a group's rank inside a sentence ("the family Ursidae").
    public const string The = "the";
}
