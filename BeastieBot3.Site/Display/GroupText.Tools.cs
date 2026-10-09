namespace BeastieBot3.Site.Display;

// The list page of a group and the links to it.

public static partial class GroupText {
    /// "the family Ursidae", or the name alone for a group whose rank is not shown ("Cetacea").
    public static string GroupInSentence(Data.GroupRow group) =>
        group.ShowRank && group.Rank != "unranked" ? $"the {group.Rank} {group.Name}" : group.Name;

    /// The group's list page: "Wikipedia list of the family Ursidae" (menu item, link, browser title).
    public static string ToolListLink(Data.GroupRow group) => "Wikipedia list of " + GroupInSentence(group);

    /// The help line under the list link: "The taxa in the family as a bulleted list ...".
    public static string ToolListHelp(Data.GroupRow group) =>
        (group.ShowRank && group.Rank != "unranked" ? $"The taxa in the {group.Rank}" : "The taxa in this group")
        + " as a bulleted list or species tables, with a choice of headings, IUCN categories and references.";

    /// ToolListLink as HTML, with a genus name in italics.
    public static Microsoft.AspNetCore.Html.HtmlString ToolListLinkHtml(Data.GroupRow group) => new("Wikipedia list of " + GroupInSentenceHtml(group));

    public const string ListPageLabel = "Wikipedia list";
    /// The link back to the group page: "Group page for the family Ursidae".
    public static Microsoft.AspNetCore.Html.HtmlString GroupPageForHtml(Data.GroupRow group) => new("Group page for " + GroupInSentenceHtml(group));

    private static string GroupInSentenceHtml(Data.GroupRow group) {
        var name = group.IsGenus ? $"<i>{SiteHtml.Encode(group.Name)}</i>" : SiteHtml.Encode(group.Name);
        return group.ShowRank && group.Rank != "unranked" ? $"the {SiteHtml.Encode(group.Rank)} {name}" : name;
    }
}
