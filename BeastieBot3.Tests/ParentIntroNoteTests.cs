using BeastieBot3.WikipediaLists;

namespace BeastieBot3.Tests;

// A parent page (one with sub-group lists) is not a complete list: each sub-group with its own list
// gets only a species count and a link. Its intro must not say "complete list".
public class ParentIntroNoteTests {
    [Fact]
    public void OrdinaryList_SaysCompleteList() {
        var note = IntroProseBuilder.BuildNotesParagraph(100, 0, 0, 0, "bird", "least concern");
        Assert.StartsWith("This is a complete list of least concern bird species as evaluated by the IUCN.", note);
    }

    [Fact]
    public void ParentList_SaysWhatTheSubGroupSectionsGive() {
        var note = IntroProseBuilder.BuildNotesParagraph(100, 5, 2, 0, "dicot", "threatened", isParent: true);
        Assert.StartsWith(
            "This is a list of threatened dicot species, subspecies and varieties as evaluated by the IUCN. "
            + "For each group that has its own list article, this list gives only the number of species in that group and a link to that article.",
            note);
        Assert.DoesNotContain("complete", note);
    }
}
