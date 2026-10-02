using BeastieBot3.Shared.Wikitext;
using BeastieBot3.SiteBuild;

namespace BeastieBot3.Tests.SiteBuild;

// Pins how one split author name is read: person (surname and initials), organisation, or kept as
// published. Names are real ones from the 2026-1 assessor credits.
public class IucnAuthorNameParserTests {
    [Theory]
    // Plain, particles, multi-word, hyphenated and apostrophe surnames.
    [InlineData("Wiig, Ø.", "Wiig", "Ø.", "SurnameInitials")]
    [InlineData("de Kok, R.", "de Kok", "R.", "SurnameInitials")]
    [InlineData("van Swaay, C.", "van Swaay", "C.", "SurnameInitials")]
    [InlineData("de los Ángeles La Torre Cuadros, M.", "de los Ángeles La Torre Cuadros", "M.", "SurnameInitials")]
    [InlineData("Martínez Salas, E.", "Martínez Salas", "E.", "SurnameInitials")]
    [InlineData("Scipio-O'Dean, C.J.", "Scipio-O'Dean", "C.J.", "SurnameInitials")]
    [InlineData("Pérez‐Miranda, F.", "Pérez‐Miranda", "F.", "SurnameInitials")]
    [InlineData("Morales M, P.A.", "Morales M", "P.A.", "SurnameInitials")]
    // Surnames that are also organisation words.
    [InlineData("Park, S.-H.", "Park", "S.-H.", "SurnameInitials")]
    // Initials with particles, hyphens, odd case, or no dots.
    [InlineData("Nogueira, C. de C.", "Nogueira", "C. de C.", "SurnameInitials")]
    [InlineData("Prudente, A.L. da C.", "Prudente", "A.L. da C.", "SurnameInitials")]
    [InlineData("Samain, M.-S.", "Samain", "M.-S.", "SurnameInitials")]
    [InlineData("Lee, Y-W", "Lee", "Y-W", "SurnameInitialsNoDots")]
    [InlineData("Qin, h.", "Qin", "h.", "SurnameInitials")]
    [InlineData("Allen, G.R", "Allen", "G.R", "SurnameInitials")]
    [InlineData("DoNascimiento, CD", "DoNascimiento", "CD", "SurnameInitialsNoDots")]
    // Generational suffixes, after the given names as CS1's |first= has them.
    [InlineData("Lowry II, P.P.", "Lowry", "P.P., II", "SurnameInitialsSuffix")]
    [InlineData("Brownell Jr., R.L.", "Brownell", "R.L., Jr.", "SurnameInitialsSuffix")]
    [InlineData("Louis Jr, E.E.", "Louis", "E.E., Jr", "SurnameInitialsSuffix")]
    [InlineData("Golamco, A., Jr.", "Golamco", "A., Jr.", "SurnameInitialsSuffix")]
    [InlineData("Driggers, III, W.B.", "Driggers", "W.B., III", "SurnameInitialsSuffix")]
    [InlineData("Guerrero, R.D. III", "Guerrero", "R.D., III", "SurnameInitialsSuffix")]
    // A given name after the comma.
    [InlineData("Mohd Yusof, Nur Adillah", "Mohd Yusof", "Nur Adillah", "SurnameGivenNames")]
    [InlineData("Qin, Hai-Ning", "Qin", "Hai-Ning", "SurnameGivenNames")]
    // One token.
    [InlineData("Tamanyan K.", "Tamanyan", "K.", "CompactSurnameFirst")]
    [InlineData("de Bélair G.", "de Bélair", "G.", "CompactSurnameFirst")]
    [InlineData("N.H. Rakotoarivelo", "Rakotoarivelo", "N.H.", "CompactInitialsFirst")]
    public void People(string name, string last, string initials, string shape) {
        var parsed = IucnAuthorNameParser.Parse(name);

        Assert.Equal(new CitationAuthor(CitationAuthorKind.Person, name, last, initials), parsed.Author);
        Assert.Equal(Enum.Parse<AuthorNameShape>(shape), parsed.Shape);
    }

    [Theory]
    [InlineData("BirdLife International")]
    [InlineData("IUCN SSC Amphibian Specialist Group")]
    [InlineData("Botanic Gardens Conservation International (BGCI)")]
    [InlineData("Royal Botanic Gardens, Kew")]
    [InlineData("Ministry of the Environment, Japan")]
    [InlineData("Australian Government, Threatened Species Scientific Committee")]
    [InlineData("Instituto Chico Mendes de Conservação da Biodiversidade (ICMBio)")]
    [InlineData("Tortoise & Freshwater Turtle Specialist Group")]
    [InlineData("NatureServe (Whittaker, J.C., Hammerson, G., Master, L. & Norris, S.J.)")]
    [InlineData("NatureServe (Hammerson, G.)")]
    [InlineData("Working Group, C.")]
    [InlineData("Missouri Botanical Garden, -.")]
    [InlineData("Sri Lankan Red List Group")]
    [InlineData("Asociación Herpetológica Española")]
    [InlineData("CNCFlora")]
    [InlineData("WWF-Malaysia")]
    [InlineData("WCMC")]
    public void Organisations(string name) {
        var parsed = IucnAuthorNameParser.Parse(name);

        Assert.Equal(new CitationAuthor(CitationAuthorKind.Organisation, name), parsed.Author);
        Assert.Equal(AuthorNameShape.Organisation, parsed.Shape);
    }

    [Theory]
    // Given name first, including Ethiopian names, which have no surname.
    [InlineData("Neil Cox", "GivenNameFirst")]
    [InlineData("Sebsebe Demissew", "GivenNameFirst")]
    [InlineData("Thomas K. Kristensen", "GivenNameFirst")]
    [InlineData("Hoang Minh Duc", "GivenNameFirst")]
    // Single names (Indonesian mononyms).
    [InlineData("Kadarusman", "SingleName")]
    // Typos and lists the splitter kept whole.
    [InlineData("Gadsden. H.", "Unknown")]
    [InlineData("Disi, M., A.M.", "Unknown")]
    [InlineData("Bidau, & Ojeda, R.", "Unknown")]
    [InlineData("Weber, O. & Sebsebe Demissew", "Unknown")]
    // Lists kept whole that name a person outside parentheses, with organisation words too.
    [InlineData("Loiselle, P. & participants of the CBSG/ANGAP CAMP \"Faune de Madagascar\" workshop, Mantasoa, Madagascar 2001", "Unknown")]
    [InlineData("Eastern Arc Mountains & Coastal Forests CEPF Plant Assessment Project & Bösenberg, J.D.", "Unknown")]
    [InlineData("Carter, R.L., Hayes, W.K. & West Indian Iguana Specialist Group", "Unknown")]
    [InlineData("Magombo, Z.L.K., Mbeiza Mutekanga, N. & Ndiritu, G.G. (Freshwater Biodiversity Assessment workshop, Uganda. Dec' 2003)", "Unknown")]
    // A single-letter surname: a typo for "Ntakimazi, G.".
    [InlineData("G, Ntakimazi", "Unknown")]
    [InlineData("Kry�tufek, B.", "Unknown")]
    public void KeptAsPublished(string name, string shape) {
        var parsed = IucnAuthorNameParser.Parse(name);

        Assert.Equal(new CitationAuthor(CitationAuthorKind.Verbatim, name), parsed.Author);
        Assert.Equal(Enum.Parse<AuthorNameShape>(shape), parsed.Shape);
    }

    [Theory]
    [InlineData("Dombrowski , A.", "Dombrowski, A.")]
    [InlineData("  Luna-Vega, I. ", "Luna-Vega, I.")]
    [InlineData("Menzies,", "Menzies")]
    public void Clean_CollapsesSpacesAndTrimsSeparators(string name, string expected) {
        Assert.Equal(expected, IucnAuthorNameParser.Clean(name));
    }
}
