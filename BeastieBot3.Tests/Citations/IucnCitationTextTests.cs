using BeastieBot3.Iucn.Citations;

namespace BeastieBot3.Tests.Citations;

// Pins the helpers for the text of IUCN's citation: removing the access date, and finding the DOI.
public class IucnCitationTextTests {
    [Fact]
    public void StripAccessedOn_RemovesOnlyTheAccessDate() {
        Assert.Equal(
            "Konan, K.M. 2025. Macrobrachium thysi (amended version of 2024 assessment). The IUCN Red List of Threatened Species 2025: e.T197913A286613460.",
            IucnCitationText.StripAccessedOn("Konan, K.M. 2025. Macrobrachium thysi (amended version of 2024 assessment). The IUCN Red List of Threatened Species 2025: e.T197913A286613460. Accessed on 18 August 2026."));
        Assert.Equal("No access date.", IucnCitationText.StripAccessedOn("No access date."));
    }

    [Fact]
    public void ExtractDoi_KeepsLanguageSuffix_IgnoresDoiInsideSpeciesNames() {
        Assert.Equal(
            "10.2305/IUCN.UK.2025-2.RLTS.T218171971A286370469.es",
            IucnCitationText.ExtractDoi("Example 2025. Example name. The IUCN Red List of Threatened Species 2025: e.T218171971A286370469. https://dx.doi.org/10.2305/IUCN.UK.2025-2.RLTS.T218171971A286370469.es. Accessed on 18 August 2026."));
        Assert.Null(IucnCitationText.ExtractDoi("BirdLife International 2025. Turdoides leucopygia. The IUCN Red List of Threatened Species 2025: e.T22716443A280957797. Accessed on 18 August 2026."));
        Assert.Null(IucnCitationText.ExtractDoi(null));
    }
}
