using BeastieBot3.Shared.Wikitext;
using BeastieBot3.Site.Data;
using BeastieBot3.Site.Pages;

namespace BeastieBot3.Site.Update;

public sealed partial class StatusUpdater {
    // The citation that replaces a {{cite iucn}} of an older assessment, in status_ref or elsewhere:
    // {{cite iucn}} for the latest assessment, or {{cite Q}} for its Wikidata item when the reader
    // asked for it (StatusUpdateOptions.CiteQ) and the assessment has one. No <ref>: only the
    // template is replaced, inside whatever wraps it.
    private string ReplacementCitation(AssessmentRow latest, IucnCitationParts parts) {
        DateOnly? downloaded = parts.DownloadedAtUtc is { } at ? DateOnly.FromDateTime(at) : null;
        return IucnReference.Render(_options.CiteQ ? ReferenceTemplate.CiteQ : ReferenceTemplate.CiteIucn, parts,
            latest.WikidataItemQid, latest.WikidataItemProperties,
            WikitextOptions.Default.ToCiteIucnOptions(_today, downloaded) with { WrapInRef = false },
            new CiteQOptions { AccessDate = downloaded, WrapInRef = false })!;
    }
}
