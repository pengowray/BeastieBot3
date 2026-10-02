using System.Text;

namespace BeastieBot3.Shared.Wikitext;

// {{IUCN status}} badges. Module:IUCN status (en-wiki) lower-cases the code and accepts ex, ew, cr,
// en, vu, nt, lc, dd, re, na, ne, pe, pew and lr/cd, with cr(pe), cr(pew), lr/nt and lr/lc as aliases;
// |2= "taxonId/assessmentId" links the assessment, |3=1 shows that link, |year= makes the link text
// "IUCN <year>" and |label= replaces it. The CLI's IUCN and SPRAT lists call this through
// IucnRedlistStatus.BuildStatusTemplate, so a change here changes every generated list.

public static class IucnStatusTemplate {
    // IUCN's category names as the CSV export and the API spell them, mapped to the template codes.
    private static readonly Dictionary<string, string> CategoryNames = new(StringComparer.OrdinalIgnoreCase) {
        ["Extinct"] = "EX",
        ["Extinct in the Wild"] = "EW",
        ["Critically Endangered"] = "CR",
        ["Endangered"] = "EN",
        ["Vulnerable"] = "VU",
        ["Near Threatened"] = "NT",
        ["Least Concern"] = "LC",
        ["Data Deficient"] = "DD",
        ["Regionally Extinct"] = "RE",
        ["Not Applicable"] = "NA",
        ["Not Evaluated"] = "NE",
        ["Lower Risk/conservation dependent"] = "LR/cd",
        ["Lower Risk/near threatened"] = "LR/nt",
        ["Lower Risk/least concern"] = "LR/lc",
    };

    /// The {{IUCN status}} code for an IUCN category: CR with the possibly extinct flags becomes
    /// CR(PE) or CR(PEW); LR/cd, LR/nt and LR/lc keep IUCN's case.
    /// The category may be a code ("EN", "lr/nt") or IUCN's full name ("Endangered",
    /// "Lower Risk/near threatened"). An unknown value comes back upper-cased.
    public static string ToTemplateCode(string category, bool possiblyExtinct, bool possiblyExtinctInTheWild) {
        // Codes are matched on the upper-cased input without trimming, which is how the CLI's list
        // generator has always done it (IucnRedlistStatus forwards here and its output must not move).
        var normalized = CategoryNames.TryGetValue(category.Trim(), out var fromName)
            ? fromName.ToUpperInvariant()
            : category.ToUpperInvariant();

        if (normalized == "CR") {
            if (possiblyExtinct) {
                return "CR(PE)";
            }
            if (possiblyExtinctInTheWild) {
                return "CR(PEW)";
            }
            return "CR";
        }

        return normalized switch {
            "CR(PE)" or "PE" => "CR(PE)",
            "CR(PEW)" or "PEW" => "CR(PEW)",
            "LR/CD" or "CD" => "LR/cd",
            "LR/NT" => "LR/nt",
            "LR/LC" => "LR/lc",
            _ => normalized
        };
    }

    /// {{IUCN status|CODE|taxonId/assessmentId|1|year=YYYY}}; no year for EX and EW. With
    /// yearAsBareLabel the year goes in |label= instead of |year=.
    public static string Render(string category, bool possiblyExtinct, bool possiblyExtinctInTheWild,
        long taxonId, long assessmentId, string? yearPublished, bool yearAsBareLabel = false) {
        var statusCode = ToTemplateCode(category, possiblyExtinct, possiblyExtinctInTheWild);
        var sb = new StringBuilder();
        sb.Append("{{IUCN status|").Append(statusCode).Append('|')
          .Append(taxonId).Append('/').Append(assessmentId).Append("|1"); // 1 = make link visible
        if (!IsExtinctCode(statusCode) && !string.IsNullOrWhiteSpace(yearPublished)) {
            // year= renders the link as "IUCN <year>"; label= renders just "<year>". The bare-label form
            // is used where the surrounding text already says "IUCN:" (the SPRAT Australia lists), so the
            // "IUCN" prefix would be redundant; the standalone IUCN lists keep "IUCN <year>".
            sb.Append(yearAsBareLabel ? "|label=" : "|year=").Append(yearPublished);
        }
        sb.Append("}}");
        return sb.ToString();
    }

    private static bool IsExtinctCode(string code) => code.ToUpperInvariant() is "EX" or "EW";
}
