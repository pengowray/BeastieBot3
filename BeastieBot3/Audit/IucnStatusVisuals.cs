using System;
using BeastieBot3.WikipediaLists;

// Colour + label for a Red List category badge. Colours are ported from the project's legacy
// palette (BeastieLegacy RedStatus.HexColor) and are used only to make long lists easier to
// scan. They are not the official IUCN category colours; the audit pages say so.

namespace BeastieBot3.Audit;

internal readonly record struct StatusVisual(string Code, string Label, string Background, string Text);

internal static class IucnStatusVisuals {
    // Returns a badge visual for a status code or category text (e.g. "CR", "CR(PE)",
    // "Critically Endangered", "LR/nt"). Unknown values get a neutral grey badge.
    public static StatusVisual For(string? statusCodeOrCategory) {
        if (string.IsNullOrWhiteSpace(statusCodeOrCategory)) {
            return new StatusVisual("", "", "#cccccc", "#333333");
        }

        // Accept either a short code ("CR", "CR(PE)") or the full category text ("Critically
        // Endangered"): try the code map first, then resolve category text.
        var descriptor = IucnRedlistStatus.TryGetDescriptor(statusCodeOrCategory, out var d)
            ? d
            : IucnRedlistStatus.ResolveFromDatabase(statusCodeOrCategory, null, null);
        var code = descriptor.Code;
        var label = descriptor.Category;
        var (bg, fg) = Palette(descriptor.TemplateName, code);
        return new StatusVisual(code, label, bg, fg);
    }

    private static (string Background, string Text) Palette(string templateCode, string fullCode) {
        // Match on the full code first (so CR(PE)/CR(PEW) share the CR colour), then the base.
        var key = fullCode.ToUpperInvariant();
        switch (key) {
            case "EX": return ("#000000", "#ffffff");
            case "EW": return ("#542344", "#ffffff");
            case "RE": return ("#3a3a3a", "#ffffff");
        }

        return templateCode.ToUpperInvariant() switch {
            "CR" or "CR(PE)" or "CR(PEW)" or "PE" or "PEW" => ("#cc3333", "#ffffff"),
            "EN" => ("#cc6633", "#ffffff"),
            "VU" => ("#cc9900", "#1d1d1d"),
            "NT" or "LR/NT" => ("#99cc99", "#173a17"),
            "LR/CD" or "CD" => ("#99cc99", "#173a17"),
            "LC" or "LR/LC" => ("#006666", "#ffffff"),
            "DD" => ("#aaaaaa", "#1d1d1d"),
            _ => ("#999999", "#ffffff"),
        };
    }
}
