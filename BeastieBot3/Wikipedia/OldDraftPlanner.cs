using System;
using System.Collections.Generic;
using System.Linq;

// Decides what wikipedia post-drafts does with a draft page that no current list produces: a list
// renamed only in letter case (List of Fungi -> List of fungi) has its old page moved to the new
// title, so the history stays with the list; any other old Beastie Bot draft is blanked. Pages
// under the base title that Beastie Bot did not make, redirects (left by earlier moves) and pages
// already blanked are not touched. Pure, so the decisions can be pinned by tests.

namespace BeastieBot3.Wikipedia;

internal enum OldDraftAction {
    Move,
    Blank,
    LeaveAlone,
}

/// <param name="NewTitle">The current draft title a <see cref="OldDraftAction.Move"/> goes to.</param>
internal sealed record OldDraftStep(string Title, OldDraftAction Action, string? NewTitle = null);

internal static class OldDraftPlanner {
    /// <param name="subpages">Every page below the base title, from the wiki.</param>
    /// <param name="texts">Current text of each non-redirect subpage that is not a current draft.</param>
    /// <param name="draftTitles">The draft titles this run posts.</param>
    /// <param name="existsOnWiki">Whether a current draft title already has a page.</param>
    /// <param name="retiredText">The text of a blanked old draft, to recognise one blanked earlier.</param>
    public static IReadOnlyList<OldDraftStep> Plan(
        IReadOnlyList<SubpageInfo> subpages,
        IReadOnlyDictionary<string, string?> texts,
        IReadOnlyCollection<string> draftTitles,
        Func<string, bool> existsOnWiki,
        string retiredText) {
        var current = new HashSet<string>(draftTitles, StringComparer.Ordinal);
        var claimed = new HashSet<string>(StringComparer.Ordinal);
        var retired = DraftPageBuilder.Normalize(retiredText).Trim();
        var steps = new List<OldDraftStep>();

        foreach (var page in subpages.OrderBy(p => p.Title, StringComparer.Ordinal)) {
            if (page.IsRedirect || current.Contains(page.Title)) {
                continue;
            }
            if (!texts.TryGetValue(page.Title, out var text) || text is null) {
                continue;
            }
            if (DraftPageBuilder.Normalize(text).Trim() == retired) {
                continue;
            }
            if (!DraftPageBuilder.IsBotDraft(text)) {
                steps.Add(new OldDraftStep(page.Title, OldDraftAction.LeaveAlone));
                continue;
            }

            var target = draftTitles.FirstOrDefault(t => string.Equals(t, page.Title, StringComparison.OrdinalIgnoreCase));
            if (target is not null && !existsOnWiki(target) && claimed.Add(target)) {
                steps.Add(new OldDraftStep(page.Title, OldDraftAction.Move, target));
            } else {
                steps.Add(new OldDraftStep(page.Title, OldDraftAction.Blank));
            }
        }

        return steps;
    }
}
