namespace BeastieBot3.Shared.SiteData;

/// The words of a folded name (SiteNameKey.Fold) for the site's spelling suggestions: runs of
/// letters at least MinLength long. site build-db stores them in name_word; the search page splits
/// the search text the same way.
public static class NameWords {
    public const int MinLength = 3;

    /// Where each word is in the text.
    public static IEnumerable<(int Start, int Length)> Find(string folded) {
        var start = -1;
        for (var i = 0; i <= folded.Length; i++) {
            var letter = i < folded.Length && char.IsLetter(folded[i]);
            if (letter && start < 0) {
                start = i;
            } else if (!letter && start >= 0) {
                if (i - start >= MinLength) {
                    yield return (start, i - start);
                }
                start = -1;
            }
        }
    }
}
