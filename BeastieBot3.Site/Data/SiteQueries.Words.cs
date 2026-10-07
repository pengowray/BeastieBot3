using System.Runtime.CompilerServices;

namespace BeastieBot3.Site.Data;

public sealed partial class SiteQueries {
    // One word index per database file: a new file has a new snapshot.
    private static readonly ConditionalWeakTable<SiteSnapshot, Lazy<NameWordIndex>> WordIndexes = new();

    /// The words of the site's names, loaded the first time a search needs them; null when the
    /// database is not ready.
    public NameWordIndex? GetNameWordIndex() {
        if (_db.Snapshot is not { } snapshot) {
            return null;
        }
        return WordIndexes.GetValue(snapshot, _ => new Lazy<NameWordIndex>(() => {
            using var connection = _db.OpenConnection();
            return NameWordIndex.Load(connection);
        })).Value;
    }
}
