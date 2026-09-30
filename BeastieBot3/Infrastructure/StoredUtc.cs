using System;
using System.Globalization;

namespace BeastieBot3.Infrastructure;

// Timestamps such as downloaded_at are written to the caches as UTC "O" strings. Plain
// DateTime.TryParse converts the trailing Z to LOCAL time, and DateTime comparison ignores Kind,
// so comparing the result against a UTC cutoff shifts the boundary by the machine's offset (ten
// hours in Australia). Read them back with this instead. A string with no offset is taken as UTC.
internal static class StoredUtc {
    internal static DateTime? Parse(string? text) =>
        DateTime.TryParse(text, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var parsed)
            ? parsed
            : null;
}
