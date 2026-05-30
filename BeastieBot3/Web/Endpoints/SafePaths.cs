using System;
using System.IO;

namespace BeastieBot3.Web.Endpoints;

// Shared path-traversal guard for the file/rules endpoints. Mirrors the original
// FilesEndpoints.TryResolveTarget logic so a relative path can never escape its root.
internal static class SafePaths {
    public static bool TryResolveUnder(string rootPath, string relative, out string fullPath, out string error) {
        fullPath = "";
        error = "";
        var resolved = Path.GetFullPath(Path.Combine(rootPath, relative ?? ""));
        var rootWithSep = rootPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                          + Path.DirectorySeparatorChar;
        if (!resolved.StartsWith(rootWithSep, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(resolved, rootPath, StringComparison.OrdinalIgnoreCase)) {
            error = "Path escapes its root.";
            return false;
        }
        fullPath = resolved;
        return true;
    }
}
