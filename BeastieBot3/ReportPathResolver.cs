using System;
using System.IO;

namespace BeastieBot3;

internal static class ReportPathResolver {
    public static string ResolveDirectory(PathsService paths, string? explicitDirectory, string? fallbackBaseDirectory) {
        if (!string.IsNullOrWhiteSpace(explicitDirectory)) {
            return EnsureDirectory(explicitDirectory);
        }

        var configured = paths.GetReportOutputDirectory();
        if (!string.IsNullOrWhiteSpace(configured)) {
            return EnsureDirectory(configured);
        }

        var baseDir = !string.IsNullOrWhiteSpace(fallbackBaseDirectory)
            ? fallbackBaseDirectory
            : paths.BaseDirectory;

        if (string.IsNullOrWhiteSpace(baseDir)) {
            baseDir = Environment.CurrentDirectory;
        }

        var defaultDir = Path.Combine(baseDir, "data-analysis");
        return EnsureDirectory(defaultDir);
    }

    public static string ResolveFilePath(
        PathsService paths,
        string? explicitFilePath,
        string? explicitDirectory,
        string? fallbackBaseDirectory,
        string defaultFileName) {
        if (!string.IsNullOrWhiteSpace(explicitFilePath)) {
            var full = Path.GetFullPath(explicitFilePath);
            var directory = Path.GetDirectoryName(full);
            EnsureDirectoryExists(directory);
            return full;
        }

        var directoryPath = ResolveDirectory(paths, explicitDirectory, fallbackBaseDirectory);
        return Path.Combine(directoryPath, defaultFileName);
    }

    private static string EnsureDirectory(string path) {
        var fullPath = Path.GetFullPath(path);
        Directory.CreateDirectory(fullPath);
        return fullPath;
    }

    private static void EnsureDirectoryExists(string? directory) {
        if (string.IsNullOrWhiteSpace(directory)) {
            return;
        }

        Directory.CreateDirectory(directory);
    }
}
