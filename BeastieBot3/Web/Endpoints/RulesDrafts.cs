using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

// Copy-on-write drafts of the rules/ files for the web Rules editor and Taxa grouping page.
//
// The draft folder holds only files with edits that have not been applied: saving a file creates its
// draft, Apply copies it to rules/ and deletes it, Discard deletes it. Every other file is read from
// rules/ directly. (The folder used to be a full copy of rules/, seeded once, which went out of date
// as soon as rules/ changed in git, and applying it would have put the old text back.)
//
// When a draft is created, the hash of the rules/ file it started from is recorded in .bases.json.
// If rules/ changes after that, the draft is reported as "source-changed" and Apply needs force, so a
// draft cannot silently undo a newer change. A draft with no recorded base (left from the old full
// copy) is reported as "unknown-base" and also needs force. A draft identical to its rules/ file is
// deleted whenever drafts are listed, since it holds no edits.

namespace BeastieBot3.Web.Endpoints;

internal sealed class RulesDrafts {
    private const string BasesFile = ".bases.json";

    public RulesDrafts(string sourceDir, string draftDir) {
        SourceDir = sourceDir;
        DraftDir = draftDir;
    }

    public static RulesDrafts For(RulesLocations loc) => new(loc.SourceRulesDir, loc.DraftRoot);

    public string SourceDir { get; }
    public string DraftDir { get; }

    internal enum DraftStatus {
        Modified,      // edited, and the rules/ file is as it was when the draft was started
        SourceChanged, // the rules/ file changed after the draft was started
        UnknownBase,   // no record of the rules/ text the draft started from (an old full-copy draft)
        DraftOnly,     // no rules/ file of that name
    }

    public bool HasDraft(string rel) => TryDraftPath(rel, out var p) && File.Exists(p);

    /// <summary>The draft if there is one, otherwise the rules/ file. Null when the path escapes its root.</summary>
    public string? EffectivePath(string rel) {
        if (TryDraftPath(rel, out var draft) && File.Exists(draft)) return draft;
        return TrySourcePath(rel, out var source) ? source : null;
    }

    public string? ReadText(string rel) {
        var path = EffectivePath(rel);
        return path is not null && File.Exists(path) ? File.ReadAllText(path) : null;
    }

    /// <summary>
    /// Saves <paramref name="content"/> as the draft of <paramref name="rel"/>. Returns false when the
    /// content matches the rules/ file, in which case no draft is kept.
    /// </summary>
    public bool Write(string rel, string content) {
        if (!TryDraftPath(rel, out var draft) || !TrySourcePath(rel, out var source)) {
            throw new ArgumentException($"Path escapes the rules folder: {rel}", nameof(rel));
        }
        var sourceText = File.Exists(source) ? File.ReadAllText(source) : null;
        if (sourceText is not null && SameText(sourceText, content)) {
            Discard(rel);
            return false;
        }
        var bases = ReadBases();
        if (!File.Exists(draft) && sourceText is not null) {
            bases[Key(rel)] = Hash(sourceText);
            WriteBases(bases);
        }
        Directory.CreateDirectory(Path.GetDirectoryName(draft)!);
        File.WriteAllText(draft, content);
        return true;
    }

    public void Discard(string rel) {
        if (TryDraftPath(rel, out var draft) && File.Exists(draft)) {
            File.Delete(draft);
        }
        var bases = ReadBases();
        if (bases.Remove(Key(rel))) {
            WriteBases(bases);
        }
    }

    /// <summary>Copies the draft to rules/ and deletes it. Refused (returns the reason) when the draft
    /// would replace a rules/ file that changed after the draft was started, unless forced.</summary>
    public string? Apply(string rel, bool force) {
        if (!TryDraftPath(rel, out var draft) || !TrySourcePath(rel, out var source)) return "path escapes the rules folder";
        if (!File.Exists(draft)) return "no draft";
        var status = StatusOf(rel);
        if (!force && status == DraftStatus.SourceChanged) return "rules/ changed after this draft was started";
        if (!force && status == DraftStatus.UnknownBase) return "this draft may be an old copy of rules/";
        Directory.CreateDirectory(Path.GetDirectoryName(source)!);
        File.Copy(draft, source, overwrite: true); // byte-for-byte; comments + custom_groups preserved
        Discard(rel);
        return null;
    }

    public DraftStatus StatusOf(string rel) {
        if (!TrySourcePath(rel, out var source) || !File.Exists(source)) return DraftStatus.DraftOnly;
        if (!ReadBases().TryGetValue(Key(rel), out var baseHash)) return DraftStatus.UnknownBase;
        return Hash(File.ReadAllText(source)) == baseHash ? DraftStatus.Modified : DraftStatus.SourceChanged;
    }

    /// <summary>Relative paths of the drafts, after deleting any that match their rules/ file.</summary>
    public List<string> DraftFiles() {
        var result = new List<string>();
        foreach (var rel in EnumerateRelative(DraftDir)) {
            var draft = Path.Combine(DraftDir, rel);
            var source = Path.Combine(SourceDir, rel);
            if (File.Exists(source) && SameText(File.ReadAllText(source), File.ReadAllText(draft))) {
                Discard(rel);
                continue;
            }
            result.Add(rel);
        }
        return result;
    }

    /// <summary>Every file in rules/ plus any draft-only file.</summary>
    public List<string> AllFiles() =>
        EnumerateRelative(SourceDir).Concat(DraftFiles())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(p => p, StringComparer.OrdinalIgnoreCase)
            .ToList();

    /// <summary>
    /// A temporary folder holding rules/ with the drafts on top, for code that loads the whole rules
    /// set from one folder (the list definition loader). The caller deletes it.
    /// </summary>
    public string MaterializeMerged() {
        var dir = Directory.CreateTempSubdirectory("bb3-rules-merged-").FullName;
        foreach (var rel in EnumerateRelative(SourceDir)) {
            CopyTo(Path.Combine(SourceDir, rel), Path.Combine(dir, rel));
        }
        foreach (var rel in EnumerateRelative(DraftDir)) {
            CopyTo(Path.Combine(DraftDir, rel), Path.Combine(dir, rel));
        }
        return dir;

        static void CopyTo(string from, string to) {
            Directory.CreateDirectory(Path.GetDirectoryName(to)!);
            File.Copy(from, to, overwrite: true);
        }
    }

    // ---- helpers ----

    private bool TryDraftPath(string rel, out string path) =>
        SafePaths.TryResolveUnder(DraftDir, rel, out path, out _) && !IsBasesFile(path);

    private bool TrySourcePath(string rel, out string path) =>
        SafePaths.TryResolveUnder(SourceDir, rel, out path, out _);

    private bool IsBasesFile(string fullPath) =>
        string.Equals(fullPath, Path.Combine(DraftDir, BasesFile), StringComparison.OrdinalIgnoreCase);

    private static IEnumerable<string> EnumerateRelative(string root) {
        if (!Directory.Exists(root)) yield break;
        foreach (var f in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)) {
            var rel = Path.GetRelativePath(root, f);
            if (rel == BasesFile) continue;
            yield return rel;
        }
    }

    // Line endings don't count as an edit (the rules files are a mix of CRLF and LF).
    private static bool SameText(string a, string b) =>
        string.Equals(a.Replace("\r\n", "\n"), b.Replace("\r\n", "\n"), StringComparison.Ordinal);

    private static string Hash(string text) =>
        Convert.ToHexString(SHA1.HashData(Encoding.UTF8.GetBytes(text.Replace("\r\n", "\n"))));

    private static string Key(string rel) => rel.Replace('\\', '/');

    private Dictionary<string, string> ReadBases() {
        var path = Path.Combine(DraftDir, BasesFile);
        if (!File.Exists(path)) return new(StringComparer.OrdinalIgnoreCase);
        try {
            var map = JsonSerializer.Deserialize<Dictionary<string, string>>(File.ReadAllText(path));
            return map is null
                ? new(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, string>(map, StringComparer.OrdinalIgnoreCase);
        } catch (JsonException) {
            return new(StringComparer.OrdinalIgnoreCase);
        }
    }

    private void WriteBases(Dictionary<string, string> bases) {
        var path = Path.Combine(DraftDir, BasesFile);
        if (bases.Count == 0) {
            if (File.Exists(path)) File.Delete(path);
            return;
        }
        Directory.CreateDirectory(DraftDir);
        File.WriteAllText(path, JsonSerializer.Serialize(bases, new JsonSerializerOptions { WriteIndented = true }));
    }
}
