using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BeastieBot3.Configuration;
using BeastieBot3.Web.Endpoints;
using YamlDotNet.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

// The probable scopes in rules/iucn-probable-scopes.yml: for each assessment IUCN published with no
// geographic scope, the scope this site thinks it has (a country for a national assessment, or a
// region), with the evidence in this site's own words. site build-db writes them to the site
// database's probable_scope table and the audit site's empty-scope page lists them.

namespace BeastieBot3.Iucn;

/// One assessment's probable scope. Kind: "national" or "regional". TaxonName: the name the file gives
/// beside the id, for checking.
internal sealed record IucnProbableScope(long AssessmentId, string Scope, string Kind, string Evidence, string? TaxonName);

internal sealed class IucnProbableScopes {
    public const string FileName = "iucn-probable-scopes.yml";
    public const string National = "national";
    public const string Regional = "regional";

    public static readonly IucnProbableScopes None = new(new Dictionary<long, IucnProbableScope>(), null);

    private readonly IReadOnlyDictionary<long, IucnProbableScope> _byAssessment;

    private IucnProbableScopes(IReadOnlyDictionary<long, IucnProbableScope> byAssessment, string? sourcePath) {
        _byAssessment = byAssessment;
        SourcePath = sourcePath;
    }

    /// The file they came from, or null when there was none.
    public string? SourcePath { get; }

    public IEnumerable<IucnProbableScope> All => _byAssessment.Values;

    public int Count => _byAssessment.Count;

    public IucnProbableScope? For(long assessmentId) => _byAssessment.GetValueOrDefault(assessmentId);

    /// The file in the editable rules folder, else the build-output copy; none when neither exists.
    public static IucnProbableScopes LoadForPaths(PathsService paths) {
        var rules = RulesPaths.Resolve(paths);
        var fromSource = LoadFromRulesDir(rules.SourceRulesDir);
        return fromSource.SourcePath is not null ? fromSource : LoadFromRulesDir(rules.BuildOutputRulesDir);
    }

    public static IucnProbableScopes LoadFromRulesDir(string? rulesDir) =>
        string.IsNullOrWhiteSpace(rulesDir) ? None : Load(Path.Combine(rulesDir, FileName));

    /// Reads the file. A missing file means none. A file that cannot be read, a group without a scope,
    /// kind or evidence, an unknown kind or an assessment in two groups throws
    /// <see cref="InvalidOperationException"/> naming the file.
    public static IucnProbableScopes Load(string path) {
        if (!File.Exists(path)) {
            return None;
        }
        try {
            return Parse(Deserializer.Deserialize<RulesFile>(File.ReadAllText(path)), path);
        } catch (YamlException ex) {
            throw new InvalidOperationException($"{path}: {ex.Message}", ex);
        }
    }

    /// From YAML text; for tests.
    public static IucnProbableScopes FromYaml(string yaml) => Parse(Deserializer.Deserialize<RulesFile>(yaml), sourcePath: null);

    private static readonly IDeserializer Deserializer = new DeserializerBuilder()
        .WithNamingConvention(UnderscoredNamingConvention.Instance)
        .Build();

    private static IucnProbableScopes Parse(RulesFile? file, string? sourcePath) {
        var name = sourcePath ?? FileName;
        var byAssessment = new Dictionary<long, IucnProbableScope>();
        var groups = file?.Scopes ?? new List<RawGroup>();
        for (var i = 0; i < groups.Count; i++) {
            var group = groups[i];
            string Required(string? value, string field) => string.IsNullOrWhiteSpace(value)
                ? throw new InvalidOperationException($"{name}: group {i + 1} under 'scopes' has no {field}.")
                : value.Trim();
            var scope = Required(group.Scope, "scope");
            var kind = Required(group.Kind, "kind").ToLowerInvariant();
            if (kind is not (National or Regional)) {
                throw new InvalidOperationException($"{name}: group {i + 1} ({scope}) has kind '{kind}'; use '{National}' or '{Regional}'.");
            }
            var evidence = Required(group.Evidence, "evidence");
            foreach (var (assessmentId, taxonName) in group.Assessments ?? new Dictionary<long, string?>()) {
                if (!byAssessment.TryAdd(assessmentId, new IucnProbableScope(assessmentId, scope, kind, evidence, taxonName?.Trim()))) {
                    throw new InvalidOperationException($"{name}: assessment {assessmentId} is in two groups.");
                }
            }
        }
        return new IucnProbableScopes(byAssessment, sourcePath);
    }

    private sealed class RulesFile {
        public List<RawGroup>? Scopes { get; set; }
    }

    private sealed class RawGroup {
        public string? Scope { get; set; }
        public string? Kind { get; set; }
        public string? Evidence { get; set; }
        public Dictionary<long, string?>? Assessments { get; set; }
    }
}
