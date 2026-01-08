using System;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace BeastieBot3.WikipediaLists;

internal sealed class WikipediaListDefinitionLoader {
    private readonly IDeserializer _deserializer;

    public WikipediaListDefinitionLoader() {
        _deserializer = new DeserializerBuilder()
            .IgnoreUnmatchedProperties()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();
    }

    public WikipediaListConfig Load(string filePath) {
        if (string.IsNullOrWhiteSpace(filePath)) {
            throw new ArgumentException("Configuration path was not provided.", nameof(filePath));
        }

        var fullPath = Path.GetFullPath(filePath);
        if (!File.Exists(fullPath)) {
            throw new FileNotFoundException($"Wikipedia list config not found: {fullPath}", fullPath);
        }

        using var reader = File.OpenText(fullPath);
        var config = _deserializer.Deserialize<WikipediaListConfig>(reader);
        if (config is null) {
            throw new InvalidOperationException($"Unable to parse Wikipedia list config at {fullPath}.");
        }

        return config;
    }
}
