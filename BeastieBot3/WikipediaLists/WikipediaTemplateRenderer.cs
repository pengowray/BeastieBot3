using System;
using System.Collections.Generic;
using System.IO;
using Stubble.Core.Builders;
using Stubble.Core.Classes;

namespace BeastieBot3.WikipediaLists;

internal sealed class WikipediaTemplateRenderer {
    private static readonly Tags CustomTags = new("<?", "?>");
    private readonly string _templateDirectory;
    private readonly Dictionary<string, string> _templateCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Stubble.Core.StubbleVisitorRenderer _renderer;

    public WikipediaTemplateRenderer(string templateDirectory) {
        if (string.IsNullOrWhiteSpace(templateDirectory)) {
            throw new ArgumentException("Template directory was not provided.", nameof(templateDirectory));
        }

        _templateDirectory = Path.GetFullPath(templateDirectory);
        if (!Directory.Exists(_templateDirectory)) {
            throw new DirectoryNotFoundException($"Template directory not found: {_templateDirectory}");
        }

        _renderer = new StubbleBuilder()
            .Configure(settings => settings.SetDefaultTags(CustomTags))
            .Build();
    }

    public string Render(string? templateName, object? context) {
        if (string.IsNullOrWhiteSpace(templateName)) {
            return string.Empty;
        }

        var template = LoadTemplate(templateName!);
        return _renderer.Render(template, context ?? new { });
    }

    private string LoadTemplate(string templateName) {
        if (_templateCache.TryGetValue(templateName, out var cached)) {
            return cached;
        }

        var fileName = templateName.EndsWith(".mustache", StringComparison.OrdinalIgnoreCase)
            ? templateName
            : templateName + ".mustache";
        var fullPath = Path.Combine(_templateDirectory, fileName);
        if (!File.Exists(fullPath)) {
            throw new FileNotFoundException($"Template '{templateName}' not found at {fullPath}.", fullPath);
        }

        var text = File.ReadAllText(fullPath);
        _templateCache[templateName] = text;
        return text;
    }
}
