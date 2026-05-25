using System.ComponentModel;
using System.Reflection;
using System.Text.Json.Serialization;
using Spectre.Console.Cli;

namespace BeastieBot3.Web.Commands;

// Reflects over a command's nested `Settings` type and converts each
// [CommandOption] property into a UI-friendly form field schema.
//
// The .NET property type drives the input control:
//   bool / bool?           -> checkbox
//   int / int? / long / long?  -> number
//   double / double? / decimal -> number (step="any")
//   enum                   -> select (with the enum's value names)
//   string / string?       -> text
//   string[]               -> repeated text (comma-separated for the MVP; the
//                             browser splits on commas before sending args)
//
// Common base-class options inherited from CommonSettings (--settings-dir,
// --ini-file) are filtered out: they are global and the web UI does not
// need to surface them on every form.

public static class CommandReflector {
    private static readonly HashSet<string> SuppressedOptionNames = new(StringComparer.OrdinalIgnoreCase) {
        "--settings-dir", "-s",
        "--ini-file",
    };

    public static FormSchema BuildSchema(Type commandType) {
        var settingsType = ResolveSettingsType(commandType);
        var fields = new List<FormField>();
        if (settingsType is not null) {
            foreach (var prop in WalkProperties(settingsType)) {
                var optAttr = prop.GetCustomAttribute<CommandOptionAttribute>();
                if (optAttr is null) continue;
                var parsed = ParseOptionTemplate(optAttr.LongNames?.FirstOrDefault(), optAttr);
                if (parsed is null) continue;
                if (SuppressedOptionNames.Contains(parsed.PrimaryName)) continue;
                if (parsed.AllNames.Any(n => SuppressedOptionNames.Contains(n))) continue;

                var descAttr = prop.GetCustomAttribute<DescriptionAttribute>();
                var defaultAttr = prop.GetCustomAttribute<DefaultValueAttribute>();
                var (kind, choices) = ClassifyType(prop.PropertyType);
                fields.Add(new FormField {
                    Name = parsed.PrimaryName,
                    AltNames = parsed.AllNames.Where(n => n != parsed.PrimaryName).ToList(),
                    Description = descAttr?.Description,
                    Kind = kind,
                    Choices = choices,
                    Placeholder = parsed.ValueLabel,
                    DefaultValue = defaultAttr?.Value?.ToString(),
                    HasDefault = defaultAttr is not null,
                });
            }
        }
        return new FormSchema { Fields = fields };
    }

    // Spectre's CommandOption / CommandArgument attributes live on the Settings
    // type — which is conventionally nested as `XCommand.Settings`. Some commands
    // use a top-level settings type instead (e.g. WikidataSeedSettings); for
    // those we infer the type from the Command<T>/AsyncCommand<T> base.
    private static Type? ResolveSettingsType(Type commandType) {
        var t = commandType.BaseType;
        while (t is not null) {
            if (t.IsGenericType) {
                var def = t.GetGenericTypeDefinition();
                if (def == typeof(Command<>) || def == typeof(AsyncCommand<>)) {
                    return t.GetGenericArguments()[0];
                }
            }
            t = t.BaseType;
        }
        return null;
    }

    // Walk public instance properties on the settings type plus its base chain.
    // Spectre handles inheritance naturally so we surface inherited options too.
    private static IEnumerable<PropertyInfo> WalkProperties(Type t) {
        var seen = new HashSet<string>();
        var current = t;
        while (current is not null && current != typeof(object) && current != typeof(CommandSettings)) {
            foreach (var p in current.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)) {
                if (seen.Add(p.Name)) yield return p;
            }
            current = current.BaseType;
        }
    }

    // Spectre's [CommandOption] template parser. Examples:
    //   "-d|--database <PATH>"
    //   "--force"
    //   "-o|--output <FILE>"
    private static ParsedTemplate? ParseOptionTemplate(string? _, CommandOptionAttribute attr) {
        var names = new List<string>();
        foreach (var n in attr.ShortNames ?? Array.Empty<string>()) names.Add("-" + n);
        foreach (var n in attr.LongNames ?? Array.Empty<string>()) names.Add("--" + n);
        if (names.Count == 0) return null;
        var primary = names.FirstOrDefault(n => n.StartsWith("--")) ?? names[0];
        return new ParsedTemplate {
            PrimaryName = primary,
            AllNames = names,
            ValueLabel = attr.ValueName,
        };
    }

    private static (FormFieldKind kind, IReadOnlyList<string>? choices) ClassifyType(Type t) {
        var u = Nullable.GetUnderlyingType(t) ?? t;
        if (u == typeof(bool)) return (FormFieldKind.Flag, null);
        if (u == typeof(int) || u == typeof(long) || u == typeof(short)) return (FormFieldKind.Integer, null);
        if (u == typeof(double) || u == typeof(float) || u == typeof(decimal)) return (FormFieldKind.Number, null);
        if (u.IsEnum) return (FormFieldKind.Choice, Enum.GetNames(u));
        if (u == typeof(string)) return (FormFieldKind.Text, null);
        if (u == typeof(string[]) || (u.IsArray && u.GetElementType() == typeof(string))) {
            return (FormFieldKind.List, null);
        }
        // Fallback: treat unknowns as text inputs so the form still renders.
        return (FormFieldKind.Text, null);
    }

    private sealed class ParsedTemplate {
        public required string PrimaryName { get; init; }
        public required List<string> AllNames { get; init; }
        public string? ValueLabel { get; init; }
    }
}

[JsonConverter(typeof(JsonStringEnumConverter<FormFieldKind>))]
public enum FormFieldKind {
    Flag,    // bool -> checkbox; if true, the option name is appended with no value
    Text,
    Integer,
    Number,
    Choice,  // enum -> select
    List,    // string[] -> comma-separated text; web client splits + appends repeated --name
}

public sealed class FormSchema {
    public required IReadOnlyList<FormField> Fields { get; init; }
}

public sealed class FormField {
    public required string Name { get; init; }
    public IReadOnlyList<string> AltNames { get; init; } = Array.Empty<string>();
    public string? Description { get; init; }
    public required FormFieldKind Kind { get; init; }
    public IReadOnlyList<string>? Choices { get; init; }
    public string? Placeholder { get; init; }
    public string? DefaultValue { get; init; }
    public bool HasDefault { get; init; }
}
