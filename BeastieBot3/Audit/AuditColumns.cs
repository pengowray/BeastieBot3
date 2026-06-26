using BeastieBot3.Audit.Model;

// Reusable column factories so every report describes its table from the same building blocks.
// A producer composes a column list from these (plus AuditColumn.Extra for report-specific
// values), and both the HTML table and the CSV come out consistent. This is the column-level
// counterpart to HtmlListRenderer: define once, render everywhere.

namespace BeastieBot3.Audit;

internal static class AuditColumns {
    public static AuditColumn ScientificName(string header = "Scientific name") => new() {
        Key = "scientificName", Header = header, Type = AuditColumnType.Taxon,
        Value = f => f.ScientificName, Href = f => f.RedlistUrl,
    };

    public static AuditColumn CommonName(string header = "Common name") => new() {
        Key = "commonName", Header = header, Type = AuditColumnType.Text, Value = f => f.CommonName,
    };

    public static AuditColumn Rank(string header = "Rank") => new() {
        Key = "rank", Header = header, Type = AuditColumnType.Text, Value = f => f.Rank,
    };

    public static AuditColumn Status(string header = "Status") => new() {
        Key = "statusCode", Header = header, Type = AuditColumnType.Status,
        Value = f => f.StatusCode, SortKey = f => AuditMapping.StatusSortKey(f.StatusCode),
        Help = "IUCN Red List category. Badge colour is a reading aid, not the official colour.",
    };

    public static AuditColumn Category(string header = "IUCN category") => new() {
        Key = "iucnCategoryText", Header = header, Type = AuditColumnType.Text, Value = f => f.StatusCategory,
    };

    public static AuditColumn Kingdom(string header = "Kingdom") => new() {
        Key = "kingdom", Header = header, Type = AuditColumnType.Text, Value = f => f.Kingdom,
    };

    public static AuditColumn Class(string header = "Class") => new() {
        Key = "class", Header = header, Type = AuditColumnType.Text, Value = f => f.Class,
    };

    public static AuditColumn Order(string header = "Order") => new() {
        Key = "order", Header = header, Type = AuditColumnType.Text, Value = f => f.Order,
    };

    public static AuditColumn Family(string header = "Family") => new() {
        Key = "family", Header = header, Type = AuditColumnType.Text, Value = f => f.Family,
    };

    public static AuditColumn Year(string header = "Year") => new() {
        Key = "yearPublished", Header = header, Type = AuditColumnType.Number, Value = f => f.YearPublished,
    };

    public static AuditColumn Latest(string header = "Latest") => new() {
        Key = "latest", Header = header, Type = AuditColumnType.Text, Value = f => AuditMapping.BoolToYesNo(f.Latest),
    };

    public static AuditColumn TaxonId(string header = "SIS id") => new() {
        Key = "taxonId", Header = header, Type = AuditColumnType.Number, Value = f => AuditMapping.LongToString(f.TaxonId),
        Help = "IUCN Species Information Service (SIS) taxon id.",
    };

    public static AuditColumn AssessmentId(string header = "Assessment") => new() {
        Key = "assessmentId", Header = header, Type = AuditColumnType.Number, Value = f => AuditMapping.LongToString(f.AssessmentId),
    };

    public static AuditColumn RedlistLink(string header = "Red List") => new() {
        Key = "redlistUrl", Header = header, Type = AuditColumnType.Url,
        Value = f => f.RedlistUrl, Href = f => f.RedlistUrl,
    };

    public static AuditColumn Field(string header = "Field") => new() {
        Key = "field", Header = header, Type = AuditColumnType.Text, Value = f => f.Field,
    };

    public static AuditColumn IssueType(string header = "Observation") => new() {
        Key = "issueType", Header = header, Type = AuditColumnType.Text, Value = f => f.IssueType,
    };

    public static AuditColumn CurrentValue(string header = "Current value", AuditColumnType type = AuditColumnType.Code) => new() {
        Key = "currentValue", Header = header, Type = type, Value = f => f.CurrentValue,
    };

    public static AuditColumn SuggestedValue(string header = "Normalised value", AuditColumnType type = AuditColumnType.Code) => new() {
        Key = "suggestedValue", Header = header, Type = type, Value = f => f.SuggestedValue,
    };

    public static AuditColumn Detail(string header = "Detail") => new() {
        Key = "detail", Header = header, Type = AuditColumnType.LongText, Value = f => f.Detail,
    };

    // A column backed by AuditFinding.Extra (report-specific values not in the canonical set).
    public static AuditColumn Custom(string key, string header, AuditColumnType type, string? help = null) => new() {
        Key = key, Header = header, Type = type, Value = f => f.Get(key), Help = help,
    };
}
