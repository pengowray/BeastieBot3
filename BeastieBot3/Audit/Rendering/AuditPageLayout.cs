using System.Globalization;
using System.Linq;
using System.Text;
using BeastieBot3.Audit.Model;

// Shared page chrome for every page in the bundle: head, header with the site title, optional
// breadcrumbs, the body, and a footer carrying the one unofficial disclaimer, attribution, licence,
// and generation date. Asset links are relative so the
// bundle works at any base URL (a local folder, a static host, an email attachment). A --limit run
// also gets a notice at the top of every page, since every count on it is partial.

namespace BeastieBot3.Audit.Rendering;

internal static class AuditPageLayout {
    // In the head of every page. AuditSiteRenderer also uses it to recognise pages an earlier run
    // wrote, so it must stay byte-identical to what earlier versions wrote.
    public const string StylesheetLink = "<link rel=\"stylesheet\" href=\"assets/audit.css\">";

    // Before the stylesheet and not deferred: theme.js sets the theme chosen in this browser
    // before the page is drawn.
    public const string ThemeScript = "<script src=\"assets/theme.js\"></script>";

    public static string Page(AuditDocument doc, string pageTitle, string? crumbsHtml, string bodyHtml, bool wide = false) {
        var cfg = doc.Config;
        var fullTitle = pageTitle.Length == 0 ? cfg.SiteTitle : $"{pageTitle} · {cfg.SiteTitle}";
        if (doc.IsLimited) {
            fullTitle = $"Partial results · {fullTitle}";
        }
        var sb = new StringBuilder();
        sb.Append("<!doctype html>\n<html lang=\"en\">\n<head>\n");
        sb.Append("<meta charset=\"utf-8\">\n");
        sb.Append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
        sb.Append("<meta name=\"robots\" content=\"noindex\">\n");
        sb.Append("<meta name=\"color-scheme\" content=\"light dark\">\n");
        sb.Append($"<title>{HtmlText.Escape(fullTitle)}</title>\n");
        sb.Append(ThemeScript).Append('\n');
        sb.Append(StylesheetLink).Append('\n');
        sb.Append(wide ? "</head>\n<body class=\"wide\">\n" : "</head>\n<body>\n");

        sb.Append("<header class=\"site\">\n<div class=\"wrap\">\n");
        sb.Append("<div class=\"site-top\">\n<div class=\"site-id\">\n");
        sb.Append($"<h1><a href=\"index.html\" style=\"color:inherit\">{HtmlText.Escape(cfg.SiteTitle)}</a></h1>\n");
        sb.Append($"<div class=\"release\">{HtmlText.Escape(cfg.Subtitle)} · IUCN Red List version {HtmlText.Escape(doc.Release)}</div>\n");
        sb.Append("</div>\n");
        sb.Append(ThemeControl);
        sb.Append("</div>\n");
        if (!string.IsNullOrEmpty(crumbsHtml)) {
            sb.Append($"<nav class=\"crumbs\">{crumbsHtml}</nav>\n");
        }
        sb.Append("</div>\n</header>\n");

        sb.Append("<main>\n<div class=\"wrap\">\n");
        sb.Append(LimitedNotice(doc));
        sb.Append(bodyHtml);
        sb.Append("</div>\n</main>\n");

        sb.Append("<footer class=\"site\">\n<div class=\"wrap\">\n");
        sb.Append("<p><strong>Unofficial and unaffiliated.</strong> This compilation is independent. ");
        sb.Append("It is not produced, reviewed, or endorsed by the IUCN, the IUCN Red List, the Species Survival Commission, or any Red List Authority. ");
        sb.Append("It is shared in good faith to help with data review. It may be incomplete or contain errors.</p>\n");
        sb.Append($"<p>Compiled by {HtmlText.Escape(cfg.ContactName)} (<a href=\"mailto:{HtmlText.Escape(cfg.Contact)}\">{HtmlText.Escape(cfg.Contact)}</a>). ");
        sb.Append($"Source data: IUCN Red List version {HtmlText.Escape(doc.Release)}, retrieved from <a href=\"https://www.iucnredlist.org\" rel=\"noopener\" target=\"_blank\">iucnredlist.org</a>. ");
        sb.Append($"Tables and CSV downloads compiled here are released under {HtmlText.Escape(cfg.CsvLicence)}. ");
        sb.Append($"Generated {HtmlText.Escape(doc.GeneratedAt)}.</p>\n");
        sb.Append("</div>\n</footer>\n");

        sb.Append("<script src=\"assets/audit.js\"></script>\n");
        sb.Append("</body>\n</html>\n");
        return sb.ToString();
    }

    // The Theme setting. Hidden until theme.js runs (audit.css), so a browser without JavaScript
    // has no setting that does nothing. System is selected in the HTML; theme.js selects the
    // choice saved in this browser.
    public const string ThemeControl =
        "<div class=\"theme-control\">\n"
        + "<label for=\"theme-select\">Theme</label>\n"
        + "<select id=\"theme-select\" autocomplete=\"off\">\n"
        + "<option value=\"system\" selected>System</option>\n"
        + "<option value=\"light\">Light</option>\n"
        + "<option value=\"dark\">Dark</option>\n"
        + "</select>\n"
        + "</div>\n";

    // The notice at the top of every page of a --limit run, so a test build cannot be mistaken
    // for the full site. Empty for a full run. Reports whose producer ignores the limit
    // (AuditReport.IgnoresRowLimit) are named, with links, as the exceptions.
    public static string LimitedNotice(AuditDocument doc) {
        if (doc.RowLimit is not { } limit) {
            return "";
        }
        var flag = $"--limit {limit.ToString(CultureInfo.InvariantCulture)}";
        var exceptions = doc.Reports.Where(r => r.IgnoresRowLimit)
            .Select(r => $"“<a href=\"{HtmlText.Escape(r.Id)}.html\">{HtmlText.Escape(r.Title)}</a>”")
            .ToList();
        var which = exceptions.Count == 0
            ? "All reports on this site"
            : $"All reports except {HtmlText.JoinWithAnd(exceptions)}";
        return "<div class=\"limited-notice\" role=\"note\">"
            + $"<strong>Partial results from a limited run (<code>{HtmlText.Escape(flag)}</code>).</strong> "
            + $"{which} checked at most {limit.ToString("N0", CultureInfo.InvariantCulture)} database rows each, so their counts and lists may be incomplete. "
            + "For complete results, run <code>redlist audit-site</code> without <code>--limit</code>."
            + "</div>\n";
    }

    public static string Crumbs(params (string Label, string? Href)[] parts) {
        var sb = new StringBuilder();
        for (var i = 0; i < parts.Length; i++) {
            if (i > 0) {
                sb.Append(" › ");
            }
            var (label, href) = parts[i];
            sb.Append(href is null ? HtmlText.Escape(label) : $"<a href=\"{HtmlText.Escape(href)}\">{HtmlText.Escape(label)}</a>");
        }
        return sb.ToString();
    }

    // The action chip: what a reader would do about the rows. Four labels site-wide, each a plain
    // instruction, so no legend is needed. When the chip sits inside a heading, aria-hidden keeps it
    // out of the accessible heading text, so a title such as "English common name issues" is never
    // read as one run-on string with the chip label appended.
    public static string ActionBadge(ActionClass action, bool inHeading = false) {
        var (cls, text) = action switch {
            ActionClass.Mechanical => ("mechanical", "Fix by script"),
            ActionClass.ByHand => ("by-hand", "Fix by hand"),
            ActionClass.Policy => ("policy", "Decide policy"),
            _ => ("informational", "No action"),
        };
        var hidden = inHeading ? " aria-hidden=\"true\"" : "";
        return $"<span class=\"badge {cls}\"{hidden}>{HtmlText.Escape(text)}</span>";
    }
}
