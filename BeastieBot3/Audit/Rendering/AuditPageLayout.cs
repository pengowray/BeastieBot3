using System.Text;
using BeastieBot3.Audit.Model;

// Shared page chrome for every page in the bundle: head, header with the site title, the
// unofficial disclaimer, optional breadcrumbs, the body, and a footer that repeats the
// disclaimer, attribution, licence, and generation date. Asset links are relative so the
// bundle works at any base URL (a local folder, a static host, an email attachment).

namespace BeastieBot3.Audit.Rendering;

internal static class AuditPageLayout {
    public static string Page(AuditDocument doc, string pageTitle, string? crumbsHtml, string bodyHtml) {
        var cfg = doc.Config;
        var fullTitle = pageTitle.Length == 0 ? cfg.SiteTitle : $"{pageTitle} · {cfg.SiteTitle}";
        var sb = new StringBuilder();
        sb.Append("<!doctype html>\n<html lang=\"en\">\n<head>\n");
        sb.Append("<meta charset=\"utf-8\">\n");
        sb.Append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
        sb.Append("<meta name=\"robots\" content=\"noindex\">\n");
        sb.Append($"<title>{HtmlText.Escape(fullTitle)}</title>\n");
        sb.Append("<link rel=\"stylesheet\" href=\"assets/audit.css\">\n");
        sb.Append("</head>\n<body>\n");

        sb.Append("<header class=\"site\">\n<div class=\"wrap\">\n");
        sb.Append($"<h1><a href=\"index.html\" style=\"color:inherit\">{HtmlText.Escape(cfg.SiteTitle)}</a></h1>\n");
        sb.Append($"<div class=\"release\">{HtmlText.Escape(cfg.Subtitle)} · IUCN Red List version {HtmlText.Escape(doc.Release)}</div>\n");
        if (!string.IsNullOrEmpty(crumbsHtml)) {
            sb.Append($"<nav class=\"crumbs\">{crumbsHtml}</nav>\n");
        }
        sb.Append("</div>\n</header>\n");

        sb.Append("<main>\n<div class=\"wrap\">\n");
        sb.Append(bodyHtml);
        sb.Append("</div>\n</main>\n");

        sb.Append("<footer class=\"site\">\n<div class=\"wrap\">\n");
        sb.Append("<p><strong>Unofficial and unaffiliated.</strong> This compilation is independent. ");
        sb.Append("It is not produced, reviewed, or endorsed by the IUCN, the IUCN Red List, the Species Survival Commission, or any Red List Authority. ");
        sb.Append("It is shared in good faith to help with data review, and any observation here may be incomplete or mistaken.</p>\n");
        sb.Append($"<p>Compiled by {HtmlText.Escape(cfg.ContactName)} (<a href=\"mailto:{HtmlText.Escape(cfg.Contact)}\">{HtmlText.Escape(cfg.Contact)}</a>). ");
        sb.Append($"Source data: IUCN Red List version {HtmlText.Escape(doc.Release)}, retrieved from <a href=\"https://www.iucnredlist.org\" rel=\"noopener\" target=\"_blank\">iucnredlist.org</a>. ");
        sb.Append($"Tables and CSV downloads compiled here are released under {HtmlText.Escape(cfg.CsvLicence)}. ");
        sb.Append($"Generated {HtmlText.Escape(doc.GeneratedAt)}.</p>\n");
        sb.Append("</div>\n</footer>\n");

        sb.Append("<script src=\"assets/audit.js\"></script>\n");
        sb.Append("</body>\n</html>\n");
        return sb.ToString();
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

    public static string BreakageBadge(BreakageClass breakage) => breakage switch {
        BreakageClass.Breaking => "<span class=\"badge breaking\">site or API affected</span>",
        BreakageClass.FixableData => "<span class=\"badge fixable\">value to tidy</span>",
        _ => "<span class=\"badge advisory\">to consider</span>",
    };
}
