local p = {}

-- Data table for colours, full names, standardized display, and wiki article links.
-- The EPBC Act (Environment Protection and Biodiversity Conservation Act 1999) lists threatened
-- species in exactly six categories (s178): Extinct, Extinct in the Wild, Critically Endangered,
-- Endangered, Vulnerable, Conservation Dependent. These deliberately mirror the IUCN 1994 (v2.3)
-- categories, so this module reuses the Module:IUCN status colour palette for visual parity across
-- conservation-status badges. (DL = "Delisted" is a display state, not a statutory category.)
local status_data = {
    ex = { long = "Extinct", bg = "black", fg = "#CC3333", display = "EX", link = "Extinction" },
    ew = { long = "Extinct in the Wild", bg = "black", fg = "white", display = "EW", link = "Extinct in the Wild" },
    cr = { long = "Critically Endangered", bg = "#CC3333", fg = "#FFCCCC", display = "CR", link = "Critically endangered species" },
    en = { long = "Endangered", bg = "#CC6633", fg = "#FFCC99", display = "EN", link = "Endangered species" },
    vu = { long = "Vulnerable", bg = "#CC9900", fg = "#FFFFCB", display = "VU", link = "Vulnerable species" },
    cd = { long = "Conservation Dependent", bg = "#006666", fg = "#99CC99", display = "CD", link = "Conservation Dependent" },
    -- Display-only state used by the taxobox machinery; carries no statutory listing.
    dl = { long = "Delisted", bg = "gray", fg = "white", is_gray = true, display = "DL", link = "" },
}

function p.main(frame)
    local args = frame:getParent().args
    local raw_input = mw.text.trim(args[1] or "")
    local code = string.lower(raw_input):gsub("%s+", "")

    local id = mw.text.trim(args[2] or "")
    local class = mw.text.trim(args[3] or args.class or "")
    local year = mw.text.trim(args.year or "")
    local label = mw.text.trim(args.label or "")

    local category = ""
    local data = status_data[code]

    -- Handle unknown codes
    if not data then
        data = { long = raw_input, bg = "#004080", fg = "white", display = raw_input, link = "" }
        if code ~= "nr" and code ~= "" then
            category = "[[Category:EPBC status templates with invalid parameters]]"
        end
    end

    -- 1. Build the status badge
    local style = string.format("background-color: %s; color: %s; padding: 0 1pt;", data.bg, data.fg)
    if data.is_gray then style = "color: gray;" end

    local title = "EPBC Act: " .. (code == "nr" and "(taxon not recognized)" or data.long)
    local display_text = (code == "nr") and ("(" .. raw_input .. ")") or data.display

    local res = string.format('<span style="%s" title="%s"><b>%s</b></span>', style, title, display_text)

    -- 2. Apply wiki link (class 2 or 12)
    if (class == "2" or class == "12") and data.link ~= "" then
        res = string.format("[[%s|%s]]", data.link, res)
    end

    -- 3. Build the external reference link to the SPRAT species profile (class 1 or 12)
    if (class == "1" or class == "12") and id ~= "" then
        local link_text = "EPBC"
        if label ~= "" then link_text = label
        elseif year ~= "" then link_text = "EPBC " .. year end

        local url = string.format("https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id=%s", id)
        res = res .. string.format('<sup> <span class="plainlinks" title="Species Profile and Threats Database (SPRAT)">[%s %s]</span></sup>', url, link_text)
    end

    return res .. category
end

return p
