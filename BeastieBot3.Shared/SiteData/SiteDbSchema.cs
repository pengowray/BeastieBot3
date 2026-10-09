namespace BeastieBot3.Shared.SiteData;

// Schema of the public site's database (Datastore:site_sqlite). `site build-db` writes it from the
// local caches; BeastieBot3.Site only reads it. The site refuses a database whose schema_version
// differs from Version, so bump Version whenever a table or column changes meaning.
//
// What the site may show from IUCN is limited by the IUCN Red List Terms of Use: no assessment
// narrative text (rationale, range, threats ...), no coded threats or habitats, no downloads. Keep
// such fields out of this database rather than hiding them in the site. The countries and areas of
// each taxon's latest global assessment (taxon_area) are here only to compare a list with the one
// area a reader chooses on the status update page; no page lists a taxon's areas.
public static class SiteDbSchema {
    public const int Version = 27;

    public const string Ddl = """
        CREATE TABLE meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE taxon (
            taxon_id                    INTEGER PRIMARY KEY,  -- IUCN SIS taxon id
            scientific_name             TEXT NOT NULL,        -- as IUCN writes it ("Panthera pardus ssp. orientalis")
            kind                        TEXT NOT NULL,        -- 'species' | 'subspecies' | 'variety' | 'subpopulation'
            kingdom                     TEXT,                 -- IUCN's spelling (upper case down to family)
            phylum                      TEXT,
            class_name                  TEXT,
            order_name                  TEXT,
            family                      TEXT,
            genus                       TEXT,
            species_epithet             TEXT,
            infra_rank                  TEXT,                 -- rank marker as written in scientific_name: 'ssp.' | 'subsp.' | 'var.'
            infra_name                  TEXT,
            subpopulation_name          TEXT,
            authority                   TEXT,                 -- HTML entities decoded
            parent_taxon_id             INTEGER,              -- the species of an infraspecific taxon or subpopulation
            common_name_en              TEXT,                 -- best English common name, capitalised; NULL when none or ambiguous
            enwiki_title                TEXT,                 -- English Wikipedia article (final title after redirects)
            wikidata_qid                TEXT,                 -- 'Q19939'
            wikidata_qid_source         TEXT,                 -- 'p627': the item states this IUCN taxon id; 'name-match': matched by name
            wikidata_p141               TEXT,                 -- wikidata_qid_source = 'p627' only: the item's IUCN conservation status (P141)
                                                              -- statements, any rank, as WikidataStatusStatement JSON
                                                              -- ([{"id":"Q140$...","value":"Q278113","rank":"normal","statedIn":["Q115962546"],
                                                              --    "taxonIds":["15951"],"references":1,"citesIucn":true}]; from its references:
                                                              -- statedIn the stated in (P248) items, taxonIds the IUCN taxon IDs (P627), references
                                                              -- how many, citesIucn whether one cites IUCN). [] when the item has none; NULL
                                                              -- when the Wikidata cache has not downloaded the item. Statements with no value
                                                              -- or an unknown value are not in the cache, so not here either
            wikidata_item_downloaded    TEXT,                 -- 'yyyy-MM-dd': when the Wikidata cache downloaded that item; NULL with wikidata_p141
            wikidata_p627_deprecated    INTEGER NOT NULL DEFAULT 0, -- 1: that item states the taxon's IUCN taxon ID (P627) only at deprecated rank
            wikidata_other_items        TEXT,                 -- wikidata_qid_source = 'p627' only: the other items that state the taxon's IUCN
                                                              -- taxon ID, as WikidataOtherTaxonItem JSON
                                                              -- ([{"qid":"Q1588648","taxonIdDeprecated":false,"p141":[...]}]; p141 NULL when
                                                              -- not downloaded); NULL when no other item states it
            col_id                      TEXT,                 -- Catalogue of Life accepted name usage id
            latest_global_assessment_id INTEGER,              -- NULL when the taxon has regional assessments only, or is not in the release
            in_release                  INTEGER NOT NULL,     -- 1: in the Red List version's CSV export. 0: only in the IUCN API cache
                                                              -- (an old or merged id, or a taxon IUCN no longer assesses); every one
                                                              -- of its assessments has is_latest = 0
            current_taxon_id            INTEGER,              -- in_release = 0 only: the taxon in the release with the same scientific
                                                              -- name (same kingdom first); NULL when there is none
            node_id                     INTEGER,              -- in_release = 1 only: the lowest higher_taxon the taxon is in (usually its genus)
            tree_pos                    INTEGER,              -- in_release = 1 only: the taxon's place in the tree, numbered depth-first; the
                                                              -- taxa under a higher_taxon have tree_pos from its first_pos to its last_pos.
                                                              -- A species comes before its subspecies, varieties and subpopulations
            list_article_title          TEXT,                 -- the article a Wikipedia list line links for the taxon (SpeciesLineFormatter), which
                                                              -- can differ from enwiki_title: a redirect with the taxon's own name is linked as it is
            list_parent_article_title   TEXT                  -- subspecies and varieties only: the article of the species, which a list line links
                                                              -- when the taxon has no article of its own
        );
        CREATE INDEX taxon_parent ON taxon(parent_taxon_id);
        CREATE INDEX taxon_current ON taxon(current_taxon_id);
        CREATE INDEX taxon_tree ON taxon(tree_pos);
        CREATE INDEX taxon_wikidata ON taxon(wikidata_qid) WHERE wikidata_qid IS NOT NULL;
        CREATE INDEX taxon_col ON taxon(col_id) WHERE col_id IS NOT NULL;

        -- The groups the taxa in the release are in: IUCN's kingdom, phylum, class, order, family and
        -- genus, and the Catalogue of Life groups between them that the placement file (`col build-placement`)
        -- keeps: a CoL group is used only when nearly all the IUCN taxon's species are in it, so a CoL
        -- group never moves a taxon out of its IUCN order or family. Node ids change with every build;
        -- the site finds a group by its rank and name.
        CREATE TABLE higher_taxon (
            node_id            INTEGER PRIMARY KEY,           -- numbered depth-first, names in alphabetical order
            parent_node_id     INTEGER,                       -- NULL for a kingdom
            depth              INTEGER NOT NULL,              -- 0 for a kingdom
            rank               TEXT NOT NULL,                 -- lower case: 'kingdom' 'phylum' 'class' 'order' 'family' 'genus', or a CoL
                                                              -- rank ('suborder', 'infraclass', 'subfamily', 'tribe', 'unranked' ...)
            name               TEXT NOT NULL,                 -- IUCN's upper-case names in title case ('Carnivora'); CoL's spelling for a CoL group
            name_key           TEXT NOT NULL,                 -- SiteNameKey.Fold(name)
            link_query         TEXT,                          -- when another group has the same rank and name: what tells them apart in the
                                                              -- group's address, 'kingdom=plantae' or 'parent=Moraceae'; NULL otherwise
            source             TEXT NOT NULL,                 -- 'iucn'; 'iucn-rule': IUCN gives "NOT ASSIGNED" and rules/iucn-not-assigned.yml
                                                              -- gives the order or family; 'col': a Catalogue of Life group
            show_rank          INTEGER NOT NULL,              -- 0: a CoL group of the same rank as the IUCN taxon above it (order Cetacea in
                                                              -- order Artiodactyla), or of no usable rank: show the name without the rank
            kingdom            TEXT NOT NULL,                 -- IUCN's kingdom, upper case
            col_id             TEXT,                          -- Catalogue of Life id of the group; NULL when not known
            common_name_en     TEXT,                          -- English name for the group ('cats'); NULL when none
            common_name_source TEXT,                          -- 'rules': rules-list.txt or taxon-rules.yml; 'wikipedia': the article the group's
                                                              -- scientific name redirects to
            enwiki_title       TEXT,                          -- English Wikipedia page or redirect with the group's name, or the name with a
                                                              -- bracketed word for its kingdom ('Ficus (plant)'); NULL when none is known
            first_pos          INTEGER NOT NULL,              -- the taxa in the group: taxon.tree_pos from first_pos to last_pos
            last_pos           INTEGER NOT NULL,
            species_count      INTEGER NOT NULL,              -- species in the release in the group
            infra_count        INTEGER NOT NULL,              -- subspecies and varieties
            subpopulation_count INTEGER NOT NULL
        );
        CREATE INDEX higher_taxon_parent ON higher_taxon(parent_node_id);
        CREATE INDEX higher_taxon_key ON higher_taxon(name_key, rank);
        CREATE INDEX higher_taxon_first ON higher_taxon(first_pos);

        -- How many taxa in a group have each category in their latest global assessment.
        CREATE TABLE higher_taxon_count (
            node_id           INTEGER NOT NULL,
            category          TEXT NOT NULL,                  -- the {{IUCN status}} code: 'CR(PE)', 'CR(PEW)', 'CR', 'LR/nt' ...
            species_count     INTEGER NOT NULL,
            infra_count       INTEGER NOT NULL,
            subpopulation_count INTEGER NOT NULL,
            PRIMARY KEY (node_id, category)
        ) WITHOUT ROWID;

        -- Other names of a group. 'col': the Catalogue of Life's English vernacular names, not checked,
        -- so a page lists them as CoL gives them. 'wikipedia': the title of the group's English
        -- Wikipedia article and of the redirects to it (English names, and other scientific names),
        -- kept only when the article is about the group (SiteGroupWikipediaNames); search finds a
        -- group by them. Neither is ever used as the group's name.
        CREATE TABLE higher_taxon_name (
            node_id  INTEGER NOT NULL,
            name     TEXT NOT NULL,
            source   TEXT NOT NULL,                           -- 'col' | 'wikipedia'
            name_key TEXT NOT NULL,                           -- SiteNameKey.Fold(name)
            PRIMARY KEY (node_id, source, name)
        ) WITHOUT ROWID;
        CREATE INDEX higher_taxon_name_key ON higher_taxon_name(name_key, source);

        -- Links from a taxon not in the release (in_release = 0) to a taxon in the release. An old id
        -- can have two links: the taxon with its name, and the one taxon whose IUCN synonyms list its
        -- name (when a taxon was split).
        CREATE TABLE taxon_link (
            taxon_id         INTEGER NOT NULL,                -- the taxon not in the release
            current_taxon_id INTEGER NOT NULL,                -- the taxon in the release
            link_kind        TEXT NOT NULL,                   -- 'same-name': the same scientific name (taxon.current_taxon_id);
                                                              -- 'iucn-synonym': the current taxon's IUCN synonyms include the old taxon's
                                                              -- scientific name, and no other taxon in the release in that kingdom lists it
            PRIMARY KEY (taxon_id, current_taxon_id)
        ) WITHOUT ROWID;
        CREATE INDEX taxon_link_current ON taxon_link(current_taxon_id);

        -- SPRAT profiles of the taxon (Australia's Species Profile and Threats Database) and their
        -- EPBC Act listings. One row per SPRAT profile: the profile whose name is the taxon's name (or
        -- lists it among its IUCN names), and every profile named after the taxon with a population in
        -- brackets: "Phascolarctos cinereus (combined populations of Qld, NSW and the ACT)".
        CREATE TABLE epbc_listing (
            taxon_id       INTEGER NOT NULL,
            sprat_taxon_id INTEGER NOT NULL,
            listed_name    TEXT NOT NULL,                     -- the name the EPBC Act lists it under, else SPRAT's scientific name
            status         TEXT,                              -- EPBC Act category: 'EX' 'EW' 'CR' 'EN' 'VU' 'CD'; NULL when not listed
            applies_to     TEXT NOT NULL,                     -- 'taxon' | 'population'
            population     TEXT,                              -- applies_to = 'population': the text in brackets after the taxon's name
            PRIMARY KEY (taxon_id, sprat_taxon_id)
        ) WITHOUT ROWID;

        -- The taxon's statuses in lists other than the IUCN Red List, one row per listing: Australia's
        -- EPBC Act and the Australian state and territory lists, as SPRAT records them; the US
        -- Endangered Species Act, from ECOS; and NatureServe's global rank with the COSEWIC and SARA
        -- statuses NatureServe records; the New Zealand Threat Classification System; and ICMBio's
        -- national assessments of Brazil's fauna (SALVE). The EPBC Act
        -- listings are in epbc_listing too, which the status update page reads.
        CREATE TABLE other_status (
            taxon_id    INTEGER NOT NULL,
            system      TEXT NOT NULL,                        -- OtherStatusSystems key: 'au-epbc', 'au-act' ... 'au-wa', 'br-salve', 'ca-cosewic',
                                                              -- 'ca-sara', 'nz-nztcs', 'us-esa', 'natureserve-global'
            status      TEXT NOT NULL,                        -- as the list writes it, with spacing, capitals and repeated values tidied:
                                                              -- 'Endangered', 'Rare', 'Vulnerable (Extinct in NT)', 'G3G4', 'G5T2'
            status_code TEXT,                                 -- natureserve-global: NatureServe's rounded rank ('G3' for G3G4, 'T2' for G5T2);
                                                              -- br-salve: the category code ('EN', 'CR(PE)')
            listed_name TEXT,                                 -- the scientific name the listing uses, when it is not the taxon's own name
            population  TEXT,                                 -- the population or area the listing applies to; NULL for the whole taxon
            source      TEXT NOT NULL,                        -- OtherStatusSources: 'sprat', 'ecos', 'natureserve', 'nztcs', 'salve'
            source_id   TEXT NOT NULL,                        -- the record's id in the source: SPRAT taxon id, ECOS Listed Species ID,
                                                              -- NatureServe element global id, NZTCS assessment id, SALVE sheet id
            url         TEXT,                                 -- the record's page at the source
            listed_on   TEXT,                                 -- yyyy-mm-dd, when the source gives it: for SALVE, the end of the assessment; for SPRAT, the date the EPBC listing
                                                              -- took effect; for ECOS, the date the taxon or population was first listed,
                                                              -- which a later change of status leaves as it is
            report      TEXT,                                 -- nz-nztcs: the report the assessment was published in,
                                                              -- 'Birds 2021 (Robertson et al. 2021)'
            country     TEXT,                                 -- ISO 3166-1 code of the country whose list or rank this is, for a system that
                                                              -- spans countries (NatureServe's national and subnational ranks); NULL when
                                                              -- the system or the list (other_status_list.country) gives the country
            list_key    TEXT,                                 -- other_status_list.list_key, for a system with several lists (the national red
                                                              -- lists from GBIF, JNCC's designations); NULL when the system is one list
            qualifier   TEXT,                                 -- what the source gives with the status: natureserve-national and -subnational:
                                                              -- 'exotic' (introduced there); cites: the Party that listed an Appendix III
                                                              -- taxon ('Nepal'); gb-jncc: a law's sections ('sections 9(4)(b) and 9(5)(a)');
                                                              -- NULL when none
            listed_under TEXT                                 -- cites: the higher taxon whose listing covers the taxon, rank and name as the site
                                                              -- shows them ('family Trochilidae'); NULL for the taxon's own listing
        );
        CREATE INDEX other_status_taxon ON other_status(taxon_id);

        -- The lists of the systems that have several (other_status.list_key): each national red list
        -- imported from GBIF, each JNCC designation. name is the list's name as its publisher gives it,
        -- and is the row label on the species page.
        CREATE TABLE other_status_list (
            list_key    TEXT PRIMARY KEY,
            system      TEXT NOT NULL,                        -- OtherStatusSystems key
            country     TEXT,                                 -- ISO 3166-1 code of the country whose list it is
            region      TEXT,                                 -- the part of the country it covers ('Flanders'); NULL for the whole country
            name        TEXT NOT NULL,                        -- 'Swedish Red List 2025', 'Wildlife and Countryside Act 1981, Schedule 5'
            title       TEXT,                                 -- a longer description for the row label's hover title; NULL when none
            sort_order  INTEGER NOT NULL,                     -- the list's place among the lists of its country
            publisher   TEXT,
            licence     TEXT,                                 -- 'CC0 1.0', 'CC BY 4.0', 'Open Government Licence v3.0'
            licence_url TEXT,
            citation    TEXT,                                 -- the citation the publisher asks for, or the dataset's
            url         TEXT,                                 -- the list's page at its publisher
            version     TEXT,                                 -- the list's year or version as the publisher gives it
            fetched     TEXT                                  -- yyyy-mm-dd, when the status lists store downloaded it
        ) WITHOUT ROWID;

        -- The taxon's IUCN Green Status of Species assessment, from the API cache's green_status table
        -- (`iucn api green-status`); the latest one when the cache has two. IUCN's justification text
        -- is left out. Scores are whole percentages as IUCN gives them (they can be negative); NULL
        -- when not given.
        CREATE TABLE green_status (
            taxon_id               INTEGER PRIMARY KEY,
            red_list_assessment_id INTEGER,            -- the Red List assessment of the page that shows it
            url                    TEXT NOT NULL,      -- that page
            assessment_date        TEXT NOT NULL,      -- yyyy-mm-dd
            published_year         INTEGER,            -- the year of the Red List release it first appeared in, when the API cache saw that;
                                                       -- NULL when it was already in the first download
            red_list_year          INTEGER,            -- the year the Red List assessment of the same page was published, when the site has it
            recovery_category      TEXT,               -- species recovery category: 'Largely Depleted'
            recovery_best          INTEGER,            -- Species Recovery Score
            recovery_min           INTEGER,
            recovery_max           INTEGER,
            legacy_category        TEXT,               -- Conservation Legacy
            legacy_best            INTEGER,
            legacy_min             INTEGER,
            legacy_max             INTEGER,
            dependence_category    TEXT,               -- Conservation Dependence
            dependence_best        INTEGER,
            dependence_min         INTEGER,
            dependence_max         INTEGER,
            gain_category          TEXT,               -- Conservation Gain
            gain_best              INTEGER,
            gain_min               INTEGER,
            gain_max               INTEGER,
            potential_category     TEXT,               -- Recovery Potential
            potential_best         INTEGER,
            potential_min          INTEGER,
            potential_max          INTEGER,
            assessors              TEXT,               -- as IUCN writes them: 'Salcedo, J., Garrote, G. & Breitenmoser, U.'
            reviewers              TEXT,
            contributors           TEXT,
            facilitators           TEXT,
            compilers              TEXT,
            citation_json          TEXT NOT NULL       -- IucnCitationParts: the assessors as authors, Year = the year assessed
        ) WITHOUT ROWID;

        -- IUCN's summary statistics tables that `iucn summary-tables` read: Table 7 (species changing Red
        -- List category, with the reason for each change; 2007 onwards) and Table 9 (Possibly Extinct and
        -- Possibly Extinct in the Wild species; 2014-1 to 2020-2).
        CREATE TABLE summary_table (
            summary_table_id INTEGER PRIMARY KEY,
            table_no         INTEGER NOT NULL,    -- 7 or 9
            release          TEXT NOT NULL,       -- the Red List version the table was published with: '2024-2'; '2007' and '2008' for those years
            url              TEXT NOT NULL,       -- where the PDF was downloaded from
            last_updated     TEXT                 -- the table's own "Last updated" date as printed: '28 October 2024'; NULL when it has none
        );

        -- The reason Table 7 gives for a change of Red List category, on the global assessment that brought
        -- the new category (SiteSummaryTables). When several tables list the change (each release's table
        -- includes the earlier releases of its year), the table latest in rules/iucn-summary-tables.yml.
        -- The Red List API and CSV export do not have the reason; IUCN's assessors record it.
        CREATE TABLE category_change (
            assessment_id          INTEGER PRIMARY KEY,
            taxon_id               INTEGER NOT NULL,
            reason                 TEXT NOT NULL,     -- 'G' genuine change, 'N' non-genuine change, 'E' the previous listing was an error
            previous_assessment_id INTEGER,           -- the global assessment before it on the site; NULL when there is none
            old_category           TEXT,              -- as the table prints it, without spaces: 'VU', 'CR(PE)', 'LR/nt'
            new_category           TEXT,
            red_list_version       TEXT,              -- the version the table says the new category was published in ('2019-3'),
                                                      -- or the table's release when the table does not say ('2008')
            summary_table_id       INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE INDEX category_change_taxon ON category_change(taxon_id);

        -- Global CR assessments that IUCN's tables list as Possibly Extinct (PE) or Possibly Extinct in the
        -- Wild (PEW): Table 9 lists each release's species, with the year of the first such assessment, and
        -- Table 7 prints 'CR(PE)' as a previous or new category. The assessment's own tags are
        -- assessment.possibly_extinct and possibly_extinct_in_the_wild; a row here may be for an assessment
        -- that has no tag.
        CREATE TABLE possibly_extinct_listing (
            assessment_id    INTEGER NOT NULL,
            tag              TEXT NOT NULL,       -- 'PE' or 'PEW'
            taxon_id         INTEGER NOT NULL,
            first_release    TEXT NOT NULL,       -- the first and last table releases that list it: '2014-1', '2016-3'
            last_release     TEXT NOT NULL,
            tables           TEXT NOT NULL,       -- the tables that list it: '7', '9' or '7 9'
            summary_table_id INTEGER NOT NULL,    -- the first table that lists it
            PRIMARY KEY (assessment_id, tag)
        ) WITHOUT ROWID;
        CREATE INDEX possibly_extinct_listing_taxon ON possibly_extinct_listing(taxon_id);

        -- The countries and parts of countries (areas) that the latest global assessments code, for
        -- the status update page's comparison of a list with one area. Hash-coded regions ("Europe")
        -- are left out.
        CREATE TABLE area (
            code     TEXT PRIMARY KEY,                       -- ISO 3166-1 alpha-2 ('BR'), or a TDWG code for part of a country ('HAW-HI')
            name     TEXT NOT NULL,                          -- IUCN's English name: 'Brazil', 'Tanzania, United Republic of', 'Hawaiian Is.'
            country  TEXT                                    -- part of a country: the code of the country it is in; NULL for a country
        ) WITHOUT ROWID;

        -- Each area a taxon's latest global assessment codes. An area coded twice (for two seasons, say)
        -- keeps the lowest origin and presence values. No page lists a taxon's areas.
        CREATE TABLE taxon_area (
            area      TEXT NOT NULL,
            taxon_id  INTEGER NOT NULL,
            origin    INTEGER NOT NULL,                      -- AreaOrigin: 1 native, 2 reintroduced, 3 introduced, 4 assisted colonisation, 5 vagrant, 6 origin uncertain
            presence  INTEGER NOT NULL,                      -- AreaPresence: 1 extant, 2 possibly extant, 3 presence uncertain, 4 possibly extinct, 5 extinct post-1500
            endemic   INTEGER NOT NULL,                      -- 1: endemic to this area. IUCN flags countries only; for part of a country: endemic to the country and recorded in no other part of it
            PRIMARY KEY (area, taxon_id)
        ) WITHOUT ROWID;

        -- The classification of the taxa in other sources, for the species page's comparison of ranks:
        -- each node above a taxon, with its parent. source 'col': id is a CoL ID (taxon.col_id);
        -- source 'wikidata': id is a QID (taxon.wikidata_qid), followed through the first parent taxon;
        -- source 'wikipedia': id 'article:<taxon.enwiki_title>' for the article's taxobox, whose parent is
        -- the name of the taxonomy template it starts from ('Felis' for Template:Taxonomy/Felis).
        CREATE TABLE ladder_node (
            source     TEXT NOT NULL,
            id         TEXT NOT NULL,
            parent_id  TEXT,
            rank       TEXT,                                 -- as the source names it: 'family', 'subtribe', 'clade'; NULL when it gives none
            name       TEXT NOT NULL,
            PRIMARY KEY (source, id)
        ) WITHOUT ROWID;

        -- The subspecies and varieties that the Catalogue of Life, Wikidata, the Mammal Diversity Database
        -- and the Reptile Database list under each IUCN species in the release, for the species page's
        -- list of subspecies and varieties, which adds IUCN's own (taxon rows whose parent_taxon_id is the
        -- species) and merges the sources' rows by name (InfraspecificNames.MergeKey). Names only: no IUCN
        -- assessment data. source 'col': accepted and provisionally accepted name usages of rank
        -- subspecies or variety whose parentID is taxon.col_id; source 'wikidata': items of
        -- `wikidata sweep-taxa`'s table with rank subspecies (Q68947) or variety (Q767728) whose parent
        -- taxon (P171) is taxon.wikidata_qid, leaving out items that are an instance of synonym, fossil
        -- taxon, unavailable combination or original combination, and items that another item names as a
        -- taxon synonym (P1420), unless each such item is named as a synonym by it in turn. Sources 'mdd'
        -- and 'reptiledb' (the checklists store, `checklists import`): the subspecies the source lists
        -- under its species of the same name as the IUCN species (or the one species its synonyms lead
        -- the name to, unless two IUCN species lead there), mammals and reptiles only; MDD's fossil
        -- subspecies left out. Only names that InfraspecificNames.Split reads are kept.
        CREATE TABLE infraspecific_name (
            taxon_id   INTEGER NOT NULL,                     -- the species (taxon.kind = 'species')
            source     TEXT NOT NULL,                        -- 'col' | 'wikidata' | 'mdd' | 'reptiledb'
            source_id  TEXT NOT NULL,                        -- the record the page links: the CoL ID ('7KGW9'); the item's QID ('Q20907143');
                                                             -- for 'mdd' and 'reptiledb' the record of the species, which lists its subspecies:
                                                             -- the MDD id ('1000002'), the Reptile Database species query ('genus=Python&species=regius')
            rank       TEXT NOT NULL,                        -- 'subspecies' | 'variety'
            name       TEXT NOT NULL,                        -- as the source writes it: 'Panthera leo melanochaita', 'Abies alba var. acutifolia'
            authority  TEXT,                                 -- the authority as the source writes it ('(C. E. H. Smith, 1858)'); NULL for Wikidata
                                                             -- (the sweep reads no authors) and when the source gives none
            PRIMARY KEY (taxon_id, source, source_id, name)
        ) WITHOUT ROWID;

        -- Species that are in the Catalogue of Life or Wikidata but are not IUCN taxa, for the group
        -- pages' lists. Species rank only. A species is here only when its genus is an IUCN genus in the
        -- same kingdom, or, when the build places by family (meta extra_species_placement = 'family'),
        -- its family is an IUCN family. Fossil species are left out (CoL extinct = true; Wikidata instance
        -- of fossil taxon, synonym, unavailable or original combination, extinct taxon). None has an IUCN
        -- assessment, so a list shows them with no {{IUCN status}}. `site build-db` writes them from the
        -- CoL database and `wikidata sweep-taxa`'s table in the Wikidata cache (SiteExtraSpeciesBuild).
        CREATE TABLE extra_species (
            extra_id         INTEGER PRIMARY KEY,         -- numbered in sort_pos order, then by name
            sources          INTEGER NOT NULL,            -- 1: in CoL only; 2: in Wikidata only; 3: in both
            scientific_name  TEXT NOT NULL,               -- "Genus epithet": CoL's name when in CoL, else Wikidata's taxon name (P225).
                                                          -- The kingdom is the kingdom of node_id
            wikidata_name    TEXT,                        -- in both sources: Wikidata's taxon name when it differs from CoL's
            col_id           TEXT,                        -- Catalogue of Life accepted name usage id
            wikidata_qid     INTEGER,                     -- the Wikidata item's number (123 for Q123)
            common_name_en   TEXT,                        -- the Wikidata item's English label when it is not a taxon name and not junk,
                                                          -- first letter capitalised; CoL's vernacular names are not checked, so never used
            enwiki_title     TEXT,                        -- the Wikidata item's English Wikipedia sitelink
            authority        TEXT,                        -- CoL's authorship of the accepted name ("(Cuvier, 1824)"), for scientific_name;
                                                          -- NULL for a species only in Wikidata (the sweep reads no authors) or when CoL has none
            node_id          INTEGER NOT NULL,            -- the higher_taxon it is placed under: its genus, or its family
            sort_pos         INTEGER NOT NULL             -- in tree order it comes after the taxon with this tree_pos (ties by name)
        );
        CREATE INDEX extra_species_node ON extra_species(node_id);
        CREATE INDEX extra_species_col ON extra_species(col_id) WHERE col_id IS NOT NULL;
        CREATE INDEX extra_species_wikidata ON extra_species(wikidata_qid) WHERE wikidata_qid IS NOT NULL;

        -- Extra species that may be the same as an IUCN taxon, or as an extra species from the other
        -- source, although no id links them. One row per pair.
        CREATE TABLE extra_overlap (
            extra_id         INTEGER NOT NULL,
            taxon_id         INTEGER,                     -- the IUCN taxon it may be the same as; NULL when other_extra_id is set
            other_extra_id   INTEGER,                     -- the extra species it may be the same as
            reason           TEXT NOT NULL,               -- 'iucn-synonym': its name is an IUCN synonym of the taxon; 'col-synonym': its name
                                                          -- (or its CoL ID on Wikidata) is a CoL synonym of the other; 'wikidata-synonym': a
                                                          -- Wikidata synonym (P1420) of the taxon; 'gender-ending': same genus, epithets that
                                                          -- differ by a Latin gender ending; 'spelling': same genus, epithets one or two letters
                                                          -- apart; 'other-genus': same epithet in another genus of the same family, with the same
                                                          -- author and year (or, for a Wikidata species, an epithet no other species there has)
            likely           INTEGER NOT NULL             -- 1 for the synonym and gender-ending reasons: a list leaves out the entry from the
                                                          -- less preferred source; 0: a list shows both, with a notice
        );
        CREATE INDEX extra_overlap_extra ON extra_overlap(extra_id);
        CREATE INDEX extra_overlap_taxon ON extra_overlap(taxon_id);

        -- The extra species under each group, for the line count of a list without reading them. Only
        -- groups with extra species under them have a row.
        CREATE TABLE higher_taxon_extra (
            node_id          INTEGER PRIMARY KEY,
            last_node_id     INTEGER NOT NULL,            -- node ids are numbered depth-first: the group's descendants are node_id to last_node_id
            col_count        INTEGER NOT NULL,            -- extra species under the group only in CoL
            wikidata_count   INTEGER NOT NULL,            -- only in Wikidata
            both_count       INTEGER NOT NULL             -- in both
        ) WITHOUT ROWID;

        -- The same counts split by where the species is placed and by whether it is likely an IUCN taxon,
        -- so the line count follows the list options. One row per group and combination that has species.
        CREATE TABLE higher_taxon_extra_count (
            node_id          INTEGER NOT NULL,
            sources          INTEGER NOT NULL,            -- as extra_species.sources: 1 CoL only, 2 Wikidata only, 3 both
            under_family     INTEGER NOT NULL,            -- 1: placed under a family because IUCN does not have its genus
                                                          -- (the list option genera=0 leaves these out); 0: under its genus
            iucn_likely      INTEGER NOT NULL,            -- 1: an extra_overlap row with likely = 1 pairs it with an IUCN taxon, so
                                                          -- a list that includes IUCN and prefers it leaves the species out
            species_count    INTEGER NOT NULL,
            PRIMARY KEY (node_id, sources, under_family, iucn_likely)
        ) WITHOUT ROWID;

        -- The name the Catalogue of Life or Wikidata gives an IUCN species in the release, when it differs
        -- from IUCN's: CoL's accepted name when the placement file matched the taxon through a CoL synonym,
        -- Wikidata's taxon name (P225) of the item linked to the taxon. A list that prefers that source uses it.
        CREATE TABLE taxon_source_name (
            taxon_id         INTEGER NOT NULL,
            source           TEXT NOT NULL,               -- 'col' | 'wikidata'
            scientific_name  TEXT NOT NULL,
            PRIMARY KEY (taxon_id, source)
        ) WITHOUT ROWID;

        CREATE TABLE assessment (
            assessment_id                INTEGER PRIMARY KEY,
            taxon_id                     INTEGER NOT NULL,
            scope                        TEXT NOT NULL,       -- 'Global', or the region as IUCN names it ('Europe'); '' when IUCN published the
                                                              -- assessment with no geographic scope (never counted as global)
            is_latest                    INTEGER NOT NULL,    -- 1 = latest assessment for its scope
            category                     TEXT NOT NULL,       -- IUCN code as published: 'LC', 'LR/nt', and pre-1994 codes such as 'V', 'Ex'
            possibly_extinct             INTEGER NOT NULL DEFAULT 0,
            possibly_extinct_in_the_wild INTEGER NOT NULL DEFAULT 0,
            criteria                     TEXT,                -- 'A2cd+4cd'
            criteria_version             TEXT,                -- '3.1', '2.3'; NULL for older assessments
            year_published               INTEGER,
            assessment_date              TEXT,                -- 'yyyy-MM-dd'
            population_trend             TEXT,                -- 'Increasing' | 'Decreasing' | 'Stable' | 'Unknown'; NULL when not given
            population_size              TEXT,                -- number of mature individuals as IUCN publishes it (supplementary_info.population_size):
                                                              -- '1000-1200', '2177', '500000-999999,800000' (range, best estimate), 'U' (unknown);
                                                              -- NULL when not given or the payload is not cached
            citation_json                TEXT,                -- IucnCitationParts as JSON; NULL when the API payload is not cached
            replaced_by_assessment_id    INTEGER,             -- the errata or amended version that replaced this assessment; NULL otherwise
            has_taxonomic_notes          INTEGER,             -- 1: the cached payload's documentation.taxonomic_notes has text; 0: empty or missing;
                                                              -- NULL when the payload is not cached. The notes themselves are narrative text and are not stored.
            wikidata_item_qid            TEXT,                -- Wikidata item for this assessment as a publication ('Q123'); NULL when none is known
            wikidata_item_properties     TEXT,                -- space-separated properties that item already has, in WikidataCitation.JudgedProperties order ('P31 P356 P2093 Len'; Len = it has an English label); NULL when no item
            wikidata_item_titles         TEXT,                -- that item's title (P1476) statements, any rank, as WikidataTitle JSON ([{"text":...,"lang":"en","rank":"normal"}]);
                                                              -- NULL when no item, or when `wikidata iucn-assessment-items` has not recorded them
            wikidata_item_label_en       TEXT,                -- that item's English label; NULL when no item or no label
            wikidata_item_assessment_id  INTEGER,             -- the assessment that item is for: assessment_id, or for an errata version that
                                                              -- shares the item of the assessment it corrects, that assessment's id
            api_not_found                INTEGER NOT NULL DEFAULT 0, -- 1: the IUCN API answered 404 (not found) when this assessment was requested,
                                                              -- although the taxon's record lists it
            credits                      TEXT                 -- the people and organisations IUCN credits (the payload's credits[]), as StoredCredits JSON
                                                              -- of credit_name ids: [{"type":"assessor","names":[12,45]},{"type":"evaluator","full":77}].
                                                              -- One group per credit type, in CreditTypes.Order; "names" the value[] entries (full names,
                                                              -- usually with an affiliation; email addresses left out), "full" the citation-form string when
                                                              -- value[] is empty, "emails" (when not 0) the value[] entries that were only an email address. NULL when the payload is not cached or has no credits
        );
        CREATE INDEX assessment_taxon ON assessment(taxon_id, year_published);
        CREATE INDEX assessment_wikidata ON assessment(wikidata_item_qid) WHERE wikidata_item_qid IS NOT NULL;

        -- The text of each distinct credit entry ("Catherine Sayer (IUCN Red List Unit)") or citation-form
        -- credit string ("Tolley, K. & Menegon, M."), for assessment.credits. Names and affiliations only.
        CREATE TABLE credit_name (
            credit_name_id INTEGER PRIMARY KEY,
            text           TEXT NOT NULL
        );

        CREATE TABLE name (
            name_id      INTEGER PRIMARY KEY,
            taxon_id     INTEGER NOT NULL,
            name         TEXT NOT NULL,
            name_type    TEXT NOT NULL,                       -- 'scientific' | 'common' | 'synonym'
            language     TEXT,                                -- ISO 639-1 code where one exists ('en', 'fr'), else the ISO 639-3 code ('yue'); IUCN's names
                                                              -- can also have IUCN's ISO 639-2 or 639-5 code ('phi'). NULL when not given (IUCN only)
            source       TEXT NOT NULL,                       -- 'iucn' | 'col' | 'wikidata' | 'wikipedia' (an article title: English Wikipedia's for 'en',
                                                              -- else the title of the Wikidata item's sitelink to the Wikipedia in that language) |
                                                              -- 'wikipedia-taxobox' (the English name in an article's taxobox) | 'mdd' | 'amphibiaweb'
            is_preferred INTEGER NOT NULL DEFAULT 0,
            authority    TEXT                                 -- synonyms only: the author and year as the source gives them ('(Phipps, 1774)');
                                                              -- NULL when the source gives none. Not part of name_key or name_fts
        );
        CREATE INDEX name_taxon ON name(taxon_id);

        -- Exact lookups by a folded key: lower case, diacritics removed, whitespace collapsed
        -- (SiteNameKey.Fold). Used for /name/{name} and to rank exact matches first in search.
        CREATE TABLE name_key (
            key      TEXT NOT NULL,
            taxon_id INTEGER NOT NULL,
            name_id  INTEGER NOT NULL,
            PRIMARY KEY (key, name_id)
        ) WITHOUT ROWID;

        -- The taxon's ids in other databases (ExternalDatabases), from the external identifiers on its
        -- Wikidata item (taxon.wikidata_qid), leaving out deprecated statements; also its Commons
        -- category (P373) and gallery (P935) and the item's Commons sitelink (property 'commonswiki').
        CREATE TABLE taxon_external_id (
            taxon_id INTEGER NOT NULL,
            property TEXT NOT NULL,                       -- the Wikidata property: 'P846' (GBIF)
            value    TEXT NOT NULL,
            PRIMARY KEY (taxon_id, property, value)
        ) WITHOUT ROWID;

        -- The IUCN status in the taxobox of the taxon's English Wikipedia article (taxon.enwiki_title),
        -- from the Wikipedia cache's copy, when the taxobox is about the taxon. status and status_system
        -- NULL: the taxobox has no IUCN status (status_system IUCN3.1 or IUCN2.3).
        CREATE TABLE enwiki_taxobox_status (
            taxon_id          INTEGER PRIMARY KEY,
            status            TEXT,                       -- as written: 'VU', 'PE', 'LR/nt'
            status_system     TEXT,                       -- 'IUCN3.1' | 'IUCN2.3'
            ref_assessment_id INTEGER,                    -- the assessment id that status_ref (or the named reference it reuses) cites
            revision_id       INTEGER,
            downloaded        TEXT NOT NULL               -- 'yyyy-MM-dd': when the cache downloaded the article
        ) WITHOUT ROWID;

        -- The words of the scientific names, synonyms and English names of taxa and of the names and
        -- English names of groups (NameWords.Find on their folded keys), for spelling suggestions when
        -- a search finds nothing. uses: how many of those keys have the word.
        CREATE TABLE name_word (
            word TEXT PRIMARY KEY,
            uses INTEGER NOT NULL
        ) WITHOUT ROWID;

        -- The names of the extra species, for search: the scientific name, Wikidata's name when it differs
        -- and the English name. Its content is extra_species, so it is rebuilt after the rows are in.
        CREATE VIRTUAL TABLE extra_name_fts USING fts5(
            scientific_name,
            wikidata_name,
            common_name_en,
            content = 'extra_species',
            content_rowid = 'extra_id',
            tokenize = 'unicode61 remove_diacritics 2',
            prefix = '2 3'
        );

        CREATE VIRTUAL TABLE name_fts USING fts5(
            name,
            content = 'name',
            content_rowid = 'name_id',
            tokenize = 'unicode61 remove_diacritics 2',
            prefix = '2 3'
        );
        """;

    /// Keys of the meta table. Every value is text.
    public static class MetaKeys {
        public const string SchemaVersion = "schema_version";
        /// UTC, ISO 8601.
        public const string BuiltAtUtc = "built_at_utc";
        /// Red List version of the CSV export the site was built from ("2026-1").
        public const string IucnRelease = "iucn_release";
        /// Range of API payload download dates ('yyyy-MM-dd').
        public const string IucnApiDownloadedFrom = "iucn_api_downloaded_from";
        public const string IucnApiDownloadedTo = "iucn_api_downloaded_to";
        /// GBIF's copy of the IUCN checklist: version text from its metadata, and its publication date.
        public const string GbifChecklistVersion = "gbif_checklist_version";
        public const string GbifChecklistPublished = "gbif_checklist_published";
        /// GBIF's recommended citation of the checklist (from its eml.xml) and the dataset DOI
        /// without a resolver prefix ("10.15468/0qnb58").
        public const string GbifChecklistCitation = "gbif_checklist_citation";
        public const string GbifChecklistDoi = "gbif_checklist_doi";
        /// Catalogue of Life release used for CoL ids ("COL26.7 XR").
        public const string ColRelease = "col_release";
        /// The Mammal Diversity Database and AmphibiaWeb versions the names come from.
        public const string MddVersion = "mdd_version";
        public const string AmphibiaWebVersion = "amphibiaweb_version";
        /// The Reptile Database's version in the checklists store ("ChecklistBank 1008 (2026-06)"), whose subspecies the site lists.
        public const string ReptileDbVersion = "reptiledb_version";
        /// The release's recommended citation and DOI from the ColDP metadata ("10.48580/dgykv").
        public const string ColCitation = "col_citation";
        public const string ColDoi = "col_doi";
        /// SPRAT report file name the EPBC statuses came from.
        public const string SpratReport = "sprat_report";
        /// When `statuses natureserve-fetch` last finished a download of NatureServe Explorer ('yyyy-MM-dd'),
        /// when the build read the status lists store.
        public const string NatureServeFetched = "natureserve_fetched";
        /// When `statuses ecos-import` last downloaded the ECOS list ('yyyy-MM-dd').
        public const string EcosFetched = "ecos_fetched";
        /// When `statuses nztcs-import` last downloaded the NZTCS assessments ('yyyy-MM-dd').
        public const string NztcsFetched = "nztcs_fetched";
        /// When `statuses salve-import` last downloaded SALVE's assessments ('yyyy-MM-dd').
        public const string SalveFetched = "salve_fetched";
        /// When `statuses cites-import` last downloaded the Checklist of CITES Species ('yyyy-MM-dd'), and
        /// the citation the Checklist asks for, with that access date.
        public const string CitesFetched = "cites_fetched";
        public const string CitesCitation = "cites_citation";
        /// JNCC's Conservation Designations for UK Taxa: when `statuses jncc-import` downloaded it
        /// ('yyyy-MM-dd'), the date of the spreadsheet ('yyyy-MM-dd', from its file name) and the
        /// attribution line JNCC asks for.
        public const string JnccFetched = "jncc_fetched";
        /// When `statuses japan-import` last downloaded Japan's Red List ('yyyy-MM-dd').
        public const string JapanFetched = "japan_fetched";
        /// When `statuses france-import` last downloaded the BDC Statuts ('yyyy-MM-dd').
        public const string FranceFetched = "france_fetched";
        public const string JnccFileDate = "jncc_file_date";
        public const string JnccAttribution = "jncc_attribution";
        /// When `iucn api green-status` last downloaded the Green Status assessments ('yyyy-MM-dd'), for
        /// the access date of their citations.
        public const string GreenStatusFetched = "green_status_fetched";
        /// The first and last Red List versions of the Table 7 and Table 9 files in summary_table
        /// ('2007', '2026-1'); absent when the build read no summary tables.
        public const string Table7FirstVersion = "table7_first_version";
        public const string Table7LastVersion = "table7_last_version";
        public const string Table9FirstVersion = "table9_first_version";
        public const string Table9LastVersion = "table9_last_version";
        /// The newest checked_at date ('yyyy-MM-dd') of any row of `iucn resolve-dois`'s doi_check
        /// table, whether the DOI was found in Crossref's list, found at doi.org or not found, when the
        /// build read that cache.
        public const string IucnDoiCheckedTo = "iucn_doi_checked_to";
        /// JSON of the Wikidata assessment item model (rules/wikidata/iucn-status.yml assessment_item)
        /// that QuickStatements batches on the site follow: WikidataItemModel.ToJson().
        public const string WikidataItemModel = "wikidata_item_model";
        /// Fingerprint of rules/iucn-not-assigned.yml (IucnNotAssignedRules.Fingerprint) used for the
        /// orders and families of the higher_taxon tree; absent when there were no rules.
        public const string NotAssignedRules = "not_assigned_rules";
        /// Whether higher_taxon has Catalogue of Life groups: 'current', 'out-of-date' (the placement
        /// file was built from another IUCN database, an older CoL file or older rules) or absent.
        public const string ColPlacementState = "col_placement_state";
        /// How extra species were placed: 'genus' or 'family' (ExtraPlacement); absent when the build added none.
        public const string ExtraSpeciesPlacement = "extra_species_placement";
        /// When `wikidata sweep-taxa` last finished a pass (UTC, ISO 8601), for the Wikidata extra species.
        public const string WikidataSweepFinished = "wikidata_sweep_finished";
        public const string TaxonCount = "taxon_count";
        public const string AssessmentCount = "assessment_count";
    }
}
