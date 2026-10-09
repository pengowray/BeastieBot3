# Wikidata IUCN status updates (dry run)

`wikidata iucn-status-plan` plans bringing the IUCN conservation status (P141) of Wikidata species and
subspecies items up to the current IUCN release. It reads local caches only and edits nothing on
Wikidata. The workflow page is "Update IUCN statuses on Wikidata" (`wikidata-iucn-status` in
`Web/Flows/FlowCatalogue.cs`). Code lives in `BeastieBot3/WikidataEdits/`.

## Decisions (September 2026)

- **Scope:** the latest *global* assessment per species/subspecies (varieties and subpopulations are
  skipped; regional assessments belong in P14254, not P141). 9,285 taxa with regional assessments only
  are left out.
- **References on a status:** (1) the release: stated in (P248) the release item, IUCN taxon ID (P627),
  reference URL (P854) of the assessment page (there is no assessment-id property), retrieved (P813) =
  when the assessment JSON was downloaded; (2) the assessment: stated in (P248) an item for that
  assessment publication.
- **Assessment items:** one item per assessment (chosen over inline citation snaks). ~6,600 exist
  (6,578 in the Wikidata cache on 13 September 2026; 2,703 cite a 2026-1 assessment); the rest
  would be created, modelled per `assessment_item` in `rules/wikidata/iucn-status.yml`. The model
  needs agreement with WikiProject Taxonomy first.
- **Edit scope:** everything: changed and missing statuses, the release and assessment references on
  statuses that already agree, and P627 on items linked only by name search (after review).
- **Rank:** Wikidata has two live conventions, so the plan records both: `preferred` (new preferred
  statement, earlier ones kept normal, MatSuBot 2025) and `replace` (overwrite in place, SuccuBot
  2013-2023). `rank_variants` in the settings chooses.
- **Coordination first:** IUCN Updater Bot (Nikola Tulechki, announced Property talk:P141 2026-07-20)
  targets the same job. Contact before any bot request. IUCN terms of use vs CC0 is unresolved since
  2013 and goes in the bot request.
- **Status values:** from P141's one-of constraint (`WikidataStatusValues` in `BeastieBot3.Shared`,
  which the dry run reads through `WikidataIucnStatusValues` and the public site also uses). LR/nt → NT, LR/lc →
  LC (Wikidata has no Lower Risk values). **LR/cd has no value** and is left unchanged (123 pairs in
  2026-1; existing Wikidata statements for those taxa mostly say LC, while the Wikipedia generator
  treats LR/cd as NT). CR(PE)/CR(PEW) are written as CR and flagged.

## Pieces

| File | Role |
|---|---|
| `WikidataIucnModel.cs` | Shared records: `IucnGlobalAssessment`, `WdTaxonItem`/`WdStatement`/`WdReference`, `TaxonItemLink`, `ExistingAssessmentItem` |
| `IucnGlobalAssessmentReader.cs`, `IucnAssessmentCitationParser.cs` | Latest global assessment per taxon from the IUCN API cache. The parser reads the credits and the citation; `Iucn/Citations/CreditNameSplitter.cs` splits each credit into names, and `Iucn/Citations/IucnCitationText` removes the "Accessed on" sentence from the citation and reads its DOI |
| `WdTaxonItemReader.cs`, `WdTaxonItemParser.cs`, `TaxonItemLinkReader.cs` | Cached Wikidata items (statements with raw JSON and `lastrevid`), taxon-item links by source |
| `ExistingAssessmentItemReader.cs` | Assessment items found by `wikidata iucn-assessment-items` |
| `TaxonLinkClassifier.cs` | Pure: tier A-D + flags for a pair |
| `IucnStatusEditPlanner.cs` | Pure: category + actions per rank variant |
| `WbEditPayloadBuilder.cs`, `AssessmentItemPayloadBuilder.cs` | Pure: wbeditentity JSON from actions + the cached statement copies; `CREATE:` placeholders for items not yet on Wikidata |
| `WikidataIucnEditConfig.cs` | `LoadFromRules` reads `rules/wikidata/iucn-status.yml` from the source rules folder, else the copy beside the program, and uses the defaults when neither has one; `ToItemModel()` gives the assessment item model as the shared `WikidataItemModel` |
| `WikidataIucnPlanStore.cs` | `wikidata_iucn_plan.sqlite` beside the Wikidata cache: latest plan's pairs, run history with counts, review decisions (kept across re-plans) |
| `WikidataIucnStatusPlanCommand.cs`, `WikidataIucnPlanReport.cs` | The dry run; report `.md`, `.csv` of every pair, sample edits `.jsonl`, sample assessment items `.jsonl` in the reports folder |

## Assessment item model, shared with the public site

The values for a new assessment item (class, published in, publisher, language, title language,
label and description templates) have their defaults in `WikidataItemModel`
(`BeastieBot3.Shared/Wikitext/WikidataCitation.cs`). `AssessmentItemConfig` takes its defaults
from it, and `WikidataCitationTests.ShippedYaml_MatchesTheSharedModelDefaults` checks that
`rules/wikidata/iucn-status.yml` has the same values. The dry run and `site build-db` both read
the file with `WikidataIucnEditConfig.LoadFromRules`. `site build-db` stores `ToItemModel()` in the
site database (meta key `wikidata_item_model`), and the public site's QuickStatements commands
create an assessment item to this model (`WikidataCitation.CreateItemCommands`; see "Wikidata items
of assessments" in `docs/public-site.md`). The site's commands differ from the dry run's payload
in these ways:

- language (P407) comes from the DOI's last part (`.es` Spanish, `.fr` French, `.pt` Portuguese),
  because the site also offers commands for assessments published in those languages; the dry run
  plans only latest global assessments, which are in English, and always writes the model's
  language;
- DOI (P356) is written only when the DOI names the assessment's own taxon and assessment ids, so
  an errata version's commands leave out the DOI of the assessment it corrects; the dry run writes
  the assessment's DOI whatever ids it names;
- a person author gets author last names (P9688) and author given names (P9687, the initials as
  IUCN's citation prints them) qualifiers on the site's commands only; the dry run reads its
  authors from the credits, not the parsed citation, so it has no initials. Both write an
  organisation listed in `IucnAuthorItems` as author (P50) with object named as (P1932);
- a second author with the same printed name is written as a new statement (`!P2093`), because
  QuickStatements would otherwise add that author's series ordinal to the first author's
  statement. The dry run's payload has a separate statement for each author already;
- the title (P1476), English label and description use the name from
  `WikidataCitation.TitleNameFor`: the name in the item's own title, else the name in the title
  registered with Crossref for the assessment's DOI, else IUCN's citation name. The dry run uses
  the assessment's scientific name (`IucnGlobalAssessment.ScientificName`);
- main subject (P921) is left out when two or more items state the taxon's IUCN taxon ID, or the
  taxon's item states it only at deprecated rank. The dry run always writes P921 with the taxon
  item it plans for.

The site's commands for an existing assessment item can also replace its title and English label
(`WikidataCitation.FixCommands`): a title of the form "Name: author list" becomes "Name", and the
label follows the model's `label` template. The dry run plans no change to existing assessment
items.

Editing `iucn-status.yml` changes the site's commands after the next `site build-db`.

## The public site's status commands

The public site also gives QuickStatements commands for a taxon item's IUCN conservation status
(`WikidataStatusEdit` in `BeastieBot3.Shared`; see "IUCN conservation status on Wikidata" in
`docs/public-site.md`). A reader runs them in QuickStatements with their own Wikidata account. The
site's commands are based on this plan. They differ from it where QuickStatements cannot do what
the plan does, and where the site follows its own rules:

- **Which statements count.** The dry run compares the item's best-ranked P141 statements,
  whatever their references cite (`IucnStatusEditPlanner.BestRanked`). The site compares and
  removes only statements with a reference that cites IUCN: one with an IUCN taxon ID (P627), one
  stated in (P248) the IUCN Red List, IUCN, an edition of the Red List or the item of an IUCN
  assessment, or one with a reference URL (P854) on iucnredlist.org or a subdomain. The site still
  takes the item's best rank over every statement that is not deprecated, whatever it cites, and
  never removes a normal-rank statement when the item has a preferred one.
- **Rank.** QuickStatements v1 cannot set a statement's rank or change its value. The site's
  Replace adds the IUCN value (or a reference to the statement that already has it) and removes,
  by statement id, the IUCN statements at the item's best rank that have another value. The dry run's `replace` variant overwrites the value of the
  first best-ranked statement. The site's Keep adds the IUCN value in the same way, removes
  nothing, and lists the ranks that the reader sets by hand. The dry run's `preferred` variant sets the ranks in its own edit. The site
  shows Keep first when the item has a statement at preferred rank, and Replace first otherwise.
  In the dry run, `rank_variants` chooses which variants are planned.
- **References.** The site adds one reference: stated in (P248) the assessment's item when one
  exists, IUCN taxon ID (P627), reference URL (P854) of the assessment page, and retrieved (P813).
  It never cites a release item: when `WikidataStatusEdit` was written, Wikidata had no item
  for release 2026-1. It adds no
  reference to a statement that already has one with the taxon's IUCN taxon ID or stated in the
  assessment's item. The site's status commands never create an assessment item. The site offers
  the commands that create one in its `{{cite Q}}` part.
- **Which taxa.** The site gives commands only when the taxon's item states the taxon's IUCN taxon
  ID at a rank that is not deprecated, and no other item states that id at any rank. So the site gives no
  commands for three kinds of link that the dry run puts in tiers C and D: an item matched by
  name, an id stated on several items, and an id stated only at deprecated rank. The site never
  adds or deprecates an IUCN taxon ID.
- **Statements with the IUCN value.** The site gives no commands when a deprecated statement, or
  two or more statements, have the IUCN value, because QuickStatements finds the statement that a
  reference goes on by its value.

Both use the same P141 values (`WikidataStatusValues`).

## Tiers

| Tier | Meaning | Would be edited |
|---|---|---|
| A | IUCN id already on the item, unambiguous, name and rank agree | yes |
| B | IUCN id on the item, name differs or rank missing | yes, flagged |
| C | exact name search (no P627 on the item), or the id is on several items | after review |
| D | synonym/label/local-name link, subspecies on species-ranked item, conflicting current id, deprecated id, item linked to several taxa | after review |

A renumbered id (the item's P627 is not in the release) is only a flag; the plan deprecates it with
reason withdrawn identifier value (Q21441764) and adds the new one.

## Safety properties

- Plans are actions, not payloads: payloads are rebuilt from the cached item and carry its `lastrevid`
  as `baserevid`, so Wikidata refuses an edit to an item that changed since.
- A changed statement is sent whole (wbeditentity replaces by id); the builder works on a copy of the
  cached statement JSON, never the cache itself.
- Items older than 30 days at plan time are counted (`StaleItems`); refresh them with
  `wikidata cache-entities --refresh-only --max-age-hours 720` before any real run.

## Not built yet

- Review UI for tier C/D (decisions table exists: `review_decisions`).
- Applying edits (needs the release item, an agreed assessment item model, and bot approval).
