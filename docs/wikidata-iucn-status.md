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
  (2,703 cite a 2026-1 assessment); the rest would be created, modelled per `assessment_item` in
  `rules/wikidata/iucn-status.yml`. The model needs agreement with WikiProject Taxonomy first.
- **Edit scope:** everything: changed and missing statuses, the release and assessment references on
  statuses that already agree, and P627 on items linked only by name search (after review).
- **Rank:** Wikidata has two live conventions, so the plan records both: `preferred` (new preferred
  statement, earlier ones kept normal, MatSuBot 2025) and `replace` (overwrite in place, SuccuBot
  2013-2023). `rank_variants` in the settings chooses.
- **Coordination first:** IUCN Updater Bot (Nikola Tulechki, announced Property talk:P141 2026-07-20)
  targets the same job. Contact before any bot request. IUCN terms of use vs CC0 is unresolved since
  2013 and goes in the bot request.
- **Status values:** from P141's one-of constraint (`WikidataIucnStatusValues`). LR/nt → NT, LR/lc →
  LC (Wikidata has no Lower Risk values). **LR/cd has no value** and is left unchanged (123 pairs in
  2026-1; existing Wikidata statements for those taxa mostly say LC, while the Wikipedia generator
  treats LR/cd as NT). CR(PE)/CR(PEW) are written as CR and flagged.

## Pieces

| File | Role |
|---|---|
| `WikidataIucnModel.cs` | Shared records: `IucnGlobalAssessment`, `WdTaxonItem`/`WdStatement`/`WdReference`, `TaxonItemLink`, `ExistingAssessmentItem` |
| `IucnGlobalAssessmentReader.cs`, `IucnAssessmentCitationParser.cs` | Latest global assessment per taxon from the IUCN API cache, credits split into names |
| `WdTaxonItemReader.cs`, `WdTaxonItemParser.cs`, `TaxonItemLinkReader.cs` | Cached Wikidata items (statements with raw JSON and `lastrevid`), taxon-item links by source |
| `ExistingAssessmentItemReader.cs` | Assessment items found by `wikidata iucn-assessment-items` |
| `TaxonLinkClassifier.cs` | Pure: tier A-D + flags for a pair |
| `IucnStatusEditPlanner.cs` | Pure: category + actions per rank variant |
| `WbEditPayloadBuilder.cs`, `AssessmentItemPayloadBuilder.cs` | Pure: wbeditentity JSON from actions + the cached statement copies; `CREATE:` placeholders for items not yet on Wikidata |
| `WikidataIucnPlanStore.cs` | `wikidata_iucn_plan.sqlite` beside the Wikidata cache: latest plan's pairs, run history with counts, review decisions (kept across re-plans) |
| `WikidataIucnStatusPlanCommand.cs`, `WikidataIucnPlanReport.cs` | The dry run; report `.md`, `.csv` of every pair, sample edits `.jsonl`, sample assessment items `.jsonl` in the reports folder |

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
