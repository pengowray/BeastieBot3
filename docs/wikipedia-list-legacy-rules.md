# Legacy taxonomy rules vs current YAML

This note summarizes the legacy list-generation rules (circa 2016) and how they map to the current YAML-driven pipeline.

## Legacy rule grammar (TaxaRuleList)
Source: BeastieLegacy/Iucn/TaxaRuleList.cs

Legacy rules were plain-text with ad-hoc tokens:

- `X = Y` → common name override for taxon `X`
- `X plural Y` → plural common name override
- `X adj Y` → adjective override
- `X wikilink Y` → heading/link override (disambiguation)
- `X includes Y` → display "Includes …" blurb
- `X comprises Y` → display "Comprises …" blurb (often grey)
- `X force-split true` → force split into lower ranks even when few items exist
- `X split-off Y` → split a sub-taxon off into its own group (legacy only)
- `X below Y:rank` → insert a new category `Y` below `X` at rank `rank` (legacy only)
- `X means Y` → explanatory text (legacy only)
- `X typo-of Y` → treat `X` as typo of `Y`, alter link/display and keep audit trail

## Current YAML rules (TaxonRulesService)
Source: rules/taxon-rules.yml + WikipediaLists/TaxonRulesDefinition.cs

Current rules are structured YAML with explicit fields:

- `common_name`, `common_plural`, `adjective`
- `wikilink`, `main_article`
- `blurb`, `comprises`
- `force_split`, `exclude`
- `use_virtual_groups` + `virtual_groups` definitions
- `list_overrides` for per-list variants

## Key differences

- **Explicit YAML vs ad-hoc text**: Current rules are typed and safer to extend.
- **Force-split retained**: Still supported via `force_split`.
- **Below/split-off/means/typo-of**: Not currently implemented in YAML. If needed, add structured equivalents (likely via list-specific overrides or a dedicated “rules-migrations” pass).
- **Virtual groups**: New YAML feature replaces some legacy “split-off” usage (e.g., paraphyletic groupings).
- **Other-bucket merging**: Now controlled by grouping thresholds (`min_items`, `min_groups_for_other`) rather than hard-coded legacy heuristics.

## Migration notes

If a legacy rule is discovered in rules-list.txt (or related artifacts) that has no YAML equivalent, document the intent and add a new YAML field before porting. Avoid reintroducing ad-hoc parsing unless the rule can’t be expressed structurally.
