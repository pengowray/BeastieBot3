# Wikipedia Cache Workflow

## Goals
- Cache the main article HTML and wikitext for English Wikipedia titles relevant to IUCN taxa.
- Track redirects, missing pages, disambiguation/set-index classifications, and taxobox metadata.
- Record how each taxon resolved (or failed) to an article, including the synonym used, redirect chain, and validation notes.

## Candidate Discovery
1. **Primary source:** Enwiki sitelinks stored in the Wikidata cache (`enwiki` site links per taxon entity).
2. **Fallbacks:**
   - Taxon scientific name.
   - Accepted name synonyms from Catalogue of Life.
   - Accepted/synonym names from IUCN datasets.
3. Each candidate title is normalized (decode URL, replace underscores with spaces, collapse whitespace, convert to normalized case for DB key) before storage.
4. Every attempt is logged into `taxon_wiki_match_attempts` for auditability and reporting.

## Fetch Process
1. Resolve or create `wiki_pages` row via `WikipediaCacheStore.UpsertPageCandidate`.
2. Request mobile HTML (article-only) via REST: `GET https://en.wikipedia.org/api/rest_v1/page/mobile-html/{title}`.
   - Contains only the main article content; no site chrome.
   - Save HTML plus SHA-256 hash, byte size, and timestamp.
3. Request metadata via Action API `action=parse` or `action=query` with `prop=categories|templates|revisions`:
   - Determine redirect target (via `redirects`/`parse.redirects` data).
   - Capture categories to detect disambiguation/set-index pages.
   - Fetch wikitext (`rvslots=main&rvprop=content`).
4. Parse redirect info and recursively follow (bounded) until landing page is stable; persist `wiki_redirect_edges` for traceability.
5. Strip categories to boolean flags (redirect/disambiguation/set-index) and persist.
6. Parse taxobox template tree (via wikitext or Parsoid JSON) to extract `scientific_name`, rank, kingdom, etc., stored in `wiki_taxobox_data`.

## Matching Logic
1. After page fetch, evaluation service validates the article:
   - If redirect leads to genus article, check monotypic rules (per COL data) before accepting.
   - Compare kingdom (and optionally class) from taxobox to the expected taxon.
   - Ensure taxobox exists; otherwise mark as needs-review.
2. On success, update `taxon_wiki_matches` row with selected `page_row_id`, synonyms used, redirect final title, and notes.
3. On failure (disambiguation, wrong kingdom, missing page), mark status accordingly so reports can highlight unresolved taxa.

## Commands
1. `wikipedia cache-status` — already implemented.
2. `wikipedia enqueue-wikidata` — seed `wiki_pages` from Wikidata sitelinks (current step).
3. `wikipedia fetch-pages` — download pending pages (batch-friendly, retry support).
4. `wikipedia match-taxa` — iterate taxa, attempt to link them to cached pages, log attempts, fill match tables.

## Rate Limits & HTTP
- Dedicated `WikipediaApiClient` with REST + Action API HttpClients, enforcing configurable minimum delay and descriptive User-Agent.
- Retries on 429/5xx with exponential backoff.

## Outstanding Work
- Implement `WikipediaApiClient` + fetch command to populate cache.
- Build taxon-to-page evaluator using cache plus COL/IUCN context.
- Reporting commands for missing pages, failed matches, pending fetch queue.
