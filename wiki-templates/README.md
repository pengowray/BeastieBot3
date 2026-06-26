# On-wiki templates

Wikipedia (Scribunto/Lua) templates authored by this project, for deployment to **en.wikipedia.org**.
These are *not* used by the C# build — they are emitted by the SPRAT Australia lists (`sprat
generate-lists`) and must exist on-wiki for those lists to render.

## `{{EPBC status}}` — the EPBC analogue of `{{IUCN status}}`

An inline coloured conservation-status badge for Australia's *Environment Protection and Biodiversity
Conservation Act 1999* (EPBC Act), with an optional reference link to the species' [SPRAT][sprat]
profile. Neither this template nor a `Module:EPBC status` exists on en.wikipedia yet (verified June
2026); deploy these three pages:

| Repo file | Wikipedia page |
| --- | --- |
| `Module-EPBC_status.lua` | `Module:EPBC status` |
| `Template-EPBC_status.wikitext` | `Template:EPBC status` |
| `Template-EPBC_status.doc.wikitext` | `Template:EPBC status/doc` |

Invocation: `{{EPBC status|<EX|EW|CR|EN|VU|CD>|<sprat_taxon_id>|<option>|year=|label=}}` — option `1`/`12`
appends the SPRAT external link, `2`/`12` links the category article. The SPRAT list generator emits the
`|<sprat_taxon_id>|1` form (e.g. `{{EPBC status|VU|86568|1}}`).

It is forked from **`Module:IUCN status`** (the Lua data-table model), reusing the IUCN colour palette
since EPBC categories deliberately mirror the IUCN ones (there is no official EPBC colour scheme). The
existing `{{Species conservation status|EPBC|…}}` ([Module:Conservation status]) covers only the taxobox
greyscale-icon case — there was no inline text-badge for EPBC, which this fills.

The original `{{IUCN status}}` reference (template, module, doc) is kept for comparison under
`D:\datasets\wiki-templates\`.

[sprat]: https://www.environment.gov.au/cgi-bin/sprat/public/publicspecies.pl?taxon_id=86568
[Module:Conservation status]: https://en.wikipedia.org/wiki/Module:Conservation_status
