# IUCN wikitext on other-language Wikipedias

Research for generating the equivalents of English `{{cite iucn}}`, `{{IUCN status}}` and the taxobox status lines on other Wikipedias. Fetched 9 October 2026 with read-only API GETs. Every template name, parameter and value below comes from a fetched template source, documentation page or live article revision; the URL is given beside each claim. Article revisions are cited by `oldid` so the quoted wikitext can be checked even after the article changes.

Method:

- Template equivalents from Wikidata sitelinks: Template:Cite IUCN is [Q26058376](https://www.wikidata.org/wiki/Q26058376), Template:IUCN status [Q18198876](https://www.wikidata.org/wiki/Q18198876), Template:Speciesbox [Q14449650](https://www.wikidata.org/wiki/Q14449650), Template:Taxobox [Q52496](https://www.wikidata.org/wiki/Q52496), Template:Taxobox/species [Q12001912](https://www.wikidata.org/wiki/Q12001912), Module:Conservation status [Q78476830](https://www.wikidata.org/wiki/Q78476830), Template:Cite web [Q5637226](https://www.wikidata.org/wiki/Q5637226). Many wikis have their own IUCN template that is not linked to the English one; those were found in the live articles and by listing the Template namespace by prefix.
- Live articles: polar bear (Q33609), lion (Q140), Wollemia nobilis (Q190510), Welwitschia mirabilis (Q156926) and baiji (Q188130, CR flagged possibly extinct), through their sitelinks. (Q157983 is Amorpha fruticosa, not Welwitschia: [wbgetentities](https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q157983&props=sitelinks&format=json).)
- Usage counts are `hastemplate:` search hits in the article namespace (`list=search&srinfo=totalhits`), which count pages that transclude the template directly or through another template.

---

## de: German Wikipedia (3,156,948 articles)

**Recommendation: support the citation only.** The German taxobox has no conservation status at all, so there are no taxobox lines to generate. German articles cite the Red List with `{{IUCN}}` (16,419 articles), which takes the taxon id, the assessment id, the assessors and the year.

### Taxobox

`Vorlage:Taxobox` (62,446 articles) has no conservation status parameter. Its full parameter list, read from the template source ([Vorlage:Taxobox](https://de.wikipedia.org/wiki/Vorlage:Taxobox)), is `Bild`, `Bildbeschreibung`, `ErdzeitalterBis/Von`, `Fundorte`, `MioBis/Von`, `Modus`, `Rangunterdrückung`, `Status_Kommentar`, `Subtaxa`, `Subtaxa_Plural`, `Subtaxa_Rang`, `TausendBis/Von`, `Taxon_Name/WissName/Rang/Autor`, `Taxon2..6_*` and `Taxon_Status`. `Taxon_Status` and `Status_Kommentar` are about taxonomic validity ("ungültig", "redundant", "obsolet"), not conservation: [Wikipedia:Taxoboxen](https://de.wikipedia.org/wiki/Wikipedia:Taxoboxen) (section on invalid taxa). The live articles agree: the taxoboxes of Eisbär ([oldid 270284317](https://de.wikipedia.org/w/index.php?oldid=270284317)) and Wollemie ([oldid 264192126](https://de.wikipedia.org/w/index.php?oldid=264192126)) have no status line, and the status is given in the text ("Die IUCN führte im Jahr 2015 den Eisbär im Status gefährdet (''vulnerable'')").

### Citation: `{{IUCN}}`

Doc: [Vorlage:IUCN/Doku](https://de.wikipedia.org/wiki/Vorlage:IUCN/Doku); source: [Vorlage:IUCN](https://de.wikipedia.org/wiki/Vorlage:IUCN), [Vorlage:IUCN/Weblink](https://de.wikipedia.org/wiki/Vorlage:IUCN/Weblink).

| Parameter | Meaning (from the doc) |
| --- | --- |
| `ID` | IUCN taxon id (the first number in `https://www.iucnredlist.org/species/12392/3339343`). Shown as missing in red if empty. |
| `ScientificName` | Required. The scientific name as IUCN shows it; the template italicises it itself, including for subspecies ("Diceros bicornis ssp. longipes") and subpopulations ("Orcaella brevirostris Songkhla Lake subpopulation"). Link text. |
| `AssessmentID` | Assessment id (the second number). With it the link is `https://www.iucnredlist.org/species/<ID>/<AssessmentID>`; without it the link is an IUCN search for the scientific name (Vorlage:IUCN/Weblink). The doc says to give it together with `Assessor` and `YearAssessed`. |
| `Year` | Year or version of the Red List ("2011.1" for numbered releases). |
| `Assessor` | Assessor(s); the doc allows the first person plus "u. a.". |
| `YearAssessed` | Year of "Date assessed"; shown only when `Assessor` is set. |
| `Abruf` | Access date. The doc asks for `YYYY-MM-DD`; the long form ("24. Februar 2009") still works. The source also accepts `Download` as an alias. |
| `Linktext`, `PureURL`, `ID2..ID9` etc. | Link text override, bare URL, several species in one call. |

There is no DOI, volume or page/article-number parameter. Output (from the source): "[link *Name*] in der Roten Liste gefährdeter Arten der IUCN <Year>. Eingestellt von: <Assessor>, <YearAssessed>. Abgerufen am <date>."

Doc example of a full citation:

```
{{IUCN
 | Year           = 2008
 | ID             = 11506
 | ScientificName = Leontopithecus rosalia
 | AssessmentID   = 3287321
 | YearAssessed   = 2008
 | Assessor       = M. C. M. Kierulff, A. B. Rylands, M. M. de Oliveira
 | Abruf          = 2018-11-13
}}
```

Live, Löwe ([oldid 269766074](https://de.wikipedia.org/w/index.php?oldid=269766074)): `<ref name="IUCN2024">{{IUCN|Year=2024|ID=15951|ScientificName=Panthera leo|YearAssessed=2023|Assessor=S. Nicholson et al.|Download=20. September 2024}}</ref>`. Authors are written "Initials Surname" in the doc example; the live article uses "et al."

### Inline status template

None. The only IUCN templates in the Template namespace are `Vorlage:IUCN` and `Vorlage:IUCNSearch` ([allpages prefix IUCN](https://de.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10&apfilterredir=nonredirects)).

### Notes for a generator

- `Year` is the Red List edition, `YearAssessed` the assessment year: the lion citation above has `Year=2024|YearAssessed=2023`.
- Without `AssessmentID` the link goes to a search page, so always pass it.

---

## fr: French Wikipedia (2,783,663 articles)

**Recommendation: support.** The French taxobox has its own status line, `{{Taxobox UICN | VU | A3c }}` (35,076 articles), which takes the category and the criteria. The citation template `{{UICN}}` (33,360 articles) takes only the taxon id, a description and the access date, so it is simple to generate but carries no assessment details.

### Taxobox: `{{Taxobox UICN}}`

French taxoboxes are a stack of one-line templates between `{{Taxobox début}}` and `{{Taxobox fin}}`; the status is its own line, `{{Taxobox UICN}}`. The form `{{Taxobox UICN | VU | A3c }}` does exist: Ours blanc ([oldid 240165899](https://fr.wikipedia.org/w/index.php?oldid=240165899)) has `{{Taxobox UICN | VU| A3c }}` after `{{Taxobox taxon | animal | espèce | Ursus maritimus | ... }}` and before `{{Taxobox CITES | II | 11/06/1992 }}`; Lion ([oldid 240110220](https://fr.wikipedia.org/w/index.php?oldid=240110220)) has `{{Taxobox UICN | VU | A2c }}`; Wollemia nobilis ([oldid 238302776](https://fr.wikipedia.org/w/index.php?oldid=238302776)) has `{{Taxobox UICN | CR | B1ab(iii)+2ab(iii) }}`.

Doc: [Modèle:Taxobox UICN/Documentation](https://fr.wikipedia.org/wiki/Mod%C3%A8le:Taxobox_UICN/Documentation); source: [Modèle:Taxobox UICN](https://fr.wikipedia.org/wiki/Mod%C3%A8le:Taxobox_UICN).

- Syntax: `{{taxobox UICN | risque | critère | précision }}`, all positional.
- `1` (risque), required. Accepted values listed by the doc and its TemplateData: `EX`, `PEW`, `PE`, `NE`, `DD`, `EW`, `CR`, `EN`, `VU`, `CD`, `NT`, `LC`, in upper case: the source builds the label from `{{UICN {{{1}}}}}` and the image name from the code, so `vu` would call a `Modèle:UICN vu` that does not exist. So CR(PE) is `PE` and CR(PEW) is `PEW`, as on English Wikipedia. The 1994 categories: the doc gives `CD` as the equivalent of LR/cd, `NT` of LR/nt and `LC` of LR/lc. The source builds the image name `Status iucn{{#ifeq:{{{1}}}|CD|2.3|3.1}} <code>-fr.svg`, so only `CD` gets the 2.3 image. Each code also needs a `Modèle:UICN <code>` label template; these exist for the 12 codes above plus `NA`, `RE` ([allpages prefix UICN](https://fr.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=UICN&apnamespace=10&apfilterredir=nonredirects)).
- `2` (critère), optional: the criteria string as IUCN writes it ("A1cd+2cd").
- `3` (précision), optional: a note such as a subspecies name, for a second `{{Taxobox UICN}}` line in the same taxobox.
- There is no status-system parameter and no reference parameter. The doc says to also put `{{UICN}}` in the article ("N'oubliez pas d'ajouter un {{m|UICN}} à la page").
- The template adds `Catégorie:Statut UICN <French category name>`.

### Citation: `{{UICN}}`

Doc: [Modèle:UICN/Documentation](https://fr.wikipedia.org/wiki/Mod%C3%A8le:UICN/Documentation); source: [Modèle:UICN](https://fr.wikipedia.org/wiki/Mod%C3%A8le:UICN).

| Parameter | Meaning |
| --- | --- |
| `1` | Required. IUCN taxon id ("dans l'adresse https://www.iucnredlist.org/species/137/220501878, le code est 137"). |
| `2` | Optional description: the italic binomial, with or without the authority (`''Canis lupus'' Linnaeus, 1758`). Without it the link text is "num <id>". |
| `consulté le` | Access date. The TemplateData autovalue is `{{subst:CURRENTDAY}} {{subst:CURRENTMONTHNAME}} {{subst:CURRENTYEAR}}`, i.e. "15 novembre 2025" with the French month name; its example is "23/11/2021". |
| `rang` | Rank word in the link text; default "espèce", suggested value "sous-espèce". |
| `ancre` | Anchor id (default `UICN`), for several `{{UICN}}` in one article. |

The source links `https://www.iucnredlist.org/details/<id>/0`, so the link always goes to the current assessment. There are no author, year, assessment id or DOI parameters. For a genus or family the doc points to `Modèle:UICN liste`.

Live, Ours blanc ([oldid 240165899](https://fr.wikipedia.org/w/index.php?oldid=240165899)): `<ref name="UICN">{{UICN|22823|''Ursus maritimus'' Phipps, 1774|consulté le=9 juillet 2018}}</ref>`, and again in the "Liens externes" list as `* {{UICN | 22823 | ''Ursus maritimus'' Phipps, 1774 | consulté le=15 septembre 2026 }}`. Lion ([oldid 240110220](https://fr.wikipedia.org/w/index.php?oldid=240110220)): `<ref>{{UICN|15951|''Panthera leo'' (Linnaeus, 1758)}}</ref>`.

`{{Bioref|UICN|2026-09-15|ref}}` also appears in Ours blanc, but only after the vernacular names, as the source of the names; it is not the status citation.

### Inline status template

None like `{{IUCN status}}`. `{{UICN VU}}` and the other `Modèle:UICN <code>` templates only print the French category name (`[[Espèce vulnérable|Vulnérable]]`, or unlinked with `court`): [Modèle:UICN VU](https://fr.wikipedia.org/wiki/Mod%C3%A8le:UICN_VU).

### Notes for a generator

- Output for the taxobox is one line: `{{Taxobox UICN | <code> | <criteria> }}`, with `PE`/`PEW` for the possibly extinct flags and `CD`/`NT`/`LC` for LR/cd, LR/nt, LR/lc.
- The criteria string is the second positional parameter, which the English taxobox has no place for.

---

## es: Spanish Wikipedia (2,141,997 articles)

**Recommendation: support.** `{{Ficha de taxón}}` (242,926 articles) takes `status`, `status_system` and `status_ref` with the English codes, PE and PEW included, and its doc says to fill `status_ref` with `<ref>{{IUCN|...}}</ref>`. `{{IUCN}}` (21,768 articles) has no id parameter: it links the taxon id that the article's own Wikidata item has in P627.

### Taxobox: `{{Ficha de taxón}}`

Doc: [Plantilla:Ficha de taxón/doc](https://es.wikipedia.org/wiki/Plantilla:Ficha_de_tax%C3%B3n/doc), section "Estado de conservación"; source of the status cell: [Plantilla:Ficha de taxón/estado de conservación](https://es.wikipedia.org/wiki/Plantilla:Ficha_de_tax%C3%B3n/estado_de_conservaci%C3%B3n) (the es page linked to English Template:Taxobox/species on Wikidata).

- `status`: the code. The doc says it may be upper or lower case, upper case preferred. The source's `#switch` accepts (each in upper and lower case): `EX`, `EW`, `CR`, `EN`, `VU`, `NT`, `LC`, `DD`, `NE`, `NR`, `PE`, `PEW`, `LR/CD` (also `LR/cd`, `lr/cd`, `LRCD`, `LRcd`, `lrcd`), `LR/NT` (and the same variants), `LR/LC` (and the same variants), `LR`, plus non-IUCN values `secure`, `DOM`, `fossil`, `pre`, `text`. Anything else adds "Categoría:Wikipedia:Taxones con estado de conservación no válido". `RE` and `NA` are not accepted. The doc's code table does not list `PE`/`PEW`, but the source shows them as "En peligro crítico, posiblemente extinto (PE)" and "... posiblemente extinto en estado salvaje (PEW)" with the `Status none PE.svg` / `PEW.svg` images.
- `status_system`: the doc gives `IUCN2.3` or `IUCN3.1`, default IUCN3.1, except that LR/LC, LR/NT and LR/CD are IUCN2.3. The source matches `uicn2.3`, `UICN2.3`, `iucn2.3`, `IUCN2.3`, `uicn3.1`, `UICN3.1`, `iucn3.1`, `IUCN3.1`.
- `status_ref`: the doc says `| status_ref = <ref>{{IUCN | ...}}</ref>`.
- `status_text`: for a non-IUCN system.
- The box does not read P141 from Wikidata: the source has no `#property` or `#invoke` call, and Wollemia nobilis ([oldid 174625925](https://es.wikipedia.org/w/index.php?oldid=174625925)) shows only what `status = CR` gives.

Live: Panthera leo ([oldid 175619158](https://es.wikipedia.org/w/index.php?oldid=175619158)) `| status = VU`, `| status_system = iucn3.1`; Lipotes vexillifer ([oldid 175041665](https://es.wikipedia.org/w/index.php?oldid=175041665)) `| status = PE`, `| status_system = iucn3.1`; Ursus maritimus ([oldid 174047306](https://es.wikipedia.org/w/index.php?oldid=174047306)) `| status = vu`, `| status_system = uicn3.1`.

### Citation: `{{IUCN}}`

Doc: [Plantilla:IUCN/doc](https://es.wikipedia.org/wiki/Plantilla:IUCN/doc); source: [Plantilla:IUCN](https://es.wikipedia.org/wiki/Plantilla:IUCN). The template is a wrapper around `{{cita web}}`.

| Parameter (aliases from the source) | Meaning |
| --- | --- |
| `título` (`title`) | Scientific name of the assessed species, unformatted (the template italicises it). Only needed when it differs from the page name; the default is the page title without its bracketed qualifier. |
| `asesores` (`autor`, `assessors`, `assessor`) | The assessors, e.g. "Ntakimazi, G." or "BirdLife International". One free-text field. |
| `año` (`year`) | Year of the assessment; becomes `fecha` of `{{cita web}}`. |
| `edición` (`edición IUCN`, `versión`, `version`) | Edition of the Red List; the default is the current year. Shown after "Lista Roja de especies amenazadas de la UICN". |
| `consultado` (`downloaded`, `accessdate`) | Access date; the doc gives the form "dd de mmmm de aaaa" (e.g. "9 de octubre de 2026"). Becomes `fechaacceso`. |

There is no taxon id, assessment id, page/article number or DOI parameter. The URL is `https://www.iucnredlist.org/details/{{#property:P627}}/0`: the IUCN taxon id (P627) of the Wikidata item connected to the article, and no URL at all when that item has none. The template also sets `idioma = inglés` and `ISSN = 2307-8235`.

Live, Panthera leo ([oldid 175619158](https://es.wikipedia.org/w/index.php?oldid=175619158)): `<ref>{{IUCN|título=Panthera leo|asesores=Nicholson, S., Bauer, H., Strampelli, P., Sogbohossou, E., Ikanda, D., Tumenta, P.F., Venktraman, M., Chapron, G. & Loveridge, A.|año=2023|edición=2023-1|consultado=2024-04-05}}</ref>`. Ursus maritimus uses `edición IUCN=2015.4|consultado=24 de enero de 2016`.

### Inline status template

None found: the only Template-namespace page starting "IUCN" is `Plantilla:IUCN` itself ([allpages prefix IUCN](https://es.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10&apfilterredir=nonredirects)).

### Notes for a generator

- Because the link comes from the article's Wikidata item, a `{{IUCN}}` for a subspecies or subpopulation cited in a species article links the species. A citation of another taxon needs `{{cita web}}` (or `{{cita publicación}}`) with an explicit URL instead.
- Authors go in one field, in IUCN's own form ("Wiig, Ø., Amstrup, S. & Thiemann, G.").

---

## it: Italian Wikipedia (1,990,050 articles)

**Recommendation: support, with care over the citation.** `{{Tassobox}}` (49,762 articles) has `statocons`, `statocons_versione` and `statocons_ref`. The citation template `{{IUCN}}` (13,004 articles) always prints "2020" and "Versione 2020.2" whatever is cited, and has no author-year, DOI or article-number fields, so it cannot give an accurate citation of a current assessment.

### Taxobox: `{{Tassobox}}`

Doc: [Template:Tassobox/man](https://it.wikipedia.org/wiki/Template:Tassobox/man) (section on stato di conservazione); source: [Modulo:Tassobox](https://it.wikipedia.org/wiki/Modulo:Tassobox) (`stato_conservazione`) and [Modulo:Tassobox/Configurazione](https://it.wikipedia.org/wiki/Modulo:Tassobox/Configurazione) (`config.stato`, `config.stato_alias`).

- `statocons`: the module lower-cases the value, maps aliases, then looks it up in `config.stato`. Codes: `ex`, `ew`, `cr`, `en`, `vu`, `nt`, `lc`, `dd`, `ne`, `pe`, `pew`, `cd`, `lr/nt`, `lr/lc`, `lr`, plus `fossile` and `sconosciuto`. `LR/CD` is an alias of `cd`. Italian words also work (`in pericolo`, `vulnerabile`, `critico`, `probabilmente estinto`, ...). Any other value prints "Parametro statocons non valido" and adds "Categoria:Errori di compilazione del template Tassobox". `PE` shows "Critico - Probabilmente estinto", `PEW` "Critico - Probabilmente estinto in natura". There is no `re` or `na`.
- `statocons_versione`: `iucn2.3` or `iucn3.1`, default iucn3.1 (doc). The module compares it case-sensitively with `iucn2.3`/`iucn3.1`, so it must be lower case; anything else falls back to iucn3.1. It changes only the image of `vu`, `en`, `cr`, `ex`, `ew`, `lr/nt` and `lr/lc`; `nt`, `lc`, `pe`, `pew` always use the 3.1 image and `cd` the 2.3 image.
- `statocons_ref`: the reference. The doc says `<ref>{{IUCN|summ=|autore=|accesso=}}</ref>`.
- `dataestinzione` (extinction date) is shown after `ex`, `pe` and `pew` (the rows with a `data` field in the configuration; the manual also lists it for EW, but the `ew` row has none).
- No Wikidata status: Template:Tassobox calls `{{#Invoke:Tassobox|stato_conservazione}}` with the article's parameters only.
- The `IUCN` doc says only the global assessment may be used in the Tassobox ("Per la compilazione del Tassobox deve essere usato esclusivamente l'assessment global").

Live: Panthera leo ([oldid 152798486](https://it.wikipedia.org/w/index.php?oldid=152798486)) `|statocons = VU`, `|statocons_versione = iucn3.1`, `|statocons_ref = <ref name=IUCN/>`; Wollemia nobilis ([oldid 150405072](https://it.wikipedia.org/w/index.php?oldid=150405072)) `|statocons=CR|statocons_versione=iucn3.1|statocons_ref=<ref name=IUCN>{{IUCN|summ=34926|autore=Conifer Specialist Group 1998}}</ref>`.

### Citation: `{{IUCN}}`

Doc: [Template:IUCN/man](https://it.wikipedia.org/wiki/Template:IUCN/man); source: [Template:IUCN](https://it.wikipedia.org/wiki/Template:IUCN). A wrapper around `{{cita testo}}`.

| Parameter | Meaning |
| --- | --- |
| `autore` | Author(s) of the assessment. Live articles append the year: `autore=Bauer, H., Nowell, K. & Packer, C. 2008`. |
| `summ` | IUCN taxon id. The doc says to leave it empty for a global assessment, because the template takes P627 from Wikidata; give it for a regional assessment or to override Wikidata. A `summ` that differs from P627 puts the article in "Categoria:Voci con template IUCN con id da controllare". |
| `assessment` | Assessment id, "needed when citing a regional assessment". With it the URL is `https://www.iucnredlist.org/species/<summ>/<assessment>`; without it `https://www.iucnredlist.org/details/<summ or P627>/0`. |
| `titolo` | Optional; the name as IUCN shows it; default is the page name. Needed when citing another taxon. |
| `cid` | Optional anchor for `{{Cita}}`. |
| `accesso` | Optional, recommended: access date. |

The source passes fixed values to `{{cita testo}}`: `anno=2020`, `edizione=Versione 2020.2`, `lingua=en`, `sito=IUCN Red List of Threatened Species`, `editore=IUCN`. No DOI, volume or article-number parameter.

Live, Ursus maritimus ([oldid 152407153](https://it.wikipedia.org/w/index.php?oldid=152407153)): `<ref name="iucn status 19 November 2021">{{IUCN|summ=22823|autore=Wiig, Ø., Amstrup, S., Atwood, T., Laidre, K., Lunn, N., Obbard, M., Regehr, E. & Thiemann, G. (2015)}}</ref>`. Wollemia nobilis also has a generic `{{Cita pubblicazione|cognome=IUCN|data=2010-10-20|titolo=Wollemia nobilis: Thomas, P.: The IUCN Red List of Threatened Species 2011: e.T34926A9898196|...|doi=10.2305/iucn.uk.2011-2.rlts.t34926a9898196.en|...}}`, so some editors work around the fixed year.

### Inline status template

None. The only Template-namespace page starting "IUCN" is `Template:IUCN` ([allpages prefix IUCN](https://it.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10&apfilterredir=nonredirects)); the `Template:Statocons <stato>` pages are status cells for boxes, not inline badges.

### Notes for a generator

- Write `statocons_versione` in lower case.
- For the global assessment, `{{IUCN|autore=...|accesso=...}}` without `summ` is what the doc asks for; the link then follows the article's Wikidata item.

---

## pt: Portuguese Wikipedia (1,184,141 articles)

**Recommendation: support.** Two taxoboxes are in use, `{{Info/Taxonomia}}` (89,019 articles) with Portuguese parameter names and `{{Speciesbox}}` (22,776) with English ones, and both take PE and PEW. `{{citar iucn}}` (8,594 articles) is a port of an older version of English `{{cite iucn}}` (Lua, `Módulo:Iucn`); it takes `página` (`page`), not `article-number`, and accepts only DOIs ending in `.en`.

### Taxobox: `{{Info/Taxonomia}}`

Doc: [Predefinição:Info/Taxonomia/doc](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:Info/Taxonomia/doc) lists `| estado = `, `| estado_ref = `, `| sistema_estado = iucn3.1 ou iucn2.3`. The codes are in the template source: [Predefinição:Info/Taxonomia](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:Info/Taxonomia).

- `estado` (alias `state`): the source's `#switch` accepts `EX`, `EW`, `CR`, `EN`, `VU`, `NT`, `LC`, `DD`, `NE`, `PE`, `PEW`, `CD`, `LR/cd`, `LR/nt` (`LR/NT`), `LR/lc` (`LR/LC`), `LR`, each also in lower case, plus `SE`, `DOM`, `CITES_A1..A3`, `FÓSSIL`, `PRE`, `texto`. `LR` alone and a bare `CD` add "Categoria:Estado de conservação inválido"; `LR/cd` is the accepted form. No `RE` or `NA`.
- `sistema_estado` (alias `state_system`): `iucn3.1` or `iucn2.3`. Lower case matters: the image name is `Status {{{sistema_estado|iucn3.1}}} <code> pt.svg`, and the "(IUCN 3.1)" label is a `#switch` on the lower-case values only.
- `estado_ref` (alias `state_ref`): the reference, shown in small text after the label.
- No Wikidata status (the source reads Wikidata only for images, P18, P13162, and the range map, P181).

Live: Urso-polar ([oldid 73129264](https://pt.wikipedia.org/w/index.php?oldid=73129264)) `| estado = VU`, `| estado_ref = <ref name="...">{{citar iucn |...}}</ref>`, `| sistema_estado = iucn3.1`; Leão ([oldid 72980079](https://pt.wikipedia.org/w/index.php?oldid=72980079)) the same with `estado_ref = <ref name="IUCN"/>`.

### Taxobox: `{{Speciesbox}}`

Source: [Predefinição:Speciesbox](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:Speciesbox) takes `status`/`estado`, `status_system`/`sistema_estado`, `status_ref`/`estado_ref` and passes them to [Predefinição:Info/Taxonomia/core](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:Info/Taxonomia/core), which draws the status cell with [Predefinição:Info/Taxonomia/species](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:Info/Taxonomia/species) (the same code table as [Predefinição:Info/Taxonomia/estado de conservação](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:Info/Taxonomia/estado_de_conserva%C3%A7%C3%A3o), the pt page linked to English Template:Taxobox/species). Its `#switch` on the system accepts `IUCN`, `iucn3.1`, `IUCN3.1` with codes `EX EW CR EN VU NT LC DD NE NR PE PEW`, and `IUCN2.3` (and `iucn2.3`) with `EX EW CR EN VU LR CD LR/cd NT LR/nt LC LR/lc DD NE NR PE`, plus lower-case `pew` (the upper-case `PEW` label under 2.3 is misspelt `PEW-` in the source). Live: Lipotes vexillifer ([oldid 73063422](https://pt.wikipedia.org/w/index.php?oldid=73063422)) `| status = PE`, `| status_system = IUCN3.1`, `| status_ref = <ref name="iucn">{{citar iucn |...}}</ref>`.

### Citation: `{{citar iucn}}`

Doc: [Predefinição:Citar iucn/doc](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:Citar_iucn/doc); source: [Módulo:Iucn](https://pt.wikipedia.org/wiki/M%C3%B3dulo:Iucn), which renders `{{citar periódico}}`.

- Doc: `título` (the name, the editor adds the italics), `página` (the electronic page number `e.T<digits>A<digits>`, from which the module builds the URL), `últimon`/`primeiron` or any author parameter of `{{citar periódico}}`, `ano` (alias `data`), `volume` (normally the year; may be omitted when `ano` is set). The doc says the template accepts all English parameters of `{{citar periódico}}`, and points to `{{make citar iucn}}`.
- Module, as read from the source: page from `page` or `página`, which must match `^[eE]%.[Tt](%d+)[Aa](%d+)$`; DOI must match `[Tt](%d+)[Aa](%d+)%.en$`, otherwise "malformed |doi= identifier" (so the `.es`, `.fr`, `.pt` DOIs that English `{{cite iucn}}` accepts are errors here); year from `year`, `ano`, `date` or `data`, copied into `volume` when that is empty; `errata=<year>` sets "errata version of <year> assessment" and makes the errata year the date; `amends=<year>` sets "amended version of <year> assessment"; the URL is built as `https://www.iucnredlist.org/species/<taxon>/<assessment>`; the journal defaults to "Lista Vermelha de Espécies Ameaçadas"; `id` is discarded. It does not read `article-number`.
- Access date: the doc examples use `acessodata=18-10-2018`; live articles use "19 de novembro de 2021" and English `access-date=30 de Janeiro de 2022`.

Live, Urso-polar ([oldid 73129264](https://pt.wikipedia.org/w/index.php?oldid=73129264)): `{{citar iucn |último1=Wiig |primeiro1=Ø. |último2=Amstrup |primeiro2=S. |...|ano=2015 |título=''Ursus maritimus'' |volume=2015 |página=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en |acessodata=19 de novembro de 2021 |idioma=en}}`. Leão: `{{citar iucn |author=Bauer, H. |author2=Packer, C. |...|year=2016 |errata=2017 |title=''Panthera leo'' |volume=2016 |page=e.T15951A115130419 |doi=10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en |access-date=30 de Janeiro de 2022}}`.

`{{Citar IUCN}}` (capital; a `hastemplate` search gives 1,231 articles, but its first hit is an article on the 2014 FIFA World Cup, so the count is unverified) is marked obsolete in favour of `{{Citar iucn}}` (`{{Pobsoleta|Citar iucn}}` in [its source](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:Citar_IUCN)).

### Inline status template

`{{IUCN status}}` exists but is an old copy used in 18 articles: `{{IUCN status|<code>|<taxon id>|<1, 2 or 12>}}`, linking `http://www.iucnredlist.org/details/<id>/0` ([Predefinição:IUCN status](https://pt.wikipedia.org/wiki/Predefini%C3%A7%C3%A3o:IUCN_status), revision of 2016). It has no assessment id or year.

### Notes for a generator

- English `{{cite iucn}}` output almost works if renamed `{{citar iucn}}`, with two changes: `|article-number=` becomes `|página=` (or `|page=`), and a non-`.en` DOI must be left out.
- Use `iucn3.1`/`iucn2.3` in lower case in `{{Info/Taxonomia}}`.

---

## nl: Dutch Wikipedia (2,228,810 articles)

**Recommendation: support the taxobox lines only.** `{{Taxobox}}` (934,702 articles, counting its wrappers such as `{{Taxobox zoogdier}}`, 8,893) has `status`, `statusbron` and `rl-id`, and makes its own IUCN reference from `rl-id`. There is no IUCN citation template.

### Taxobox: `{{Taxobox}}` and its wrappers

Source and inline documentation (TemplateData): [Sjabloon:Taxobox](https://nl.wikipedia.org/wiki/Sjabloon:Taxobox); the wrappers pass the same parameters on ([Sjabloon:Taxobox zoogdier](https://nl.wikipedia.org/wiki/Sjabloon:Taxobox_zoogdier)). The doc only says `status` is "De beschermingsstatus" and `rl-id` is "Rode lijst id"; the values below are from the source's `#switch`.

- `status`: each group of values below is accepted (Dutch, English or code):
  - EX: `Uitgestorven`, `uitgestorven`, `Extinct`, `extinct`, `EX` (the year of extinction goes in `in`)
  - EW: `Uitgestorven in het wild`, `UIHW`, `Extinct in the Wild`, `EW` (and spellings without spaces)
  - CR: `Kritiek`, `kritiek`, `Critically Endangered`, `CR`
  - EN: `Bedreigd`, `bedreigd`, `Endangered`, `endangered`, `EN`
  - VU: `Kwetsbaar`, `kwetsbaar`, `Vulnerable`, `vulnerable`, `VU`
  - NT: `Gevoelig`, `gevoelig`, `Near Threatened`, `near threatened`, `NT`, `LR/NT`
  - LR/cd: `Van bescherming afhankelijk`, `conservation dependent`, `VBA`, `LR/CD`, `CD` (and spellings without spaces)
  - LC: `Veilig`, `Niet bedreigd`, `Secure`, `Least Concern`, `least concern`, `LC`, `LR/LC`
  - DD: `Onzeker`, `onzeker`, `DataDeficient`, `DD`
  - NE/NA: `Nvt`, `nvt`, `NA`, `na`, `NE` (all shown as "Niet geëvalueerd")
  - also `Fossiel`/`fossil`.
  There is no PE or PEW; the baiji article uses `status = Kritiek`.
- `statusbron`: free text shown in brackets after the status; live articles put a year there (`statusbron = 2015` in IJsbeer).
- `rl-id`: the IUCN taxon id. When it is set (in the article namespace) the box itself adds a reference named `IUCN`: `{{en}} [https://www.iucnredlist.org/details/<rl-id>/0 <naam or page name> op de IUCN Red List of Threatened Species].`
- There is no status-system parameter: the LR codes are just aliases of NT, LC and CD.
- No reference parameter, and no Wikidata status.

Live: IJsbeer ([oldid 71591001](https://nl.wikipedia.org/w/index.php?oldid=71591001)) `| status = VU`, `| statusbron = 2015`, `| rl-id = 22823`; Chinese vlagdolfijn ([oldid 71585349](https://nl.wikipedia.org/w/index.php?oldid=71585349)) `| status = Kritiek`, `| statusbron = 2017`, `| rl-id = 12119`.

### Citation

No IUCN template: `Sjabloon:IUCN` does not exist ([API result](https://nl.wikipedia.org/w/api.php?action=query&titles=Sjabloon:IUCN&format=json)) and no Template-namespace page starts with "IUCN" ([allpages](https://nl.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10&apfilterredir=nonredirects)). Leeuw (dier) ([oldid 70243860](https://nl.wikipedia.org/w/index.php?oldid=70243860)) cites with a bare link: `<ref>[https://www.iucnredlist.org/details/15951/0 Red list, IUCN]</ref>`. The general web citation template is `{{Citeer web}}` (261,427 articles), linked to English Cite web on Wikidata.

### Inline status template

None.

### Notes for a generator

- The box already writes a reference named `IUCN`, so an article reference with `name="IUCN"` and different content would be a second definition of the same name. Use another name for a full citation.

---

## ru: Russian Wikipedia (2,121,464 articles)

**Recommendation: citation only, later; the status normally comes from Wikidata.** `{{Таксон}}` (60,205 articles) shows P141 from the article's Wikidata item when `iucnstatus` is empty, and the doc says the `iucn` id should usually be left empty too. A local `iucnstatus` overrides Wikidata, and its letter case selects the criteria version. The citation template `{{IUCN}}` (4,612 articles) takes only the taxon id, the name and the access date, plus `author` and `date`.

### Taxobox: `{{Таксон}}`

Doc: [Шаблон:Таксон/doc](https://ru.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:%D0%A2%D0%B0%D0%BA%D1%81%D0%BE%D0%BD/doc), section IV; status values: [Шаблон:Таксон/IUCN/doc](https://ru.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:%D0%A2%D0%B0%D0%BA%D1%81%D0%BE%D0%BD/IUCN/doc); source: [Шаблон:Таксон/IUCN](https://ru.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:%D0%A2%D0%B0%D0%BA%D1%81%D0%BE%D0%BD/IUCN).

- `iucnstatus`: upper case means IUCN 3.1 and lower case means IUCN 2.3: `LC`/`lc`, `NT`/`nt`, `VU`/`vu`, `EN`/`en`, `CR`/`cr`, `EW`/`ew`, `EX`/`ex`; `CD` or `cd` is always 2.3 (LR/cd); `DD` or `dd` has no version. There is no NE, no PE/PEW and no `LR/..` spelling: LR/nt is `nt` and LR/lc is `lc`. Any other value shows "Статус не указан". The source reads `{{Wikidata|p141|{{{1|}}}|plain=true}}`, and [Шаблон:Wikidata/doc](https://ru.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:Wikidata/doc) says the second argument overrides Wikidata and Wikidata is used only when it is empty. The Wikidata values map to the 3.1 forms (Q211005 LC, Q719675 NT, Q158862 CD, Q278113 VU, Q11394/Q96377276 EN, Q219127 CR, Q239509 EW, Q237350 EX, Q3245245 DD).
- `iucn`: IUCN taxon id. The doc: "В большинстве случаев параметр должен оставаться пустым, так как значение берётся из соответствующего статье элемента Викиданных" (leave it empty in most cases; the value comes from Wikidata). The box source takes the id from the P627 value in the first reference of the item's first P141 statement (`{{#invoke:Wikibase|struc|claims|P141|1|references|1|snaks|P627|1|datavalue|value}}`), and uses `iucn` only when that lookup returns an error. The link is `https://www.iucnredlist.org/details/<id>/0`.
- There is no reference parameter in the box.

Live: Белый медведь ([oldid 155197224](https://ru.wikipedia.org/w/index.php?oldid=155197224)) `| iucnstatus  = VU` and no `iucn`; Лев ([oldid 154895177](https://ru.wikipedia.org/w/index.php?oldid=154895177)) `| iucnstatus = VU`, `| iucn = 15951`; Китайский речной дельфин ([oldid 154867678](https://ru.wikipedia.org/w/index.php?oldid=154867678)) `| iucnstatus = CR` with a comment that the assessors say the species may be extinct.

### Citation: `{{IUCN}}`

Doc: [Шаблон:IUCN/doc](https://ru.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:IUCN/doc); source: [Шаблон:IUCN](https://ru.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:IUCN). A wrapper around ru `{{cite web}}`.

| Parameter | Meaning |
| --- | --- |
| `1` | IUCN taxon id. In the source it is the local value of `{{wikidata|p627|...}}`, so it overrides the item's P627; when both are empty the link is an IUCN search for P225. |
| `2` | Name of the taxon; default is P225 from Wikidata. Italicised by the template. |
| `3` (or `accessdate`) | Date the link was checked. Doc example: `24 декабря 2014 г.` |
| `author` | Author(s) of the assessment. |
| `date` | Date the assessment was published. |
| `ref` | Anchor for `{{sfn}}`, default "IUCN Red List". |

The link is always `https://www.iucnredlist.org/details/<id>/0` (the current assessment). No DOI, assessment id or page parameter.

Live, Лев: `<ref name="IUCN">{{IUCN|15951}}</ref>`. Doc example: `* {{IUCN|9295|{{btname|Romanogobio albipinnatus|(Lukasch, 1933)}}|24 декабря 2014 г.}}`.

`{{Cite iucn}}` also exists (289 articles): [Шаблон:Cite iucn](https://ru.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:Cite_iucn) calls [Модуль:Cite iucn](https://ru.wikipedia.org/wiki/%D0%9C%D0%BE%D0%B4%D1%83%D0%BB%D1%8C:Cite_iucn), an older copy of the English module: it reads `page` (`e.T..A..`), accepts only DOIs ending in `.en`, and has no documentation page.

### Inline status template

None (Template-namespace pages starting "IUCN": `IUCN`, `IUCN-search`, `IUCN1`, `IUCN2006`, `IUCNCS`; [allpages](https://ru.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10&apfilterredir=nonredirects)).

### Notes for a generator

- When Wikidata's P141 is current, no box wikitext is needed. To override, write `| iucnstatus = VU` (upper case for 3.1, lower case for 2.3); there is no way to show CR(PE), so it is `CR`.

---

## ja: Japanese Wikipedia (1,521,863 articles)

**Recommendation: support.** `{{生物分類表}}` (21,640 articles) has `status` and `status_ref`, and the version is part of the code (`VU2.3`). `{{Cite iucn}}` (1,393 articles) is a recent copy of the English module with `article-number`, `errata`, `amends` and DOIs in en/es/fr/pt.

### Taxobox: `{{生物分類表}}`

Doc: [Template:生物分類表/doc](https://ja.wikipedia.org/wiki/Template:%E7%94%9F%E7%89%A9%E5%88%86%E9%A1%9E%E8%A1%A8/doc), which transcludes [プロジェクト:生物/分類表](https://ja.wikipedia.org/wiki/%E3%83%97%E3%83%AD%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88:%E7%94%9F%E7%89%A9/%E5%88%86%E9%A1%9E%E8%A1%A8) (section 保全状況評価); source: [Template:生物分類表](https://ja.wikipedia.org/wiki/Template:%E7%94%9F%E7%89%A9%E5%88%86%E9%A1%9E%E8%A1%A8).

- `status`: the doc lists `LC`, `NT`, `VU`, `EN`, `CR`, `EW`, `EX` (upper or lower case) and the Japanese Ministry of the Environment categories. The source also accepts `DD`, `NE`, `PE`, `PEW`, `LR/cd`, `LR/nt` (also `NT2.3`), `LR/lc` (also `LC2.3`), `LR`, and the 2.3 forms `VU2.3`, `EN2.3`, `CR2.3`, `EW2.3`, `EX2.3`, `DD2.3`; plain `VU` etc. are labelled "IUCN Red List Ver.3.1 (2001)". `CD` alone draws the EPBC image (`Status {{{status_system|EPBC}}} CD.svg`), so LR/cd is `LR/cd`. There is no `RE` or `NA`.
- `status_system`: only used in image names for `PE`, `PEW`, `CD` and the non-IUCN systems; the IUCN codes ignore it. With `PE` the image is `Status_{{{status_system|none}}}_PE.svg`: the default gives `File:Status none PE.svg`, which exists on Commons, while `status_system = IUCN3.1` gives `File:Status IUCN3.1 PE.svg`, which does not ([Commons API check](https://commons.wikimedia.org/w/api.php?action=query&titles=File:Status%20none%20PE.svg|File:Status%20IUCN3.1%20PE.svg&format=json)). So leave `status_system` out.
- `status_ref`: the reference, shown beside the heading.
- `status_text`: extra text under the status (articles put the CITES appendix there).
- The project page asks for only the status of the article's own taxon in the box, not of subspecies or subpopulations, and for a "保全状況評価" section in the body too ([プロジェクト:生物](https://ja.wikipedia.org/wiki/%E3%83%97%E3%83%AD%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88:%E7%94%9F%E7%89%A9), section 保全状況評価の表示).

Live: ホッキョクグマ ([oldid 110399309](https://ja.wikipedia.org/w/index.php?oldid=110399309)) `|status = VU`; イノシシ ([oldid 111243140](https://ja.wikipedia.org/w/index.php?oldid=111243140)) `| status = LC | status_system = IUCN3.1` and `|status_ref = <ref>{{cite iucn ...}}</ref>`; ヨウスコウカワイルカ ([oldid 110993377](https://ja.wikipedia.org/w/index.php?oldid=110993377)) `|status = CR` with the IUCN citation as plain text in `status_ref`.

### Citation: `{{Cite iucn}}`

Doc: [Template:Cite iucn/doc](https://ja.wikipedia.org/wiki/Template:Cite_iucn/doc); source: [Module:Cite iucn](https://ja.wikipedia.org/wiki/%E3%83%A2%E3%82%B8%E3%83%A5%E3%83%BC%E3%83%AB:Cite_iucn), rendering `{{cite journal2}}`.

- Doc parameters: `title` (italics added by the editor), `article-number` (`e.T<digits>A<digits>`, from which the URL is built), `lastn`/`firstn` or other author parameters, `date` (alias `year`), `errata` (year of the errata version), `amends` (year of the amended assessment), `volume` (normally the year; may be left out when `date` is set). Copy-paste form: `{{Cite IUCN | last𝑛 = | first𝑛 = | date = | title = | article-number = | doi = | access-date = {{subst:CURRENTDAY}} {{subst:CURRENTMONTHNAME}} {{subst:CURRENTYEAR}} }}`.
- Module: DOI must end in `.en`, `.es`, `.fr` or `.pt` (line `({['en'] = true, ['es'] = true, ['fr'] = true, ['pt'] = true})[lang_tag]`), and `language` is taken from the DOI's suffix.
- Date format: the doc examples use English dates (`access-date=18 October 2018`); the live article uses `access-date=28 May 2025`.

Live, イノシシ: `{{cite iucn |author=Keuling, O. |author2=Leus, K. |year=2019 |title=''Sus scrofa'' |volume=2019 |article-number=e.T41775A44141833 |doi=10.2305/IUCN.UK.2019-3.RLTS.T41775A44141833.en |access-date=28 May 2025}}`. Older articles cite IUCN as plain text (ホッキョクグマ: `<ref name="iucn">Wiig, Ø., ... 2015. ''Ursus maritimus''. The IUCN Red List of Threatened Species 2015: e.T22823A14871490. https://doi.org/10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en. Downloaded on 09 April 2020.</ref>`).

### Inline status template

- `{{IUCN status}}` exists (20 articles), an old copy: `{{IUCN status|<code>|<IUCN species ID>|<1, 2 or 12>}}` with no assessment id or year ([Template:IUCN status/doc](https://ja.wikipedia.org/wiki/Template:IUCN_status/doc)). Its doc tells editors to read the project page first, which advises against listing IUCN statuses in articles on genera and families, because such lists go out of date.
- For the body section, one template per category, e.g. `{{Critically endangered|IUCN=3.1|ref=<ref>...</ref>}}` (`IUCN=2.3` for the 1994 version; [Template:Critically endangered](https://ja.wikipedia.org/wiki/Template:Critically_endangered)); used in ライオン.

---

## zh: Chinese Wikipedia (1,559,799 articles)

**Recommendation: support.** `{{Speciesbox}}` (88,631 articles) takes the English `status`, `status_system` and `status_ref`, and its status cell is a copy of the English pre-module code table, with PE and PEW. Two citation templates are in use: `{{cite iucn}}` (4,912 articles, an older copy of the English module with `page`, no documentation) and `{{IUCN}}` (3,939 articles, documented, with `id`, `page`, `doi`, `errata`, `amends`).

### Taxobox: `{{Speciesbox}}` (and the other automatic taxoboxes)

Doc: [Template:Speciesbox/doc](https://zh.wikipedia.org/wiki/Template:Speciesbox/doc) (TemplateData: `status`, example `LC`; `status_system`, example `IUCN3.1`; `status_ref`); the status cell is drawn by [Template:Taxobox/species](https://zh.wikipedia.org/wiki/Template:Taxobox/species), called from [Template:Taxobox/core](https://zh.wikipedia.org/wiki/Template:Taxobox/core) with the system first and the code second.

- Under `IUCN3.1` (also `iucn3.1`, `IUCN`): `EX EW CR EN VU NT LC DD NE NR PE PEW`, each also in lower case.
- Under `IUCN2.3` (also `iucn2.3`): `EX EW CR EN VU LR CD LR/cd NT LR/nt LC LR/lc DD NE NR PE PEW` (each `LR/..` also `LR/CD`, `lr/cd` etc.).
- No `NA` or `RE`. No Wikidata status.

Live: 白鱀豚 ([oldid 93454644](https://zh.wikipedia.org/w/index.php?oldid=93454644)) `| status = PE`, `| status_system = IUCN3.1`, `| status_ref = <ref name="IUCN">{{cite iucn |...}}</ref>`; 狮 ([oldid 93475510](https://zh.wikipedia.org/w/index.php?oldid=93475510)) `| status = VU`, `| status_system = IUCN3.1`, `| status_ref = {{r|IUCN}}`.

### Citation: `{{IUCN}}`

Doc: [Template:IUCN/doc](https://zh.wikipedia.org/wiki/Template:IUCN/doc); source: [Template:IUCN](https://zh.wikipedia.org/wiki/Template:IUCN) (a wrapper around `{{Citation}}`).

| Parameter | Meaning (from the doc) |
| --- | --- |
| `authorn` (`author` for one) | One assessor per parameter, "Surname, I." The doc advises against the free-text `authors`/`assessors`, because CS1 separates authors with semicolons; the template adds the "&" before the last author itself (`last-author-amp = yes`). |
| `year` (`date`) | Year the assessment was published. Required. |
| `id` | IUCN taxon id. Required. The link is `https://www.iucnredlist.org/details/<id>/0`. |
| `title` (`taxon`) | Scientific name, italicised by the editor, with rank abbreviations upright (`''Neophocaena asiaeorientalis'' ssp. ''asiaeorientalis''`). Required. |
| `errata` | The words from IUCN's citation, "errata version published in 2017"; the template adds the brackets. |
| `amends` | The words "amended version of 2015 assessment". |
| `page` | Article number `e.T<taxon>A<assessment>`. |
| `doi` | DOI starting "10.2305", without `https://doi.org/`. |
| `access-date` (`downloaded`) | ISO 8601 `YYYY-MM-DD`; not needed when a DOI is given. |

Doc example (the lion): `{{IUCN |author1=Bauer, H. |author2=Packer, C. |author3=Funston, P.F. |author4=Henschel, P. |author5=Nowell, K. |year=2016 |id=15951 |title=''Panthera leo'' |errata=errata version published in 2017 |page=e.T15951A115130419 |doi=10.2305/IUCN.UK.2016-3.RLTS.T15951A107265605.en}}`; 狮 has this citation as `<ref name="IUCN">`.

### Citation: `{{cite iucn}}`

[Template:Cite iucn](https://zh.wikipedia.org/wiki/Template:Cite_iucn) calls [Module:Cite iucn](https://zh.wikipedia.org/wiki/Module:Cite_iucn) (revision of July 2024), an older copy of the English module: it reads `page`, accepts DOIs ending `.en` or `.es` only, takes `errata`/`amends` as years, and wraps `{{cite journal}}`. There is no doc page ([Template:Cite iucn/doc](https://zh.wikipedia.org/wiki/Template:Cite_iucn/doc) is missing). Live, 白鱀豚: `{{cite iucn |author1=Smith, B.D. |author2=Wang, D. |...|year=2017 |title=''Lipotes vexillifer'' |page=e.T12119A50362206 |doi=10.2305/IUCN.UK.2017-3.RLTS.T12119A50362206.en}}`.

The versioned `{{IUCN2008}}` (730) and `{{IUCN2006}}` (589) are older forms still in articles (北極熊 uses `{{IUCN2008|assessors=...|year=2008|id=22823|title=Ursus maritimus|downloaded=2010年2月22日}}`).

### Inline status template

None: `Template:IUCN status` does not exist on zh ([API](https://zh.wikipedia.org/w/api.php?action=query&titles=Template:IUCN%20status&format=json)).

### Notes for a generator

- The English taxobox lines can be used as they are.
- Emit `{{IUCN}}` rather than `{{cite iucn}}`: it is documented, passes any DOI through, takes the taxon id directly, and is the form 狮 uses; `{{cite iucn}}` has no doc page and rejects `.fr`/`.pt` DOIs.
- In `{{IUCN}}`, `errata`/`amends` take the whole phrase ("errata version published in 2017"), not just the year as on English Wikipedia; `|page=` carries the article number.

---

## pl: Polish Wikipedia (1,710,537 articles)

**Recommendation: support.** `{{Zwierzę infobox}}` (45,126 articles) and `{{Takson infobox}}` (21,592, used for plants) both take `status IUCN` and `IUCN id`, and the box writes a reference named `iucn` unless the article defines one. `{{IUCN}}` (5,213 articles) takes the taxon id or the DOI, the authors, the release year and version, and the access date.

### Taxobox: `{{Zwierzę infobox}}`, `{{Takson infobox}}`

Docs: [Szablon:Zwierzę infobox/opis](https://pl.wikipedia.org/wiki/Szablon:Zwierz%C4%99_infobox/opis), [Szablon:Takson infobox/opis](https://pl.wikipedia.org/wiki/Szablon:Takson_infobox/opis) (its IUCN parameters say "patrz Szablon:IUCN"); sources: [Szablon:Zwierzę infobox](https://pl.wikipedia.org/wiki/Szablon:Zwierz%C4%99_infobox), [Szablon:Takson infobox](https://pl.wikipedia.org/wiki/Szablon:Takson_infobox). The plant article Welwiczja przedziwna ([oldid 80717519](https://pl.wikipedia.org/w/index.php?oldid=80717519)) uses `{{Takson infobox}}`; `Szablon:Roślina infobox` does not exist.

- `status IUCN`: the doc says EX, EW, CR, EN, VU, NT, LC in upper or lower case, anything else showing "brak danych" (no data). The source's `#switch` has only upper-case `EX`, `EW`, `CR`, `EN`, `VU`, `NT`, `LC`, `DD`; any other value, lower case included, shows "brak danych" and adds "Kategoria:Infoboksy – błędne dane – ... – status IUCN". So write upper case. There is no PE/PEW, NE, LR/.. or status system; the status is shown with the 3.1 images. Shown only when `gatunek` or `podgatunek` is set.
- `IUCN id`: the IUCN taxon id. The box adds a footnote under the heading "Kategoria zagrożenia (CKGZ)". If the article text contains `<ref name="iucn">` (the source tests `%<ref%s+name%s*=%s*"?iucn"?%s*%>`), it uses `<ref name="iucn" />`; otherwise it writes its own reference to `https://www.iucnredlist.org/details/<IUCN id>/0`. The Zwierzę infobox doc says the same: to give a fuller citation, put `<ref name="iucn">...</ref>` in the article's references section.
- The box adds a category from the status (Gatunki krytycznie zagrożone, ...), and maintenance categories when only one of `status IUCN` and `IUCN id` is given.
- No Wikidata status.

Live: Niedźwiedź polarny ([oldid 80433403](https://pl.wikipedia.org/w/index.php?oldid=80433403)) `|status IUCN = VU`, `|IUCN id = 22823`; Baji chiński ([oldid 78372794](https://pl.wikipedia.org/w/index.php?oldid=78372794)) `|status IUCN = CR`, `|IUCN id = 12119`; Wolemia szlachetna ([oldid 79844101](https://pl.wikipedia.org/w/index.php?oldid=79844101)) `{{Takson infobox}}` with `|IUCN id = 34926`, `|status IUCN = CR`.

### Citation: `{{IUCN}}`

Doc: [Szablon:IUCN/opis](https://pl.wikipedia.org/wiki/Szablon:IUCN/opis); source: [Szablon:IUCN](https://pl.wikipedia.org/wiki/Szablon:IUCN) (via `Moduł:Cytuj`).

| Parameter | Meaning |
| --- | --- |
| `id` | Required. IUCN taxon id (the digits after "e.T" in IUCN's citation). The source checks it is digits only (or `<digits>/Europe`). Link: `https://www.iucnredlist.org/details/<id>/0`. |
| `nazwa` | Required. Scientific name. |
| `data dostępu` | Required. Access date; the doc example is `{{CURRENTYEAR}}-{{CURRENTMONTH}}-{{CURRENTDAY2}}` (ISO). |
| `autor` | Assessors. An author containing "International" or "Group" is marked as an organisation (`*` prefix, from the source). |
| `iucn rok` | Year of the Red List edition, four digits ("2013"). |
| `wersja` | Red List version ("2013.2"). |
| `doi` | DOI; "can be used instead of id". With a DOI the template drops the iucnredlist URL. |
| `odn`, `archiwum` | Harvard anchor; archive URL. |

There is no assessment id or article-number parameter, and no assessment year as such (live articles put the assessment year in `iucn rok`).

Live, Niedźwiedź polarny: `<ref name="iucn">{{IUCN |id = 22823 |nazwa = Ursus maritimus |autor = Ø. Wiig, S. Amstrup, T. Atwood, K. Laidre, N. Lunn, M. Obbard, E. Regehr, G. Thiemann |iucn rok = 2015 |wersja = 2015.1 |data dostępu = 2015-07-13}}</ref>`. Authors are written "Initials Surname".

### Inline status template

None (`Szablon:IUCN` is the only Template-namespace page starting "IUCN"; [allpages](https://pl.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10&apfilterredir=nonredirects)).

### Notes for a generator

- Name the citation `<ref name="iucn">` (lower case): the infobox then points its own footnote at it instead of adding a second reference.
- CR(PE) can only be written `CR`.

---

## sv: Swedish Wikipedia (2,628,482 articles)

**Recommendation: support, low priority.** `{{Taxobox}}` (1,321,969 articles) takes `status` and `status_ref`, but its code table has no PE, PEW or NE and no status system, although its doc lists them. The citation templates are one per Red List release (`{{IUCN2025.1}}`, `{{IUCN2024-2}}`, ...), each printing its own fixed version, and none exists for releases after 2025.1.

### Taxobox: `{{Taxobox}}`

Doc: [Mall:Taxobox/dok](https://sv.wikipedia.org/wiki/Mall:Taxobox/dok); source: [Mall:Taxobox](https://sv.wikipedia.org/wiki/Mall:Taxobox).

- `status`: the source lower-cases the value and accepts `dom`, `dd`, `lr`, `lc` (also `lr/lc`, `lrlc`, `se`, `secure`), `nt` (also `lr/nt`, `lrnt`), `lr/cd` (`lrcd`), `vu`, `en`, `cr`, `ew`, `ex` (`extinct`, with `extinct` = year), `fossil`, `pre`, `text`. Any other value is printed as it is, with no category. The doc's TemplateData also lists `PE` and `NE` and a `status_system` parameter ("iucn3.1, iucn2.3, EPBC etc."), but the source has neither; `PE` would show as the bare text "PE". The status is shown under the name as "Status i världen: <Swedish category name>".
- `status_ref`: appended after the status; the doc gives `<ref>{{IUCN2012.2|...}}</ref>`.
- No Wikidata status (the box reads Wikidata only for the image and range map).

Live: Isbjörn ([oldid 59249082](https://sv.wikipedia.org/w/index.php?oldid=59249082)) `| status = VU`, `| status_ref = <ref name=IUCN>{{IUCN2018.1 | assessors = Wiig, Ø., Amstrup, S., Atwood, T., Laidre, K., Lunn, N., Obbard, M., Regehr, E. & Thiemann, G. | år = 2015 | id = 22823/14871490 | titel = Ursus maritimus | hämtdatum = 7 mars 2023 }}</ref>`; Asiatisk floddelfin ([oldid 59479390](https://sv.wikipedia.org/w/index.php?oldid=59479390)) `| status = CR`.

### Citation: `{{IUCN<release>}}`

Doc: [Mall:IUCN2025.1/dok](https://sv.wikipedia.org/wiki/Mall:IUCN2025.1/dok); source: [Mall:IUCN2025.1](https://sv.wikipedia.org/wiki/Mall:IUCN2025.1). The Template namespace has one template per release from `IUCN2006` to `IUCN2025.1` ([allpages](https://sv.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10)); the newer ones have the same parameters.

| Parameter | Meaning |
| --- | --- |
| `assessors` | The assessors (a person or an organisation). |
| `år` | Year of the assessment. |
| `id` | Required. `<taxon id>/<assessment id>` as at the end of the URL; the link is `https://www.iucnredlist.org/species/<id>`. |
| `titel` | Scientific name of the assessed species. |
| `hämtdatum` | Access date, passed through `{{Date}}`; live articles write "7 mars 2023". |
| `språk` | Optional language note. |

Output: "<assessors> <år> ''[link <titel>]''. Från: IUCN <år>. ''IUCN Red List of Threatened Species.'' Version 2025.1. Läst <date>." The version is fixed by the template name. Usage: `IUCN2012.2` 31,756 articles (older form: `http://oldredlist.iucnredlist.org/details/<id>/0`), `IUCN2018.1` 6,114, `IUCN2024-2` 2,005, `IUCN2025.1` 816.

### Inline status template

None (only `Mall:IUCN-kategori`, a category navigation box).

### Notes for a generator

- Pick the template named for the release cited; for a release with no template, there is nothing to pick without creating one.
- Possibly extinct can only be `cr`.

---

## uk: Ukrainian Wikipedia (1,437,067 articles)

**Recommendation: support.** Three boxes take the English parameters `status`, `status_system`, `status_ref`: `{{Картка:Таксономія}}` (49,944 articles), `{{Speciesbox}}` (22,505) and `{{Taxobox}}` (17,462), all with PE and PEW. `{{Cite IUCN}}` (1,786 articles; `{{cite iucn}}` redirects to it) is a 2025 copy of the English module that takes `page` and Ukrainian parameter names.

### Taxobox

Doc: [Шаблон:Картка:Таксономія/документація](https://uk.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:%D0%9A%D0%B0%D1%80%D1%82%D0%BA%D0%B0:%D0%A2%D0%B0%D0%BA%D1%81%D0%BE%D0%BD%D0%BE%D0%BC%D1%96%D1%8F/%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%B0%D1%86%D1%96%D1%8F) (section "Охоронний статус"); the Speciesbox status cell is [Шаблон:Taxobox/species](https://uk.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:Taxobox/species), called from `Шаблон:Taxobox/core`.

- `status_system`: `IUCN3.1`, `iucn3.1`, `IUCN` or `iucn` for 3.1; `IUCN2.3` or `iucn2.3` for 2.3 (doc; the source's `#switch` has `|iucn2.3|IUCN2.3=` and `|iucn|IUCN|iucn3.1|IUCN3.1=`).
- `status` under 3.1: `EX EW CR EN VU NT LC DD NE NR PE PEW`, upper or lower case; under 2.3: `EX EW CR EN VU LR CD NT LC DD NE NR PE PEW`, with `LR/CD`, `LR/cd`, `LR/NT`, `LR/nt`, `LR/LC`, `LR/lc` accepted. Any other value is treated as an invalid status (doc). `Шаблон:Taxobox/species` has the same lists.
- `status_ref`: the source of the status (doc).
- No Wikidata status (no P141 in the three box sources).

Live: Ведмідь білий ([oldid 48435060](https://uk.wikipedia.org/w/index.php?oldid=48435060)) `{{Картка:Таксономія}}` with `| status = VU`, `| status_system = iucn3.1`; Лев ([oldid 48373850](https://uk.wikipedia.org/w/index.php?oldid=48373850)) `{{Speciesbox}}` with `| status = VU`, `| status_system=iucn3.1`, `| status_ref = <ref name="IUCN">{{cite iucn|...}}</ref>`.

### Citation: `{{Cite IUCN}}`

Doc: [Шаблон:Cite IUCN/документація](https://uk.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:Cite_IUCN/%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%B0%D1%86%D1%96%D1%8F); source: [Модуль:Cite IUCN](https://uk.wikipedia.org/wiki/%D0%9C%D0%BE%D0%B4%D1%83%D0%BB%D1%8C:Cite_IUCN) (August 2025), rendering `{{cite journal}}`.

- Doc parameters (Ukrainian aliases in brackets): `title` (`назва`; italics by the editor), `page` (`сторінка`; the article number `e.T<digits>A<digits>`, from which the URL is built), `lastn`/`firstn` (`прізвищеn`/`ім'яn`) or any `{{cite journal}}` author parameter, `date` (`year`, `дата`, `рік`), `errata` (`еррата`; year), `amends` (`поправка`; year), `volume` (`том`; normally the year). Copy-paste form uses `access-date: {{CURRENTDAY}} {{CURRENTMONTHNAMEGEN}} {{CURRENTYEAR}}`, i.e. a Ukrainian date with the month in the genitive ("9 жовтня 2026").
- Module: DOI must end in `.en`, `.es`, `.fr` or `.pt`. It does not read `article-number`.

Live, Лев: `{{cite iucn|title=''Panthera leo''|author1=Nicholson, S.|author2=Bauer, H.|...|author9=Loveridge, A.|year=2024|amends=2023|page=e.T15951A259030422|access-date=10 лютого 2025}}`.

`{{IUCN}}` (12,208 articles) is a simpler link template ([Шаблон:IUCN/документація](https://uk.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:IUCN/%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%B0%D1%86%D1%96%D1%8F)): `1` (or `id`) taxon id, required; `2` (or `title`) name, default the page name; `3` (or `accessdate`) date checked; `assessors`, `year`, `version`. Link `https://www.iucnredlist.org/details/<id>/0`. Live, Дельфін озерний ([oldid 44630543](https://uk.wikipedia.org/w/index.php?oldid=44630543)): `{{IUCN|12119|{{btname|Lipotes vexillifer|Miller, 1918}}|10 червня 2010&nbsp;р.}}`.

### Inline status template

`{{IUCN VU}}`, `{{IUCN CR}}` etc. (about 50 articles each, mostly lists) print only a coloured code badge, with no parameters ([Шаблон:IUCN VU](https://uk.wikipedia.org/wiki/%D0%A8%D0%B0%D0%B1%D0%BB%D0%BE%D0%BD:IUCN_VU)).

### Notes for a generator

- The English taxobox lines work unchanged. For the citation, rename `|article-number=` to `|page=`.

---

## ca: Catalan Wikipedia (805,426 articles)

**Recommendation: do not support for now.** The taxobox `{{Infotaula ésser viu}}` (76,423 articles) takes the status only from Wikidata and has no local parameter, so no wikitext is needed there. `{{cite iucn}}` redirects to a template that ignores all its parameters, and the IUCN-specific templates are old and little used.

### Taxobox: `{{Infotaula ésser viu}}`

Doc: [Plantilla:Infotaula ésser viu/ús](https://ca.wikipedia.org/wiki/Plantilla:Infotaula_%C3%A9sser_viu/%C3%BAs): "La versió sense paràmetres importa automàticament les dades de Wikidata." Under "Conservació" it lists P141, P3648 and P8556 with no manual parameter name beside them. The source ([Plantilla:Infotaula ésser viu](https://ca.wikipedia.org/wiki/Plantilla:Infotaula_%C3%A9sser_viu)) shows `{{#invoke:Wikidades | claim | property=P141 | list=false }}` under the label "UICN". The live articles have empty boxes: Os polar ([oldid 38417437](https://ca.wikipedia.org/w/index.php?oldid=38417437)) and Wollemia nobilis ([oldid 38623348](https://ca.wikipedia.org/w/index.php?oldid=38623348)). **No wikitext needed; the status comes from Wikidata.**

### Citation

- `{{cite iucn}}` redirects to `Plantilla:Cita IUCN Redlist` ([source](https://ca.wikipedia.org/wiki/Plantilla:Cita_IUCN_Redlist)), which is a fixed `{{ref-web}}` to `http://www.iucnredlist.org/` titled "The IUCN Red List of Threatened Species" with `consulta = 26/04/2009`, and passes none of the caller's parameters on. Os polar has `{{cite iucn |author=Wiig, Ø. |...|page=e.T22823A14871490 |doi=10.2305/IUCN.UK.2015-4.RLTS.T22823A14871490.en |access-date=19 novembre 2021}}` in a reference, which therefore shows only that fixed text. It is in 307 articles.
- `{{IUCN}}` (642 articles; doc [Plantilla:IUCN/ús](https://ca.wikipedia.org/wiki/Plantilla:IUCN/%C3%BAs), source [Plantilla:IUCN](https://ca.wikipedia.org/wiki/Plantilla:IUCN)): `assessors` (or `autor`), `year` (or `any`), `id` (taxon id; link `http://www.iucnredlist.org/details/<id>/0`), `title` (or `títol`), `downloaded` (or `consulta`). The doc also lists `versió`, which the source does not read. Doc example: `{{IUCN |autor=Kierulff, M.C.M., Rylands, A.B. & de Oliveira, M.M. |any=2008 |id=11506 |títol=Leontopithecus rosalia |consulta=6 d'octubre de 2008}}`. Wollemia nobilis has `{{IUCN |autor=Thomas, P. |any=2011 |id=9898196 |...}}`, which passes the assessment id where the template expects the taxon id (34926), so its link is wrong.
- `{{IUCN2008}}` (2,126 articles) and other versioned templates from 2006 to 2017 also exist ([allpages](https://ca.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10&apfilterredir=nonredirects)).

### Inline status template

None.

---

## vi: Vietnamese Wikipedia (1,304,951 articles)

**Recommendation: support, lower priority.** `{{Bảng phân loại}}` (780,274 articles, most of them bot-created) and `{{Speciesbox}}` (10,857) take the English `status`, `status_system`, `status_ref` and draw the status from a copy of the English code table, with PE and PEW. `{{Chú thích IUCN}}` (6,398 articles; `{{cite iucn}}` redirects to it) is an older copy of the English module: `page`, and DOIs ending `.en` only.

### Taxobox

Doc: [Bản mẫu:Bảng phân loại/doc](https://vi.wikipedia.org/wiki/B%E1%BA%A3n_m%E1%BA%ABu:B%E1%BA%A3ng_ph%C3%A2n_lo%E1%BA%A1i/doc) (TemplateData: `status` codes LC, LR/lc, NT, LR/nt, LR/cd, VU, EN, CR, PE, EW, EX, DD, ...; `status_system` "'iucn3.1', 'iucn2.3', 'EPBC' etc. Required if status given"; `status_ref`). The status cell is [Bản mẫu:Bảng phân loại/loài](https://vi.wikipedia.org/wiki/B%E1%BA%A3n_m%E1%BA%ABu:B%E1%BA%A3ng_ph%C3%A2n_lo%E1%BA%A1i/lo%C3%A0i) (`Bản mẫu:Taxobox/species` redirects there), called by both `{{Bảng phân loại}}` and `Bản mẫu:Taxobox/core` (Speciesbox). Its `#switch`: under `IUCN3.1` (`iucn3.1`, `IUCN`) `EX EW CR EN VU NT LC DD NE NR PE PEW`; under `IUCN2.3` (`iucn2.3`) `EX EW CR EN VU LR CD LR/cd NT LR/nt LC LR/lc DD NE NR PE PEW`; upper or lower case. No Wikidata status.

Live: Gấu trắng Bắc Cực ([oldid 75354469](https://vi.wikipedia.org/w/index.php?oldid=75354469)) `{{Bảng phân loại}}` with `| status = VU`, `| status_system = iucn3.1`; Cá heo sông Trường Giang ([oldid 71528468](https://vi.wikipedia.org/w/index.php?oldid=71528468)) `| status = CR`, `| status_system = IUCN3.1`.

### Citation: `{{Chú thích IUCN}}`

Doc: [Bản mẫu:Chú thích IUCN/doc](https://vi.wikipedia.org/wiki/B%E1%BA%A3n_m%E1%BA%ABu:Ch%C3%BA_th%C3%ADch_IUCN/doc) (in English): `title`, `page` (`e.T<digits>A<digits>`, used to build the URL), `lastn`/`firstn` or other author parameters, `year` (alias `date`), `errata` (year), `amends` (year), `volume`. Source: [Mô đun:Cite IUCN](https://vi.wikipedia.org/wiki/M%C3%B4_%C4%91un:Cite_IUCN) (June 2024), whose DOI check is `[Tt](%d+)[Aa](%d+)%.en$`. Doc examples use English dates (`access-date=18 October 2018`).

Live, Cá heo sông Trường Giang: `{{cite iucn |author=Smith, B.D. |author2=Wang, D. |...|date=2017 |title=''Lipotes vexillifer'' |volume=2017 |page=e.T12119A50362206 |doi=10.2305/IUCN.UK.2017-3.RLTS.T12119A50362206.en |access-date=20 December 2021}}`. Gấu trắng Bắc Cực uses `|article-number=` instead, which the module does not read.

`{{IUCN}}` (8,335 articles) is the older id-based template (required `id`, `year`, `title`/`taxon`, `version`; link `http://www.iucnredlist.org/details/<id>/0`); its doc ([Bản mẫu:IUCN/doc](https://vi.wikipedia.org/wiki/B%E1%BA%A3n_m%E1%BA%ABu:IUCN/doc)) calls the id-based IUCN templates obsolete and recommends a journal citation with the DOI.

### Inline status template

None among the Template-namespace pages starting "IUCN" ([allpages](https://vi.wikipedia.org/w/api.php?action=query&list=allpages&apprefix=IUCN&apnamespace=10&apfilterredir=nonredirects)).

### Notes for a generator

- Same as uk: the English taxobox lines work unchanged; in the citation use `|page=` and leave out a non-`.en` DOI.

---

## Taxobox codes side by side

How each wiki's box takes the cases that differ between wikis. "–" means the box has no way to show it. Sources are in each wiki's section.

| Wiki (box) | Status parameter | System parameter | CR(PE) / CR(PEW) | LR/cd | LR/nt | LR/lc | NE | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fr (`{{Taxobox UICN}}`) | positional 1 | none | `PE` / `PEW` | `CD` | `NT` | `LC` | `NE` | upper case only; criteria in positional 2 |
| es (`{{Ficha de taxón}}`) | `status` | `status_system` = `IUCN3.1` / `IUCN2.3` | `PE` / `PEW` | `LR/cd` | `LR/nt` | `LR/lc` | `NE` | any case |
| it (`{{Tassobox}}`) | `statocons` | `statocons_versione` = `iucn3.1` / `iucn2.3`, lower case | `PE` / `PEW` | `CD` (or `LR/CD`) | `LR/NT` | `LR/LC` | `NE` | |
| pt (`{{Info/Taxonomia}}`) | `estado` | `sistema_estado` = `iucn3.1` / `iucn2.3`, lower case | `PE` / `PEW` | `LR/cd` | `LR/nt` | `LR/lc` | `NE` | bare `CD` and `LR` are invalid |
| pt, zh, uk, vi (`{{Speciesbox}}`; uk `{{Картка:Таксономія}}`, vi `{{Bảng phân loại}}`) | `status` | `status_system` = `IUCN3.1` / `IUCN2.3` | `PE` / `PEW` | `LR/cd` | `LR/nt` | `LR/lc` | `NE` | same lines as English; no `NA` |
| ja (`{{生物分類表}}`) | `status` | none for IUCN (version in the code: `VU2.3`, `EN2.3`, ...) | `PE` / `PEW` | `LR/cd` | `LR/nt` | `LR/lc` | `NE` | leave `status_system` out |
| nl (`{{Taxobox}}`) | `status` | none | – (`CR`) | `LR/CD` or `CD` | `LR/NT` or `NT` | `LR/LC` or `LC` | `NE` | `statusbron` (year), `rl-id` (taxon id) |
| pl (`{{Zwierzę infobox}}`, `{{Takson infobox}}`) | `status IUCN` | none | – (`CR`) | – | – | – | – | upper case only; `IUCN id` |
| ru (`{{Таксон}}`) | `iucnstatus` | letter case: `VU` is 3.1, `vu` is 2.3 | – (`CR`) | `cd` | `nt` | `lc` | – | empty means P141 from Wikidata |
| sv (`{{Taxobox}}`) | `status` | none | – (`cr`) | `lr/cd` | `lr/nt` | `lr/lc` | – | any case |
| ca (`{{Infotaula ésser viu}}`) | – | – | – | – | – | – | – | P141 from Wikidata only |
| de (`{{Taxobox}}`) | – | – | – | – | – | – | – | no conservation status |

## Citation templates side by side

| Wiki | Template (articles) | Taxon id | Assessment id / article number | DOI | Authors | Access date |
| --- | --- | --- | --- | --- | --- | --- |
| de | `{{IUCN}}` (16,419) | `ID` | `AssessmentID` | – | `Assessor` (one field) | `Abruf`, `YYYY-MM-DD` asked for |
| fr | `{{UICN}}` (33,360) | positional 1 | – | – | – | `consulté le`, French date |
| es | `{{IUCN}}` (21,768) | – (P627 of the article's item) | – | – | `asesores` (one field) | `consultado`, "9 de octubre de 2026" |
| it | `{{IUCN}}` (13,004) | `summ` (empty for global: P627) | `assessment` (with `summ`) | – | `autore` (one field) | `accesso` |
| pt | `{{citar iucn}}` (8,594) | from `página` | `página` / `page` | `.en` only | `autorn`, `últimon`/`primeiron`, English names | `acessodata` |
| ja | `{{Cite iucn}}` (1,393) | from `article-number` | `article-number` | en/es/fr/pt | CS1 | `access-date` |
| zh | `{{IUCN}}` (3,939) / `{{cite iucn}}` (4,912) | `id` / from `page` | `page` | any / `.en`, `.es` | `authorn` | `access-date`, ISO |
| uk | `{{Cite IUCN}}` (1,786) | from `page` | `page` (`сторінка`) | en/es/fr/pt | CS1 | `access-date`, Ukrainian date |
| pl | `{{IUCN}}` (5,213) | `id` | – | `doi` (replaces the URL) | `autor` (one field) | `data dostępu`, ISO in the doc |
| ru | `{{IUCN}}` (4,612) | positional 1 (else P627) | – | – | `author` | positional 3 |
| sv | `{{IUCN2025.1}}` etc. | `id` = `taxon/assessment` | in `id` | – | `assessors` (one field) | `hämtdatum` |
| vi | `{{Chú thích IUCN}}` (6,398) | from `page` | `page` | `.en` only | CS1 | `access-date` |
| nl | none (box `rl-id` makes a link) | | | | | |
| ca | none usable (`{{cite iucn}}` ignores its parameters) | | | | | |

## Summary

Article counts are from `meta=siteinfo&siprop=statistics` on each wiki, 9 October 2026 (e.g. [de](https://de.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=statistics)).

| Wiki | Articles | Taxobox status | IUCN citation template | Inline status template | Recommendation |
| --- | --- | --- | --- | --- | --- |
| de | 3,156,948 | no | `{{IUCN}}` | none | Citation only |
| fr | 2,783,663 | yes (`{{Taxobox UICN}}` line) | `{{UICN}}` (taxon id only) | none (`{{UICN VU}}` is a label) | **Support first** |
| sv | 2,628,482 | yes (no PE/NE, no system) | `{{IUCN<release>}}`, one per release | none | Later |
| nl | 2,228,810 | yes (`status`, `statusbron`, `rl-id`) | none | none | Taxobox lines only, later |
| es | 2,141,997 | yes (English codes) | `{{IUCN}}` (id from Wikidata) | none | **Support first** |
| ru | 2,121,464 | from Wikidata (local override) | `{{IUCN}}` (taxon id only) | none | Citation only, later |
| it | 1,990,050 | yes (`statocons`) | `{{IUCN}}` (prints 2020 / 2020.2 always) | none | Taxobox yes; citation needs a generic template |
| pl | 1,710,537 | yes (no PE, no LR) | `{{IUCN}}` | none | **Support first** |
| zh | 1,559,799 | yes (English lines) | `{{IUCN}}` and `{{cite iucn}}` | none | **Support first** |
| ja | 1,521,863 | yes (`VU2.3` style codes) | `{{Cite iucn}}` (English copy) | `{{IUCN status}}` (old copy, 20 articles) | **Support first** |
| uk | 1,437,067 | yes (English lines) | `{{Cite IUCN}}` (English copy, `page`) | `{{IUCN VU}}` badges, no link | **Support first** |
| vi | 1,304,951 | yes (English lines) | `{{Chú thích IUCN}}` (`page`, `.en` DOIs) | none | Later (cheap, as uk) |
| pt | 1,184,141 | yes (`estado`; Speciesbox also) | `{{citar iucn}}` (English copy, `página`) | `{{IUCN status}}` (old copy, 18 articles) | **Support first** |
| ca | 805,426 | from Wikidata only | none usable | none | No |

**Recommended first seven: fr, es, pl, zh, ja, uk, pt.** Each has both a taxobox status field and one IUCN citation template that carries the assessment, so the site can give the same three outputs it gives for English. Four of them (zh, ja, uk, pt) use copies of the English templates and module, so their output is the English output with a few parameter changes (`page`/`página` for `article-number`, DOI suffix limits, lower-case `iucn3.1` in pt's own box). fr, es and pl each need their own small renderer. Next: de (largest wiki, citation only, and `{{IUCN}}` takes the assessment id) and it (taxobox lines are easy, but `{{IUCN}}` always prints the 2020.2 edition).

Things that apply to several wikis:

- Of the 14 wikis checked, only pt and ja have an `{{IUCN status}}` (the two pages linked to the English template on Wikidata), both old copies used in about 20 articles each, with a taxon id but no assessment id or year. uk has code badges (`{{IUCN VU}}`) with no link or parameters. So the list-line output has no real equivalent outside English.
- Every wiki writes the reference with `<ref>`; the only reference-name conventions found are pl (`<ref name="iucn">` is picked up by the infobox) and nl (the box itself defines `name="IUCN"`).
- Where a wiki's doc and source disagree, the source was followed: sv's doc lists PE, NE and `status_system` but its box has none of them; pl's doc says lower case is accepted but its `#switch` is upper case only; es's doc omits PE/PEW, which its source accepts.
