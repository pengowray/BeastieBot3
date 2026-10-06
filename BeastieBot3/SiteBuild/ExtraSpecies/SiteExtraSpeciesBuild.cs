using BeastieBot3.CommonNames;

// The species of `site build-db` that are in the Catalogue of Life or Wikidata but are not IUCN
// taxa, for the group pages' lists (extra_species, extra_overlap, higher_taxon_extra and
// taxon_source_name in SiteDbSchema). Runs after the tree of groups is built and before the names
// are written, because the overlap rules read the taxa's synonym lists.
//
// Same taxon (no extra row):
//   CoL: the usage is an IUCN taxon's col_id (the placement file's species match), or has the name
//        of an IUCN species in the same kingdom.
//   Wikidata: the item is an IUCN taxon's item, states an IUCN taxon id (P627) of a taxon in the
//        database, states the CoL ID (P10585) of an IUCN taxon, or has the name of an IUCN species.
// A Wikidata item that states a CoL entry's id, or has its name, is merged into that entry.
//
// Placement: under the IUCN genus with the same name in the same kingdom, else (ExtraPlacement.Family)
// under the IUCN family of that name. A Wikidata item's kingdom and family come from its parent
// taxa; when the kingdom is unknown, the genus name must be used in one kingdom only.
//
// Overlaps (possible same species not linked by an id): see OverlapReason. Each extra entry is
// compared with the IUCN species and, for the name tests, with the extra entries from the other
// source; entries from the same source are not compared with each other.

namespace BeastieBot3.SiteBuild.ExtraSpecies;

internal sealed class SiteExtraSpeciesBuild {
    public List<ExtraEntry> Entries { get; } = new();
    public List<ExtraOverlap> Overlaps { get; } = new();
    public List<TaxonSourceName> SourceNames { get; } = new();
    /// For each group with extra entries under it: its last descendant node id, and the entries under
    /// it by source (CoL only, Wikidata only, both).
    public Dictionary<int, (int LastNodeId, int Col, int Wikidata, int Both)> NodeCounts { get; } = new();
    /// The entries under each group by sources (1 CoL, 2 Wikidata, 3 both), placement under a family
    /// rather than a genus, and a likely overlap with an IUCN taxon (higher_taxon_extra_count).
    public Dictionary<(int NodeId, int Sources, bool UnderFamily, bool IucnLikely), int> SplitCounts { get; } = new();
    public ExtraSpeciesStats Stats { get; } = new();
    public List<string> Warnings { get; } = new();

    private readonly Dictionary<(string Kingdom, string Genus), SiteTreeNode> _genusNodes = new();
    private readonly Dictionary<string, HashSet<string>> _genusKingdoms = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Kingdom, string Family), SiteTreeNode> _familyNodes = new();
    private readonly Dictionary<(string Kingdom, string Name), SiteTaxon> _iucnSpecies = new();
    private readonly Dictionary<string, SiteTaxon> _iucnByColId = new(StringComparer.Ordinal);
    private readonly Dictionary<string, SiteTaxon> _iucnByQid = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ExtraEntry> _byColId = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Kingdom, string Name), ExtraEntry> _byName = new();

    public static SiteExtraSpeciesBuild Run(IReadOnlyList<SiteTaxon> taxonList, IReadOnlyDictionary<long, SiteTaxon> taxa, SiteTaxonTree tree,
        ExtraPlacement placement, string? colDatabase, string? colPlacement, string? wikidataCache, Action<string> progress, CancellationToken ct) {
        var build = new SiteExtraSpeciesBuild();
        build.Stats.Placement = placement;
        if (placement == ExtraPlacement.None) {
            return build;
        }
        build.Index(taxonList, tree);

        // Wikidata is read first, for the fossil species it marks; the CoL rows go in first, so a
        // Wikidata item can merge into its CoL species.
        var hasCol = colDatabase is not null && File.Exists(colDatabase);
        var wikidataOnly = new List<(ExtraEntry Entry, IReadOnlyList<string> ColIds)>();
        var fossils = (Names: new HashSet<string>(StringComparer.Ordinal), ColIds: new HashSet<string>(StringComparer.Ordinal));
        List<WikidataSpeciesRow>? wikidataRows = null;
        if (wikidataCache is not null && File.Exists(wikidataCache)) {
            if (ExtraSpeciesWikidataReader.HasSweep(wikidataCache, out var finished, out var warning)) {
                build.Stats.WikidataSweepFinished = finished;
                progress("Wikidata taxon sweep: species items");
                var genusFilter = build._genusNodes.Keys.Select(k => k.Genus).ToHashSet(StringComparer.Ordinal);
                var familyFilter = placement == ExtraPlacement.Family
                    ? build._familyNodes.Keys.Select(k => k.Family).ToHashSet(StringComparer.Ordinal)
                    : new HashSet<string>();
                wikidataRows = ExtraSpeciesWikidataReader.Read(wikidataCache, genusFilter, familyFilter, build.Stats, fossils, ct);
            }
            if (warning is not null) {
                build.Warnings.Add(warning);
            }
        }

        if (hasCol) {
            progress("Catalogue of Life: accepted species of the IUCN genera" + (placement == ExtraPlacement.Family ? " and families" : string.Empty));
            var genera = build._genusNodes.Keys.Select(k => k.Genus).Distinct(StringComparer.Ordinal).ToList();
            var families = placement == ExtraPlacement.Family
                ? build._familyNodes.Keys.Select(k => SiteTaxonTree.TitleCase(k.Family)).Distinct(StringComparer.Ordinal).ToList()
                : new List<string>();
            build.AddCol(ExtraSpeciesColReader.ReadSpecies(colDatabase!, genera, families, build.Stats, ct), placement, fossils);
        } else {
            build.Warnings.Add("The Catalogue of Life database was not found, so no species from the Catalogue of Life were added.");
        }
        if (wikidataRows is not null) {
            build.AddWikidata(wikidataRows, taxa, placement, wikidataOnly);
        }

        if (hasCol) {
            progress("Catalogue of Life: synonyms");
            var names = wikidataOnly.Select(w => w.Entry.Name).Concat(build._iucnSpecies.Keys.Select(k => k.Name)).ToHashSet(StringComparer.Ordinal);
            var ids = wikidataOnly.SelectMany(w => w.ColIds).ToHashSet(StringComparer.Ordinal);
            var (byName, byId) = ExtraSpeciesColReader.ReadSynonyms(colDatabase!, names, ids, build.Stats, ct);
            build.AddColSynonymOverlaps(wikidataOnly, byName, byId);
            if (colPlacement is not null && File.Exists(colPlacement)) {
                build.AddColNamesOfIucnTaxa(colDatabase!, colPlacement, ct);
            }
        }
        progress("Comparing names");
        build.AddWikidataSynonymOverlaps(wikidataOnly);
        build.AddSynonymListOverlaps();
        build.AddNameOverlaps();
        build.SetPositions(taxonList);
        build.CountNodes(tree);
        build.Stats.TaxonSourceNames = build.SourceNames.Count;
        foreach (var overlap in build.Overlaps) {
            build.Stats.OverlapsByReason[overlap.Reason] = build.Stats.OverlapsByReason.GetValueOrDefault(overlap.Reason) + 1;
        }
        return build;
    }

    // ------------------------------------------------------------ indexes

    private void Index(IReadOnlyList<SiteTaxon> taxonList, SiteTaxonTree tree) {
        foreach (var node in tree.Nodes) {
            _nodeById[node.NodeId] = node;
            if (node.Rank == "genus") {
                _genusNodes.TryAdd((node.Kingdom, node.Name), node);
                if (!_genusKingdoms.TryGetValue(node.Name, out var kingdoms)) {
                    _genusKingdoms[node.Name] = kingdoms = new HashSet<string>(StringComparer.Ordinal);
                }
                kingdoms.Add(node.Kingdom);
            } else if (node.Rank == "family") {
                _familyNodes.TryAdd((node.Kingdom, node.Name.ToUpperInvariant()), node);
            }
        }
        // Taxa in the release first, so a name or id maps to the current taxon.
        foreach (var taxon in taxonList.OrderBy(t => t.InRelease ? 0 : 1).ThenBy(t => t.TaxonId)) {
            if (taxon.Kind != SiteTaxonKind.Species) {
                continue;
            }
            if (taxon.InRelease && taxon.Kingdom is { } kingdom) {
                _iucnSpecies.TryAdd((kingdom.ToUpperInvariant(), taxon.ScientificName), taxon);
            }
            if (taxon.ColId is { } colId) {
                _iucnByColId.TryAdd(colId, taxon);
            }
            if (taxon.WikidataQid is { } qid) {
                _iucnByQid.TryAdd(qid, taxon);
            }
        }
    }

    private static SiteTreeNode? FamilyAncestor(SiteTreeNode node) {
        for (var at = node; at is not null; at = at.Parent) {
            if (at.Rank == "family") {
                return at;
            }
        }
        return null;
    }

    private SiteTreeNode? Place(string kingdom, string genus, string? family, ExtraPlacement placement, out bool underGenus) {
        underGenus = false;
        if (_genusNodes.TryGetValue((kingdom, genus), out var genusNode)) {
            underGenus = true;
            return genusNode;
        }
        if (placement == ExtraPlacement.Family && family is not null
            && _familyNodes.TryGetValue((kingdom, family.ToUpperInvariant()), out var familyNode)) {
            return familyNode;
        }
        return null;
    }

    // ------------------------------------------------------------ Catalogue of Life

    private void AddCol(List<ColSpeciesRow> rows, ExtraPlacement placement, (HashSet<string> Names, HashSet<string> ColIds) fossils) {
        foreach (var row in rows.OrderBy(r => r.Genus, StringComparer.Ordinal).ThenBy(r => r.Epithet, StringComparer.Ordinal)) {
            var name = row.Genus + " " + row.Epithet;
            if (fossils.ColIds.Contains(row.Id) || fossils.Names.Contains(name)) {
                Stats.ColFossilOnWikidata++;
                continue;
            }
            if (_iucnByColId.ContainsKey(row.Id) || _iucnSpecies.ContainsKey((row.Kingdom, name))) {
                Stats.ColSameAsIucn++;
                continue;
            }
            if (_byName.ContainsKey((row.Kingdom, name))) {
                continue; // CoL has two accepted usages with one name; the first is kept.
            }
            var node = Place(row.Kingdom, row.Genus, row.Family, placement, out var underGenus);
            if (node is null) {
                continue;
            }
            var entry = new ExtraEntry {
                ColId = row.Id, Genus = row.Genus, Epithet = row.Epithet, Authority = row.Authority, Kingdom = row.Kingdom,
                Node = node, FamilyNode = FamilyAncestor(node),
            };
            Add(entry, underGenus);
        }
    }

    private void Add(ExtraEntry entry, bool underGenus) {
        Entries.Add(entry);
        if (entry.ColId is { } colId) {
            _byColId[colId] = entry;
        }
        _byName[(entry.Kingdom, entry.Name)] = entry;
        if (underGenus) {
            Stats.PlacedUnderGenus++;
        } else {
            Stats.PlacedUnderFamily++;
        }
    }

    // ------------------------------------------------------------ Wikidata

    private void AddWikidata(List<WikidataSpeciesRow> rows, IReadOnlyDictionary<long, SiteTaxon> taxa, ExtraPlacement placement,
        List<(ExtraEntry, IReadOnlyList<string>)> wikidataOnly) {
        foreach (var row in rows.OrderBy(r => r.Qid)) {
            var qid = "Q" + row.Qid;
            var name = row.Genus + " " + row.Epithet;
            var iucn = SameIucnTaxon(row, qid, taxa);
            var kingdom = row.Kingdom ?? iucn?.Kingdom?.ToUpperInvariant()
                ?? (_genusKingdoms.TryGetValue(row.Genus, out var kingdoms) && kingdoms.Count == 1 ? kingdoms.First() : null);
            if (iucn is null && kingdom is not null) {
                iucn = _iucnSpecies.GetValueOrDefault((kingdom, name));
            }
            if (iucn is not null) {
                Stats.WikidataSameAsIucn++;
                if (iucn.InRelease && !string.Equals(iucn.ScientificName, name, StringComparison.Ordinal)
                    && !SourceNames.Any(s => s.TaxonId == iucn.TaxonId && s.Source == "wikidata")) {
                    SourceNames.Add(new TaxonSourceName(iucn.TaxonId, "wikidata", name));
                }
                continue;
            }
            if (kingdom is null) {
                Stats.WikidataAmbiguousKingdom++;
                continue;
            }
            var col = row.ColIds.Select(id => _byColId.GetValueOrDefault(id)).FirstOrDefault(e => e is not null)
                ?? _byName.GetValueOrDefault((kingdom, name));
            if (col is not null) {
                if (col.Qid is not null) {
                    Stats.WikidataSameNameRepeat++;
                    continue;
                }
                col.Qid = row.Qid;
                if (!string.Equals(col.Name, name, StringComparison.Ordinal)) {
                    col.WikidataName = name;
                }
                SetNames(col, row);
                Stats.WikidataMergedWithCol++;
                continue;
            }
            var node = Place(kingdom, row.Genus, row.Family, placement, out var underGenus);
            if (node is null) {
                Stats.WikidataUnplaced++;
                continue;
            }
            var entry = new ExtraEntry {
                Qid = row.Qid, Genus = row.Genus, Epithet = row.Epithet, Kingdom = kingdom, Node = node, FamilyNode = FamilyAncestor(node),
            };
            SetNames(entry, row);
            Add(entry, underGenus);
            wikidataOnly.Add((entry, row.ColIds));
            if (row.SynonymOf.Count > 0) {
                _synonymOf[entry] = row.SynonymOf;
            }
        }
    }

    private SiteTaxon? SameIucnTaxon(WikidataSpeciesRow row, string qid, IReadOnlyDictionary<long, SiteTaxon> taxa) {
        if (_iucnByQid.TryGetValue(qid, out var byQid)) {
            return byQid;
        }
        foreach (var id in row.IucnTaxonIds) {
            if (taxa.TryGetValue(id, out var taxon)) {
                if (taxon.InRelease) {
                    return taxon;
                }
                if (taxon.CurrentTaxonId is { } current && taxa.TryGetValue(current, out var currentTaxon)) {
                    return currentTaxon;
                }
            }
        }
        foreach (var colId in row.ColIds) {
            if (_iucnByColId.TryGetValue(colId, out var byCol)) {
                return byCol;
            }
        }
        return null;
    }

    // English name: the item's English label when it is not a taxon name and not junk; article: the
    // item's English Wikipedia sitelink.
    private void SetNames(ExtraEntry entry, WikidataSpeciesRow row) {
        if (entry.EnwikiTitle is null && row.EnwikiTitle is { } title) {
            entry.EnwikiTitle = title;
            Stats.Articles++;
        }
        if (entry.CommonNameEn is null && row.LabelEn is { } label && CommonNameEn(label, entry) is { } common) {
            entry.CommonNameEn = common;
            Stats.CommonNames++;
        }
    }

    internal static string? CommonNameEn(string label, ExtraEntry entry) {
        var verdict = CommonNameQuality.Assess(label);
        if (verdict.IsJunk) {
            return null;
        }
        var name = verdict.Name.Trim();
        if (name.Length < 3 || string.Equals(name, entry.Name, StringComparison.OrdinalIgnoreCase)
            || string.Equals(name, entry.Genus, StringComparison.OrdinalIgnoreCase)
            || name.Contains('(') || name.Any(char.IsDigit)) {
            return null;
        }
        return char.ToUpperInvariant(name[0]) + name[1..];
    }

    // ------------------------------------------------------------ overlaps

    private readonly HashSet<(ExtraEntry, long?, ExtraEntry?)> _overlapKeys = new();

    private void AddOverlap(ExtraEntry entry, long? taxonId, ExtraEntry? other, string reason) {
        // One row per pair; the reasons are added in order, likely ones first.
        if (other is not null && _overlapKeys.Contains((other, null, entry))) {
            return;
        }
        if (_overlapKeys.Add((entry, taxonId, other))) {
            Overlaps.Add(new ExtraOverlap(entry, taxonId, other, reason));
        }
    }

    private void AddColSynonymOverlaps(List<(ExtraEntry Entry, IReadOnlyList<string> ColIds)> wikidataOnly,
        Dictionary<string, List<string>> byName, Dictionary<string, string> byId) {
        void Link(ExtraEntry entry, string acceptedId) {
            if (_iucnByColId.TryGetValue(acceptedId, out var taxon) && taxon.InRelease) {
                AddOverlap(entry, taxon.TaxonId, null, OverlapReason.ColSynonym);
            } else if (_byColId.TryGetValue(acceptedId, out var other) && other != entry) {
                AddOverlap(entry, null, other, OverlapReason.ColSynonym);
            }
        }
        foreach (var (entry, colIds) in wikidataOnly) {
            if (entry.ColId is not null) {
                continue;
            }
            foreach (var id in colIds) {
                if (byId.TryGetValue(id, out var accepted)) {
                    Link(entry, accepted);
                }
            }
            foreach (var accepted in byName.GetValueOrDefault(entry.Name) ?? []) {
                Link(entry, accepted);
            }
        }
        // An IUCN species whose name CoL lists as a synonym of a CoL entry that IUCN does not have.
        foreach (var ((kingdom, name), taxon) in _iucnSpecies) {
            foreach (var accepted in byName.GetValueOrDefault(name) ?? []) {
                if (_byColId.TryGetValue(accepted, out var entry) && entry.Kingdom == kingdom) {
                    AddOverlap(entry, taxon.TaxonId, null, OverlapReason.ColSynonym);
                }
            }
        }
    }

    private readonly Dictionary<ExtraEntry, IReadOnlyList<long>> _synonymOf = new();

    // A Wikidata-only entry whose item another item names as taxon synonym (P1420): that item's IUCN
    // taxon, or its extra entry.
    private void AddWikidataSynonymOverlaps(List<(ExtraEntry Entry, IReadOnlyList<string> ColIds)> wikidataOnly) {
        var byQid = Entries.Where(e => e.Qid is not null).GroupBy(e => e.Qid!.Value).ToDictionary(g => g.Key, g => g.First());
        foreach (var (entry, _) in wikidataOnly) {
            foreach (var qid in _synonymOf.GetValueOrDefault(entry) ?? []) {
                if (_iucnByQid.TryGetValue("Q" + qid, out var taxon) && taxon.InRelease) {
                    AddOverlap(entry, taxon.TaxonId, null, OverlapReason.WikidataSynonym);
                } else if (byQid.TryGetValue(qid, out var other) && other != entry) {
                    AddOverlap(entry, null, other, OverlapReason.WikidataSynonym);
                }
            }
        }
    }

    // The entry's name is in an IUCN species' synonym lists (IUCN, CoL or Wikidata).
    private void AddSynonymListOverlaps() {
        var index = new Dictionary<(string Kingdom, string Name), List<(SiteTaxon Taxon, string Reason)>>();
        void Index(SiteTaxon taxon, IEnumerable<SiteSynonym> synonyms, string reason) {
            foreach (var synonym in synonyms) {
                if (ExtraSpeciesNameRules.SplitBinomial(synonym.Name) is not { } split) {
                    continue;
                }
                var key = (taxon.Kingdom!.ToUpperInvariant(), split.Genus + " " + split.Epithet);
                if (!index.TryGetValue(key, out var list)) {
                    index[key] = list = new List<(SiteTaxon, string)>();
                }
                list.Add((taxon, reason));
            }
        }
        foreach (var taxon in _iucnSpecies.Values) {
            Index(taxon, taxon.IucnSynonyms, OverlapReason.IucnSynonym);
            Index(taxon, taxon.ColSynonyms, OverlapReason.ColSynonym);
            Index(taxon, taxon.WikidataSynonyms, OverlapReason.WikidataSynonym);
        }
        foreach (var entry in Entries) {
            foreach (var name in new[] { entry.Name, entry.WikidataName }) {
                if (name is null || !index.TryGetValue((entry.Kingdom, name), out var matches)) {
                    continue;
                }
                foreach (var (taxon, reason) in matches) {
                    AddOverlap(entry, taxon.TaxonId, null, reason);
                }
            }
        }
    }

    // Gender endings and spelling in the same genus; the same epithet in another genus of the family.
    private void AddNameOverlaps() {
        var iucnByGenus = _iucnSpecies.Values.Where(t => t.Genus is not null && t.SpeciesEpithet is not null)
            .ToLookup(t => (t.Kingdom!.ToUpperInvariant(), t.Genus!));
        var entriesByGenus = Entries.ToLookup(e => (e.Kingdom, e.Genus));
        foreach (var entry in Entries) {
            foreach (var taxon in iucnByGenus[(entry.Kingdom, entry.Genus)]) {
                if (NameReason(entry.Epithet, taxon.SpeciesEpithet!) is { } reason) {
                    AddOverlap(entry, taxon.TaxonId, null, reason);
                }
            }
            // Entries from the other source only: CoL-only against Wikidata-only.
            if (entry.ColId is not null && entry.Qid is null) {
                foreach (var other in entriesByGenus[(entry.Kingdom, entry.Genus)]) {
                    if (other.ColId is null && NameReason(entry.Epithet, other.Epithet) is { } reason) {
                        AddOverlap(entry, null, other, reason);
                    }
                }
            }
        }

        // Same epithet, other genus, same family.
        var familyOf = new Dictionary<int, SiteTreeNode?>();
        SiteTreeNode? FamilyOfNode(SiteTreeNode node) {
            if (!familyOf.TryGetValue(node.NodeId, out var family)) {
                familyOf[node.NodeId] = family = FamilyAncestor(node);
            }
            return family;
        }
        var iucnByFamilyEpithet = new Dictionary<(int Family, string Epithet), List<SiteTaxon>>();
        foreach (var taxon in _iucnSpecies.Values) {
            if (taxon.NodeId is not { } nodeId || taxon.SpeciesEpithet is null || !_nodeById.TryGetValue(nodeId, out var node)
                || FamilyOfNode(node) is not { } family) {
                continue;
            }
            var key = (family.NodeId, taxon.SpeciesEpithet);
            if (!iucnByFamilyEpithet.TryGetValue(key, out var list)) {
                iucnByFamilyEpithet[key] = list = new List<SiteTaxon>();
            }
            list.Add(taxon);
        }
        var entryEpithetCount = Entries.Where(e => e.FamilyNode is not null)
            .GroupBy(e => (e.FamilyNode!.NodeId, e.Epithet)).ToDictionary(g => g.Key, g => g.Count());
        foreach (var entry in Entries) {
            if (entry.FamilyNode is null || !iucnByFamilyEpithet.TryGetValue((entry.FamilyNode.NodeId, entry.Epithet), out var taxa)) {
                continue;
            }
            var others = taxa.Where(t => !string.Equals(t.Genus, entry.Genus, StringComparison.Ordinal)).ToList();
            foreach (var taxon in others) {
                var sameAuthor = ExtraSpeciesNameRules.SameAuthority(entry.Authority, taxon.Authority);
                var unique = entry.Authority is null && taxa.Count == 1
                    && entryEpithetCount.GetValueOrDefault((entry.FamilyNode.NodeId, entry.Epithet)) == 1;
                if (sameAuthor || unique) {
                    AddOverlap(entry, taxon.TaxonId, null, OverlapReason.OtherGenus);
                }
            }
        }
    }

    private static string? NameReason(string a, string b) =>
        ExtraSpeciesNameRules.IsGenderVariant(a, b) ? OverlapReason.GenderEnding
        : ExtraSpeciesNameRules.IsSpellingVariant(a, b) ? OverlapReason.Spelling
        : null;

    // ------------------------------------------------------------ CoL names of IUCN taxa

    private void AddColNamesOfIucnTaxa(string colDatabase, string colPlacement, CancellationToken ct) {
        var matches = ExtraSpeciesColReader.ReadSynonymMatches(colPlacement);
        var wanted = new List<(SiteTaxon Taxon, string AcceptedId)>();
        foreach (var (kingdom, genus, species, acceptedId) in matches) {
            if (_iucnSpecies.TryGetValue((kingdom.ToUpperInvariant(), genus + " " + species), out var taxon)) {
                wanted.Add((taxon, acceptedId));
            }
        }
        var names = ExtraSpeciesColReader.ReadNames(colDatabase, wanted.Select(w => w.AcceptedId).Distinct(), ct);
        foreach (var (taxon, acceptedId) in wanted) {
            if (names.TryGetValue(acceptedId, out var name) && !string.Equals(name, taxon.ScientificName, StringComparison.Ordinal)) {
                SourceNames.Add(new TaxonSourceName(taxon.TaxonId, "col", name));
            }
        }
        Stats.TaxonSourceNames = SourceNames.Count;
    }

    // ------------------------------------------------------------ positions and counts

    private readonly Dictionary<int, SiteTreeNode> _nodeById = new();

    // An entry under a genus sorts among the genus's species by name, after the species before it
    // (and that species' subspecies); an entry under a family sorts after the family's taxa.
    private void SetPositions(IReadOnlyList<SiteTaxon> taxonList) {
        var blocksByNode = new Dictionary<int, List<(string Name, int LastPos)>>();
        foreach (var group in taxonList.Where(t => t.InRelease && t.NodeId is not null && t.TreePos is not null)
                     .GroupBy(t => t.NodeId!.Value)) {
            var ordered = group.OrderBy(t => t.TreePos).ToList();
            var speciesHere = ordered.Where(t => t.Kind == SiteTaxonKind.Species).Select(t => t.TaxonId).ToHashSet();
            var blocks = new List<(string Name, int LastPos)>();
            foreach (var taxon in ordered) {
                var child = taxon.Kind != SiteTaxonKind.Species && taxon.ParentTaxonId is { } p && speciesHere.Contains(p);
                if (child && blocks.Count > 0) {
                    blocks[^1] = (blocks[^1].Name, taxon.TreePos!.Value);
                } else {
                    blocks.Add((taxon.ScientificName, taxon.TreePos!.Value));
                }
            }
            blocksByNode[group.Key] = blocks;
        }
        foreach (var entry in Entries) {
            if (entry.Node.Rank != "genus") {
                entry.SortPos = entry.Node.LastPos;
                continue;
            }
            var pos = entry.Node.FirstPos - 1;
            foreach (var (name, lastPos) in blocksByNode.GetValueOrDefault(entry.Node.NodeId) ?? []) {
                if (string.Compare(name, entry.Name, StringComparison.OrdinalIgnoreCase) > 0) {
                    break;
                }
                pos = lastPos;
            }
            entry.SortPos = pos;
        }
        Entries.Sort((a, b) => a.SortPos != b.SortPos
            ? a.SortPos.CompareTo(b.SortPos)
            : StringComparer.OrdinalIgnoreCase.Compare(a.Name, b.Name) is var byName and not 0 ? byName
            : StringComparer.Ordinal.Compare(a.Name, b.Name));
        for (var i = 0; i < Entries.Count; i++) {
            Entries[i].ExtraId = i + 1;
        }
    }

    private void CountNodes(SiteTaxonTree tree) {
        var counts = new Dictionary<int, int[]>();
        var iucnLikely = Overlaps.Where(o => o.Likely && o.TaxonId is not null).Select(o => o.Entry).ToHashSet();
        foreach (var entry in Entries) {
            var slot = entry.ColId is not null && entry.Qid is not null ? 2 : entry.ColId is not null ? 0 : 1;
            var split = (Sources: slot + 1, UnderFamily: entry.Node.Rank != "genus", IucnLikely: iucnLikely.Contains(entry));
            for (var at = entry.Node; at is not null; at = at.Parent) {
                var key = (at.NodeId, split.Sources, split.UnderFamily, split.IucnLikely);
                SplitCounts[key] = SplitCounts.GetValueOrDefault(key) + 1;
            }
            switch (slot) {
                case 0: Stats.ColEntries++; break;
                case 1: Stats.WikidataEntries++; break;
                default: Stats.BothEntries++; break;
            }
            for (var at = entry.Node; at is not null; at = at.Parent) {
                if (!counts.TryGetValue(at.NodeId, out var c)) {
                    counts[at.NodeId] = c = new int[3];
                }
                c[slot]++;
            }
        }
        // Nodes are numbered depth-first, so a node's descendants have the ids after it up to the
        // last one in its subtree.
        var last = tree.Nodes.ToDictionary(n => n.NodeId, n => n.NodeId);
        for (var i = tree.Nodes.Count - 1; i >= 0; i--) {
            var node = tree.Nodes[i];
            if (node.Parent is { } parent && last[node.NodeId] > last[parent.NodeId]) {
                last[parent.NodeId] = last[node.NodeId];
            }
        }
        foreach (var (nodeId, c) in counts) {
            NodeCounts[nodeId] = (last[nodeId], c[0], c[1], c[2]);
        }
    }
}
