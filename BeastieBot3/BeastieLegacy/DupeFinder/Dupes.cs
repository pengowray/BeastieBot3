using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace beastie {
    public class Dupes {
    public Dictionary<string, List<IUCNBitri>> allFoundNames = new Dictionary<string, List<IUCNBitri>>(); // <comparison string, list of matching>
    public Dictionary<string, string> dupes = new Dictionary<string, string>(); // output: <comparison string, example of non-mangled string>

    public Dupes? alsoMatch; // Dictionary<string, List<IUCNBitri>> alsoMatchThese = null

        // taxons which share a common name
    public static Dupes FindByCommonNames(IEnumerable<IUCNBitri> bitris, Dupes? alsoMatch = null) {
            // Dictionary<string, List<IUCNBitri>> alsoMatchThese = null) {
            //FindDupes(bitris, alsoMatchThese, AllCommonNamesNormalizer);

            Dupes dupes = new Dupes();
            dupes.alsoMatch = alsoMatch;
            dupes.FindDupes(bitris, AllCommonNamesNormalizerDictionary);

            return dupes;
        }

        // taxons which lead to the same page on Wikipedia
    public static Dupes FindWikiAmbiguous(IEnumerable<IUCNBitri> bitris, Dupes? alsoMatch = null) {
            // Dictionary<string, List<IUCNBitri>> alsoMatchThese = null) {
            //FindDupes(bitris, alsoMatchThese, AllCommonNamesNormalizer);

            Dupes dupes = new Dupes();
            dupes.alsoMatch = alsoMatch;
            dupes.FindDupes(bitris, WikiPageNameNormalizer);
            dupes.SortBestMatchFirst();

            return dupes;
        }

        public void SortBestMatchFirst() {
            // For all dupes, sort their bitris lists (in "allFoundNames") so the first item is an "exact" match or the best possible match.
            // an "exact" match is where the title matches the bitri, or otherwise the taxobox matches it
            //foreach (var dupe in dupes) {
            foreach (var dupe in dupes.OrderBy(e => e.Value)) {
                if (!allFoundNames.TryGetValue(dupe.Key, out var list) || list.Count == 0) {
                    continue;
                }

                list.Sort((a, b) => StringComparer.InvariantCulture.Compare(a.BasicName(), b.BasicName())); // first sort by basic name

                int matchIndex = -1;
                for (int i = 0; i < list.Count; i++) {
                    var bitri = list[i];
                    if (bitri.BasicName() == dupe.Key) {
                        // page name is the same as a bitri
                        matchIndex = i;
                        break;
                    }
                }

                if (matchIndex == -1) {
                    for (int i = 0; i < list.Count; i++) {
                        var bitri = list[i];
                        var taxonName = bitri.TaxonName();
                            // check against taxobox taxon
                            string taxoboxName = taxonName.taxonField;
                        string basicName = bitri.BasicName();
                            //Console.WriteLine("Taxobox name: " + taxoboxName + ".. vs basic name:" + basicName );
                            if (taxoboxName != null && taxoboxName.Contains(basicName)) {
                                // matches scientific name used in taxobox 
                                //Console.WriteLine("Found within");
                                // TODO: note that some badly formatted taxoboxes contain have multiple scientific names.. watch out for these.
                            matchIndex = i;
                            break;
                        }
                    }
                }

                if (matchIndex == -1) {
                    for (int i = 0; i < list.Count; i++) {
                        var bitri = list[i];
                        var taxonName = bitri.TaxonName();
                        string? taxoboxName = taxonName.taxonField;
                        string epithet = bitri.epithet;
                        if (!string.IsNullOrEmpty(taxoboxName) && taxoboxName.Contains(epithet, StringComparison.Ordinal)) {
                            // partially matches scientific name used in taxobox 
                            //TODO: consider trinomials better?
                            matchIndex = i;
                            break;
                        }
                    }
                }

                if (matchIndex >= 0 && matchIndex < list.Count) {
                    var item = list[matchIndex];
                    list.RemoveAt(matchIndex);
                    list.Insert(0, item);
                    //Console.WriteLine("Moved item to front for: " + dupe.Key + " item: " + item.BasicName());
                } else {
                    //Console.WriteLine("No best match: " + dupe.Key);
                }
}
            }
        }

        //static IEnumerable<Tuple<string, string>> AllCommonNamesNormalizer(IUCNBitri bitri) { // Tuple: <normalized, example string>
        static Dictionary<string, string> AllCommonNamesNormalizerDictionary(IUCNBitri bitri) {

            Dictionary<string, string> newNames = new Dictionary<string, string>(); // <normalized name, an example of non-normalized name>
            string? exampleName = bitri.TaxonName().CommonName(false);
            if (exampleName != null)
                newNames[exampleName.NormalizeForComparison()] = exampleName;

            // get other common names from iucn red list
            string[]? iucnNames = bitri.CommonNamesEng();
            if (iucnNames != null) {
                foreach (string name in iucnNames) {
                    string norm = name.NormalizeForComparison();
                    if (norm == string.Empty)
                        continue;

                    newNames[norm] = name;
                }
            }

            return newNames;
        }

        //unused (doesn't give example string)
        static IEnumerable<string> AllCommonNamesNormalizer(IUCNBitri bitri) {

            HashSet<string> newNames = new HashSet<string>();
            string? exampleName = bitri.TaxonName().CommonName(false);
            if (exampleName != null) {
                string normalized = exampleName.NormalizeForComparison();
                if (!string.IsNullOrEmpty(normalized)) {
                    newNames.Add(normalized);
                }
            }

            // get other common names from iucn red list
            string[]? iucnNames = bitri.CommonNamesEng();
            if (iucnNames != null) {
                foreach (string name in iucnNames) {
                    string norm = name.NormalizeForComparison();
                    if (norm == string.Empty)
                        continue;

                    newNames.Add(norm);
                }
            }

            foreach (string name in newNames) {
                yield return name;
            }
        }

        //static IEnumerable<string> F(IUCNBitri bitri) {
        static Dictionary<string, string> WikiPageNameNormalizer(IUCNBitri bitri) {

            Dictionary<string, string> newNames = new Dictionary<string, string>(); // <normalized name, an example of non-normalized name>

            //string exampleName = bitri.TaxonName().CommonName(false);
            string pageTitle = bitri.TaxonName().pageTitle;

            if (!string.IsNullOrEmpty(pageTitle)) {
                //newNames[exampleName.NormalizeForComparison()] = exampleName;
                newNames[pageTitle.UpperCaseFirstChar()] = pageTitle;
            }

            return newNames;
        }

        public void FindDupes (
                IEnumerable<IUCNBitri> bitris,
                Func<IUCNBitri, Dictionary<string, string>> normalizer,
                bool showProgress = false
                /*
                out Dictionary<string, List<IUCNBitri>> allFoundNames, // output: <comparison string, list of matching>
                                                                       //out Dictionary<string, string> dupes, // output: <comparison string, example name>
                out HashSet<string> dupes,
                Dictionary<string, List<IUCNBitri>> alsoMatchThese,
                Func<IUCNBitri, IEnumerable<string>> normalizer,
                //Func<IUCNBitri, Dictionary<string, string>> normalizer, 
                */
                ) { // normalized string or wikipage name

            //dupes = new HashSet<string>(); // normalized strings

            //Func<IUCNBitri, string> getComparisonString;
            Dictionary<string, List<IUCNBitri>>? alsoMatchThese = alsoMatch?.allFoundNames;

            foreach (IUCNBitri bitri in bitris) {

                // e.g. find if they conflict with other binoms or trinoms
                foreach (var item in normalizer(bitri)) {
                    //string normalized = item;
                    string normalized = item.Key;
                    string example = item.Value;

                    if (allFoundNames.TryGetValue(normalized, out var currentList)) {

                        // conflict found
                        dupes[normalized] = example;
                        //dupes.Add(normalized);
                        currentList.Add(bitri);
                        if (showProgress) {
                            Console.WriteLine("... Dupe found (normal): {0}. {2} & {3}",
                                normalized, bitri.FullDebugName(), currentList[0].FullDebugName());

                        }

                    } else {

                        // e.g. novel trinomial common name ...
                        var newList = new List<IUCNBitri> { bitri };
                        allFoundNames[normalized] = newList;

                        // but it is it used for a binomial's common name?
                        if (alsoMatchThese != null && alsoMatchThese.TryGetValue(normalized, out var matches) && matches.Count > 0) {
                            // duplicate
                            dupes[normalized] = example;
                            if (showProgress) {
                                Console.WriteLine("... Dupe found (also match): {1}. {2} & {3}",
                                    normalized, example, bitri.FullDebugName(), matches[0].FullDebugName());
                            }
                        }
                    }
                }
            }

            //return dupes;

        }

        public void ExportWithBitris(TextWriter output, string keyword = "dupe", Dupes? alsoShow = null, bool wikiize = false) {

            alsoShow ??= alsoMatch; // default to showing these too.. TODO: really should show both

            Dictionary<string, List<IUCNBitri>>? alsoShowThese = alsoShow?.allFoundNames;

            foreach (var dupeEntry in dupes.OrderBy(d => d.Value)) { // .OrderBy(e => e.Value) // already sorted via SortBestMatchFirst()
                string dupeNomralized = dupeEntry.Key;
                string dupeExampleName = dupeEntry.Value;

                //Console.WriteLine(dupe);
                allFoundNames.TryGetValue(dupeNomralized, out var biList);
                List<IUCNBitri>? triList = null;
                bool isTrinom = alsoShowThese != null && alsoShowThese.TryGetValue(dupeNomralized, out triList);
                bool isBinom = biList != null;

                string format = wikiize
                    ? "# [[{0}]] {1} ''{2}{3}{4}'' "
                    : "{0} {1} {2}{3}{4}";

                string listString = string.Format(format,
                    dupeExampleName,
                    keyword, // dupeNomralized,
                    (isBinom && biList != null ? biList.Select(bt => bt.FullDebugName()).JoinStrings(", ") : ""),
                    (isBinom && isTrinom ? ", " : ""),
                    (isTrinom && triList != null ? triList.Select(bt => bt.FullDebugName()).JoinStrings(", ") : "")
                    );

                output.WriteLine(listString);
            }

        }

        public void SplitSpeciesSspLevel(out Dupes SpeciesDupes, out Dupes HigherDupes) {
            SplitLevels(new TaxonPage.Level[] { TaxonPage.Level.sp, TaxonPage.Level.ssp }, out SpeciesDupes, out HigherDupes);
        }
        public void SplitSspLevel(out Dupes SpeciesDupes, out Dupes HigherDupes) {
            SplitLevels(new TaxonPage.Level[] { TaxonPage.Level.ssp }, out SpeciesDupes, out HigherDupes);
        }

        public void SplitLevels(TaxonPage.Level[] spLevels, out Dupes SpeciesDupes, out Dupes HigherDupes) {
            SpeciesDupes = new Dupes();
            HigherDupes = new Dupes();

            SpeciesDupes.alsoMatch = alsoMatch;
            foreach (var item in allFoundNames) {
                if (item.Value == null || item.Value.Count == 0)
                    continue;

                var level = item.Value[0].TaxonName().pageLevel;
                bool isSpeciesLevel = spLevels.Contains(level); // (level == TaxonPage.Level.sp || level == TaxonPage.Level.ssp);
                Dupes bucket = (isSpeciesLevel ? SpeciesDupes : HigherDupes);
                bucket.allFoundNames[item.Key] = item.Value;
                if (dupes.ContainsKey(item.Key)) {
                    bucket.dupes[item.Key] = dupes[item.Key];
                }
            }
        }

        public void ExportWithBitrisSpeciesLevelPagesOnly(TextWriter output, string keyword = "is linked from", Dupes? alsoShow = null, bool showSpeciesLevel = true) {
            alsoShow ??= alsoMatch; // default to showing these too.. TODO: really should show both

            Dictionary<string, List<IUCNBitri>>? alsoShowThese = alsoShow?.allFoundNames;

            foreach (var dupeEntry in dupes.OrderBy(e => e.Value)) {
                string dupeNomralized = dupeEntry.Key;
                string dupeExampleName = dupeEntry.Value;


                //Console.WriteLine(dupe);
                allFoundNames.TryGetValue(dupeNomralized, out var biList);
                List<IUCNBitri>? triList = null;
                bool isTrinom = alsoShowThese != null && alsoShowThese.TryGetValue(dupeNomralized, out triList);

                if (biList != null && biList.Count > 0) {
                    var level = biList[0].TaxonName().pageLevel;
                    if (showSpeciesLevel != (level == TaxonPage.Level.sp || level == TaxonPage.Level.ssp))
                        continue;
                } else {
                    continue; // not found wtf
                }

                string listString = string.Format("* [[{0}]] {1} ''{2}{3}{4}'' ",
                    dupeExampleName,
                    keyword, // dupeNomralized,
//                    (isBinom ? biList.Select(bt => bt.FullName()).OrderBy(a => a).JoinStrings(", ") : ""),
//                    (isBinom && isTrinom ? ", " : ""),
//                    (isTrinom ? triList.Select(bt => bt.FullName()).OrderBy(a => a).JoinStrings(", ") : ""));
                    (biList != null ? biList.Select(bt => bt.FullDebugName()).JoinStrings(", ") : ""), // ordered already by SortBestMatchFirst()
                    (biList != null && isTrinom ? ", " : ""),
                    (isTrinom && triList != null ? triList.Select(bt => bt.FullDebugName()).JoinStrings(", ") : ""));

                output.WriteLine(listString);
            }

        }


    }
}