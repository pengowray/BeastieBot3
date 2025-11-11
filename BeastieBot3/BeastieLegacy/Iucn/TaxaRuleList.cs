using System;
using System.Collections.Generic;
using System.IO;

#nullable enable

// TODO
// (done) lemurs, marsupials (in rules)
// (done) display of trinoms + stocks/pops
// add "low priority" option to '=' rules, to use the Wiki's names when they become available.
// display of infraspecies rank ("var.", etc)
// statistics: numbers: assessed, CR, threatened, DD, (recently) extinct
// statistics: are all from < 3 countries?
// rename '=' to 'common' or 'is-named' or 'in-english' or something

// weird page with two taxoboxes: https://en.wikipedia.org/wiki/Bongo_(antelope)

//Limnodynastidae redirects to Myobatrachidae, but both families are used by IUCN
namespace beastie {
    
    public class TaxaRuleList {

        // grammar:
        // X wikilink Y // don't wikilink to X, use Y instead (for disambiguation), e.g. Anura wikilink Anura (frog)
        // X = Y    -- use Y as the common name for (singular)
        // X = Y family  -- use "family" in the heading instead of "species" (for "cavy family")
        // X = Y species Z -- don't add "species" to name if already in name (eg Hylidae)
        // X plural Y -- Y is the plural common name for X, but don't specify a singular common name
        // X includes Y. Have a blurb under the heading saying "Includes y"
        // X comprises Y. Have grey text in brackets under the heading with what it comprises.
        // // comment
        // X force-split true // split taxa into lower ranks if available, even if there are few of them
        // X below Y Z // Place new category Y below existing category X, and make it rank Z


        public const string GeneralRules = null;

        private static TaxaRuleList? _instance;
        public static TaxaRuleList Instance() {
            throw new NotImplementedException("[No longer implemented] Need to update TaxaRuleList.GeneralRules to read from rules-list.txt.");

            if (_instance == null) {
                _instance = new TaxaRuleList();
                _instance.Compile();
            }

            return _instance;
        }

    private readonly string rules;
    private Dictionary<string, string> caps = new Dictionary<string, string>();

        // compiled:
        public Dictionary<string, TaxonRules> records { get; } = new Dictionary<string, TaxonRules>();

        public HashSet<string>? BinomAmbig { get; set; }
        public HashSet<string>? InfraAmbig { get; set; }
        public HashSet<string>? WikiPageAmbig { get; set; } // pages that are pointed to by multiple species
        public Dictionary<string, string> ScientificTypos { get; } = new Dictionary<string, string>();

        // see also: https://en.wikipedia.org/wiki/User:Beastie_Bot/Redirects_to_same_title
        public Dupes? WikiSpeciesDupes { get; set; } // Page names that link to same species
        public Dupes? WikiHigherDupes { get; set; } // Page names that link to the same higher taxon
        public Dictionary<string, string> Caps {
            get => caps;
            set => caps = value ?? new Dictionary<string, string>();
        } // lowercase, corrected case. For IUCN Red List common names

        //public Dictionary<string, TaxonRules.Field> fields = new Dictionary<string, TaxonRules.Field>(); // const

        public TaxaRuleList() {
            rules = GeneralRules;

            //     public enum TaxonDisplayField { None, commonName, commonPlural, forcesplit, splitOff, below, includes, comprises, means, wikilink }

            //fields["force-split"] = TaxonRules.Field.forcesplit;
            //fields["="] = TaxonRules.Field.commonName;
    }

    public void Compile() {
            int lineNumber = 0;
            using var reader = new StringReader(rules);
            string? rawLine;
            while ((rawLine = reader.ReadLine()) != null) {
                string line = rawLine;
                lineNumber++;

                if (string.IsNullOrWhiteSpace(line))
                    continue;

                // remove comments
                int commentIndex = line.IndexOf("//", StringComparison.Ordinal);
                if (commentIndex >= 0) {
                    line = line.Substring(0, commentIndex);
                }

                line = line.Trim();

                if (string.IsNullOrEmpty(line))
                    continue;

                if (line.Contains(" = ", StringComparison.Ordinal)) {
                    string[]? parts = SplitAndAddToRecord(line, " = ", "!", lineNumber, TaxonRules.Field.commonName, TaxonRules.Field.commonPlural);

                    // warn if -s ending
                    if (parts != null) {
                        string common = parts[1];
                        if (common.EndsWith("s")
                            && !common.Contains("species")
                            && !common.EndsWith("fishes")
                            && !common.EndsWith("colobus") 
                            && !common.EndsWith("hippopotamus")
                            && !common.EndsWith("rhinoceros") 
                            && !common.EndsWith("pogonomys") // (temporary)
                            ) {
                            Warning(lineNumber, line, "Common name may be plural (ends with 's'): " + common);
                        }
                    }

                } else if (line.Contains("force-split", StringComparison.Ordinal)) {
                    SplitAndAddToRecord(line, " force-split ", lineNumber, TaxonRules.Field.forcesplit);

                } else if (line.Contains(" plural ", StringComparison.Ordinal)) {
                    SplitAndAddToRecord(line, " plural ", lineNumber, TaxonRules.Field.commonPlural);

                } else if (line.Contains(" adj ", StringComparison.Ordinal)) {
                    SplitAndAddToRecord(line, " adj ", lineNumber, TaxonRules.Field.adj);

                } else if (line.Contains(" split-off ", StringComparison.Ordinal)) {
                    SplitAndAddToRecord(line, " split-off ", lineNumber, TaxonRules.Field.splitoff);

                } else if (line.Contains(" below ", StringComparison.Ordinal)) {
                    //SplitAndAddToRecord(line, " below ", lineNumber, TaxonRules.Field.below);
                    SplitAndAddToRecord(line, " below ", ":", lineNumber, TaxonRules.Field.below, TaxonRules.Field.belowRank);
                    //TODO: error if rank missing

                } else if (line.Contains(" includes ", StringComparison.Ordinal)) {
                    SplitAndAddToRecord(line, " includes ", lineNumber, TaxonRules.Field.includes);

                } else if (line.Contains(" comprises ", StringComparison.Ordinal)) {
                    SplitAndAddToRecord(line, " comprises ", lineNumber, TaxonRules.Field.comprises);

                } else if (line.Contains(" means ", StringComparison.Ordinal)) {
                    SplitAndAddToRecord(line, " means ", lineNumber, TaxonRules.Field.means);

                } else if (line.Contains(" wikilink ", StringComparison.Ordinal)) {
                    //SplitAndAddToRecord(line, " wikilink ", lineNumber, TaxonRules.Field.wikilink);
                    SplitAndAddToRecord(line, " wikilink ", lineNumber, TaxonRules.Field.wikilink);

                } else if (line.Contains(" typo-of ", StringComparison.Ordinal)) {
                    // [same as wikilink but also adds to additional field]
                    // Changes both the link and display of a taxon.
                    // Also adds a note when listed to say "Listed by IUCN as..." with the original taxon name.
                    // Changes included at the end of "Common Name Issues" report (IUCNCommonNameIssuesReport).
                    // Adds to both wikilink and typo-of fields.

                    SplitAndAddToRecord(line, " typo-of ", lineNumber, TaxonRules.Field.wikilink);
                    string[]? typo = SplitAndAddToRecord(line, " typo-of ", lineNumber, TaxonRules.Field.typoOf);
                    if (typo is { Length: 2 }) {
                        ScientificTypos[typo[0]] = typo[1];
                    }

                }
            }
        }

        string[]? SplitAndAddToRecord(string line, string seperator, int lineNumber, TaxonRules.Field setField = TaxonRules.Field.None) {
            var parts = line.Split(new string[] { seperator }, 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 2) {
                Error(lineNumber, line,
                    string.Format("'{0}' statement missing arguments. Needs something on either side: {1}", seperator, line));
                return null;
            } else {
                parts[0] = parts[0].Trim();
                parts[1] = parts[1].Trim();
                string taxonString = parts[0];
                string fieldValue = parts[1];

                if (setField != TaxonRules.Field.None) {
                    TaxonRules record = GetOrCreateRecord(taxonString);

                    record[setField] = fieldValue;
                }

                return parts;
            }
        }

        //string[] SplitAndAddToDictionaries(string line, string seperator1, string seperator2, int lineNumber, Dictionary<string, string> addToDictionary1 = null, Dictionary<string, string> addToDictionary2 = null) {
        string[]? SplitAndAddToRecord(string line, string seperator1, string seperator2, int lineNumber, TaxonRules.Field setField1 = TaxonRules.Field.None, TaxonRules.Field setField2 = TaxonRules.Field.None) {
            
            var parts = line.Split(new string[] { seperator1 }, 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length != 2) {
                Error(lineNumber, line,
                    string.Format("'{0}' statement missing arguments. Needs something on either side of '{1}'", line, seperator1));
                return null;
            } else {
                var subparts = parts[1].Split(new string[] { seperator2 }, 2, StringSplitOptions.RemoveEmptyEntries);

                if (subparts.Length == 0) {
                    return null; // error?

                } else if (subparts.Length == 1) { // if (parts.Length == 1) {
                    parts[0] = parts[0].Trim();
                    parts[1] = parts[1].Trim();

                    string taxonString = parts[0];
                    string fieldValue = parts[1];

                    if (setField1 != TaxonRules.Field.None) {
                        TaxonRules record = GetOrCreateRecord(taxonString);

                        record[setField1] = fieldValue;
                    }

                    return parts;

                } else {
                    parts[0] = parts[0].Trim();
                    string taxonString = parts[0];

                    if (subparts.Length != 2) {
                        Error(lineNumber, line,
                            string.Format("'{0}' statement has too many arguments or something. Should have one of each separator '{1}' and '{2}'", line, seperator1, seperator2));
                        return null;
                    }

                    subparts[0] = subparts[0].Trim();
                    subparts[1] = subparts[1].Trim();

                    if (setField1 != TaxonRules.Field.None) {
                        TaxonRules record = GetOrCreateRecord(taxonString);
                        record[setField1] = subparts[0];

                    }
                    if (setField2 != TaxonRules.Field.None) {
                        TaxonRules record = GetOrCreateRecord(taxonString);
                        record[setField2] = subparts[1];
                    }

                    return new string[] { taxonString, subparts[0], subparts[1] };
                }
            }
        }

        TaxonRules GetOrCreateRecord(string taxonString) {
            if (!records.TryGetValue(taxonString, out var record)) {
                record = new TaxonRules();
                records[taxonString] = record;
            }

            return record;
        }

        void Warning(int lineNumber, string line, string warning) {
            Console.Error.WriteLine("Warning on line {0}: {1}", lineNumber, warning);
            Console.Error.WriteLine("Line {0}: {1}", lineNumber, line);
        }

        void Error(int lineNumber, string line, string error) {
            Console.Error.WriteLine("Error on line {0}: {1}", lineNumber, error);
            Console.Error.WriteLine("Line {0}: {1}", lineNumber, line);
        }

        public TaxonRules? GetDetails(string taxon) {
            return records.TryGetValue(taxon, out var details) ? details : null;
        }

        public TaxonRules GetOrCreateDetails(string taxon) {
            if (records.TryGetValue(taxon, out var details)) {
                return details;
            }

            var newDetails = new TaxonRules();
            records[taxon] = newDetails;
            return newDetails;
        }
    }
}
