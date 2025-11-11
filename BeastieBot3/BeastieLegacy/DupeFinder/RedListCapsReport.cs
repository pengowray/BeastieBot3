using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

#nullable enable

namespace beastie {
    public class RedListCapsReport {
        private readonly Dictionary<string, string> wordToExample = new Dictionary<string, string>(StringComparer.Ordinal); // lowercase word, example common name (first word in name not included)

        public void FindWords(TaxonNode topNode) {

            foreach (var bitri in topNode.DeepBitris().Where(bt => !bt.isStockpop)) {
                var taxonName = bitri.TaxonName();
                string? name = taxonName.CommonName(); // must call this before checking commonNameFromIUCN
                if (!taxonName.commonNameFromIUCN || string.IsNullOrWhiteSpace(name))
                    continue;

                string lowercased = name.ToLowerInvariant();

                // split into words, with excessive checks for punctuation because it's from some weird stackoverflow example
                //var name = "'Oh, you can't help that,' said the Cat: 'we're all mad here. I'm mad. You're mad.'";
                var punctuation = lowercased.Where(char.IsPunctuation).Distinct().ToArray();
                var wordsWithPunctuation = lowercased.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var words = wordsWithPunctuation
                    .Select(x => x.Trim(punctuation))
                    .Where(x => x.Length > 0)
                    .ToList();

                if (words.Count == 0)
                    continue;

                //NOTE/TODO: last word is almost never capitalized, except in rare exceptions:
                // * of: "Jewel of Burma", "Scaly snail of Doboz", "Queen of the Andes"
                // * de: "Jarendeua de Sapo" ?
                // * from: "River crayfish from the South" ?
                // * Douglas-fir: "Chinese Douglas-fir"
                bool hasOf = words.Any(w => w is "of" or "de" or "del" or "di" or "from");
                bool hasPunctuation = wordsWithPunctuation.Length > 0 && wordsWithPunctuation[^1].Any(char.IsPunctuation); // contains punctuation
                bool dontskiplast = hasOf || hasPunctuation;

                var relevantWords = words.Skip(1).ToList(); // ignore first word (which is always title case for Wikipedia purposes)

                if (!dontskiplast) {
                    if (relevantWords.Count > 0) {
                        relevantWords.RemoveAt(relevantWords.Count - 1); // skip last word because it's probably going to be lowercase
                    }
                }

                foreach (var word in relevantWords) {
                    string correctedExample = CorrectCaps(name).UpperCaseFirstChar();

                    if (wordToExample.ContainsKey(word)) {
                        // append example
                        string existing = wordToExample[word];
                        if (existing.Length < 50) { // if there's already 50 characters of example, that's enough
                            wordToExample[word] = existing + ", " + correctedExample;
                        }

                    } else {
                        // add word with example
                        wordToExample[word] = correctedExample;
                    }
                }
            }

        }

        public void PrintWords(TextWriter? output) {
            output ??= Console.Out;

            output.WriteLine("// Rename to (or replace) caps.txt to use capitalization from this file. Generated file preserves caps but not comments. ");

            var caps = TaxaRuleList.Instance().Caps;

            foreach (var entry in wordToExample.OrderBy(e => e.Key, StringComparer.Ordinal)) {
                string word = entry.Key;
                string example = entry.Value;

                if (caps.TryGetValue(word, out var knownCaps)) {
                    output.WriteLine("{0} // {1}", knownCaps, example);
                } else {
                    output.WriteLine("{0} // {1}", word, example);
                }
            }

        }

        public static string CorrectCaps(string name) {
            if (string.IsNullOrWhiteSpace(name)) {
                return name;
            }

            var caps = TaxaRuleList.Instance().Caps;
            var punctuation = name.Where(char.IsPunctuation).Distinct().ToArray();
            var words = name.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            var result = new StringBuilder();
            foreach (string word in words) {
                string trimmed = word.Trim(punctuation);
                string lowered = trimmed.ToLowerInvariant();
                string fixedCaps = caps.TryGetValue(lowered, out var knownCaps) ? knownCaps : lowered;

                if (trimmed == word) {
                    result.Append(fixedCaps);
                } else {
                    int start = word.IndexOf(trimmed, StringComparison.Ordinal);
                    string leftTrim = start > 0 ? word[..start] : string.Empty;
                    string rightTrim = word.Substring(start + trimmed.Length);

                    result.Append(leftTrim);
                    result.Append(fixedCaps);
                    result.Append(rightTrim);
                }

                result.Append(' ');
            }

            return result.ToString().TrimEnd();
        }

        public static void ReadCapsToRules() {
            string filename = FileConfig.Instance().CapsReportFile + ".txt"; // note: remove '_generated' from filename for it to be read back
            Dictionary<string, string> caps = new Dictionary<string, string>(StringComparer.Ordinal); // lowercase, corrected case. For IUCN Red List common names

            try {
                using var capsReader = new StreamReader(filename, Encoding.UTF8);
                string? line;
                while ((line = capsReader.ReadLine()) != null) {
                    // remove comment
                    int commentIndex = line.IndexOf("//", StringComparison.Ordinal);
                    if (commentIndex >= 0) {
                        line = line.Substring(0, commentIndex);
                    }

                    line = line.Trim();

                    if (string.IsNullOrEmpty(line)) {
                        continue;
                    }

                    // add to caps
                    string lower = line.ToLowerInvariant();
                    if (lower != line) {
                        caps[lower] = line;
                    }
                }

                TaxaRuleList.Instance().Caps = caps;

            } catch (Exception ex) {
                Console.WriteLine("failed to read caps rule file: {0} ({1})", filename, ex.Message);
            }          
        }

    }

}