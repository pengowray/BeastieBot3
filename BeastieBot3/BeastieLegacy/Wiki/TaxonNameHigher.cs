using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace beastie {
    // abstract and null version of TaxonPage for ranks higher than genus (excluding genus). i.e. ones that aren't italicized
    class TaxonNameHigher : TaxonName {

        public TaxonNameHigher(string taxon) : base(taxon) {
        }

        [Obsolete]
        public override string CommonOrTaxoNameLowerPref() {
            return taxon;
        }

        public override string CommonNameLink(bool uppercase = true, PrettyStyle style = PrettyStyle.JustNames) {
            return string.Format("[[{0}]]", taxon);
        }


        //// eg "[[Tarsiidae|Tarsier]] species" or  "[[Hominidae|Great apes]]" or "[[Lorisoidea]]"" or "[[Cetartiodactyla|Cetartiodactyls]]"
        public override string CommonNameGroupTitleLink(bool upperFirstChar = true, string groupof = "species") {
            return string.Format("[[{0}]] {1}", taxon, groupof);
        }

        public override string? CommonName(bool allowIUCNName = true) {
            return null;
        }


    }
}
