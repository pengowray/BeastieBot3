using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace beastie {
    public class TaxonNameUnassigned : TaxonName {

        public TaxonNameUnassigned(string taxon) : base(taxon) {
            isAssigned = false;
        }

        [Obsolete]
        public override string CommonOrTaxoNameLowerPref() {
            return "not assigned";
        }

        public override string CommonNameLink(bool uppercase = true, PrettyStyle style = PrettyStyle.JustNames) {
            if (uppercase) {
                return "Not assigned";
            } else {
                return "not assigned";
            }
        }

        public override string CommonNameGroupTitleLink(bool upperFirstChar = true, string groupof = "species") {
            return CommonNameLink(upperFirstChar);
        }

        public override string? CommonName(bool allowIUCNName = true) {
            return null;
        }
    }
}
