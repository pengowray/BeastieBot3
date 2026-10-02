// Language codes of the common names in the IUCN API's taxon records. IUCN writes ISO 639-2
// three-letter codes, in the bibliographic (B) form where one exists ("fre", "ger", "chi", "may"),
// plus some collective codes ("phi", "map", "sai") and two withdrawn ones ("scr" Croatian, "scc"
// Serbian). The site database stores the two-letter ISO 639-1 code wherever the language has one,
// so names from IUCN and Wikidata ("fr") group together, and keeps any other code as IUCN gives it.
//
// The table is written out rather than taken from CultureInfo, because .NET knows only the
// terminology (T) codes and the result would depend on the ICU data on the machine.

namespace BeastieBot3.SiteBuild;

internal static class IucnLanguageCodes {
    // ISO 639-1 code, then its ISO 639-2 codes (B first when B and T differ).
    private static readonly (string Iso1, string[] Iso2)[] Languages = {
        ("aa", ["aar"]), ("ab", ["abk"]), ("ae", ["ave"]), ("af", ["afr"]), ("ak", ["aka"]), ("am", ["amh"]),
        ("an", ["arg"]), ("ar", ["ara"]), ("as", ["asm"]), ("av", ["ava"]), ("ay", ["aym"]), ("az", ["aze"]),
        ("ba", ["bak"]), ("be", ["bel"]), ("bg", ["bul"]), ("bi", ["bis"]), ("bm", ["bam"]), ("bn", ["ben"]),
        ("bo", ["tib", "bod"]), ("br", ["bre"]), ("bs", ["bos"]), ("ca", ["cat"]), ("ce", ["che"]), ("ch", ["cha"]),
        ("co", ["cos"]), ("cr", ["cre"]), ("cs", ["cze", "ces"]), ("cu", ["chu"]), ("cv", ["chv"]), ("cy", ["wel", "cym"]),
        ("da", ["dan"]), ("de", ["ger", "deu"]), ("dv", ["div"]), ("dz", ["dzo"]), ("ee", ["ewe"]), ("el", ["gre", "ell"]),
        ("en", ["eng"]), ("eo", ["epo"]), ("es", ["spa"]), ("et", ["est"]), ("eu", ["baq", "eus"]), ("fa", ["per", "fas"]),
        ("ff", ["ful"]), ("fi", ["fin"]), ("fj", ["fij"]), ("fo", ["fao"]), ("fr", ["fre", "fra"]), ("fy", ["fry"]),
        ("ga", ["gle"]), ("gd", ["gla"]), ("gl", ["glg"]), ("gn", ["grn"]), ("gu", ["guj"]), ("gv", ["glv"]),
        ("ha", ["hau"]), ("he", ["heb"]), ("hi", ["hin"]), ("ho", ["hmo"]), ("hr", ["hrv", "scr"]), ("ht", ["hat"]),
        ("hu", ["hun"]), ("hy", ["arm", "hye"]), ("hz", ["her"]), ("ia", ["ina"]), ("id", ["ind"]), ("ie", ["ile"]),
        ("ig", ["ibo"]), ("ii", ["iii"]), ("ik", ["ipk"]), ("io", ["ido"]), ("is", ["ice", "isl"]), ("it", ["ita"]),
        ("iu", ["iku"]), ("ja", ["jpn"]), ("jv", ["jav"]), ("ka", ["geo", "kat"]), ("kg", ["kon"]), ("ki", ["kik"]),
        ("kj", ["kua"]), ("kk", ["kaz"]), ("kl", ["kal"]), ("km", ["khm"]), ("kn", ["kan"]), ("ko", ["kor"]),
        ("kr", ["kau"]), ("ks", ["kas"]), ("ku", ["kur"]), ("kv", ["kom"]), ("kw", ["cor"]), ("ky", ["kir"]),
        ("la", ["lat"]), ("lb", ["ltz"]), ("lg", ["lug"]), ("li", ["lim"]), ("ln", ["lin"]), ("lo", ["lao"]),
        ("lt", ["lit"]), ("lu", ["lub"]), ("lv", ["lav"]), ("mg", ["mlg"]), ("mh", ["mah"]), ("mi", ["mao", "mri"]),
        ("mk", ["mac", "mkd"]), ("ml", ["mal"]), ("mn", ["mon"]), ("mr", ["mar"]), ("ms", ["may", "msa"]), ("mt", ["mlt"]),
        ("my", ["bur", "mya"]), ("na", ["nau"]), ("nb", ["nob"]), ("nd", ["nde"]), ("ne", ["nep"]), ("ng", ["ndo"]),
        ("nl", ["dut", "nld"]), ("nn", ["nno"]), ("no", ["nor"]), ("nr", ["nbl"]), ("nv", ["nav"]), ("ny", ["nya"]),
        ("oc", ["oci"]), ("oj", ["oji"]), ("om", ["orm"]), ("or", ["ori"]), ("os", ["oss"]), ("pa", ["pan"]),
        ("pi", ["pli"]), ("pl", ["pol"]), ("ps", ["pus"]), ("pt", ["por"]), ("qu", ["que"]), ("rm", ["roh"]),
        ("rn", ["run"]), ("ro", ["rum", "ron", "mol"]), ("ru", ["rus"]), ("rw", ["kin"]), ("sa", ["san"]), ("sc", ["srd"]),
        ("sd", ["snd"]), ("se", ["sme"]), ("sg", ["sag"]), ("si", ["sin"]), ("sk", ["slo", "slk"]), ("sl", ["slv"]),
        ("sm", ["smo"]), ("sn", ["sna"]), ("so", ["som"]), ("sq", ["alb", "sqi"]), ("sr", ["srp", "scc"]), ("ss", ["ssw"]),
        ("st", ["sot"]), ("su", ["sun"]), ("sv", ["swe"]), ("sw", ["swa"]), ("ta", ["tam"]), ("te", ["tel"]),
        ("tg", ["tgk"]), ("th", ["tha"]), ("ti", ["tir"]), ("tk", ["tuk"]), ("tl", ["tgl"]), ("tn", ["tsn"]),
        ("to", ["ton"]), ("tr", ["tur"]), ("ts", ["tso"]), ("tt", ["tat"]), ("tw", ["twi"]), ("ty", ["tah"]),
        ("ug", ["uig"]), ("uk", ["ukr"]), ("ur", ["urd"]), ("uz", ["uzb"]), ("ve", ["ven"]), ("vi", ["vie"]),
        ("vo", ["vol"]), ("wa", ["wln"]), ("wo", ["wol"]), ("xh", ["xho"]), ("yi", ["yid"]), ("yo", ["yor"]),
        ("za", ["zha"]), ("zh", ["chi", "zho"]), ("zu", ["zul"]),
    };

    // Codes that say the language is unknown or that there is none: undetermined, no linguistic
    // content, uncoded, several languages, and the range reserved for local use.
    private static readonly HashSet<string> NoLanguage = new(StringComparer.Ordinal) {
        "und", "zxx", "mis", "mul", "qaa-qtz",
    };

    private static readonly Dictionary<string, string> ToIso1 = BuildMap();

    private static Dictionary<string, string> BuildMap() {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (iso1, iso2) in Languages) {
            map[iso1] = iso1;
            foreach (var code in iso2) {
                map[code] = iso1;
            }
        }
        return map;
    }

    /// The code to store for a common name: the ISO 639-1 code when the language has one, the
    /// code as given (lower case) when it has none, and null when the code says the language is
    /// unknown or missing. A byte order mark (one IUCN record has "﻿aar") is removed.
    public static string? Normalise(string? code) {
        if (code is null) {
            return null;
        }
        var trimmed = code.Trim().TrimStart('﻿').Trim().ToLowerInvariant();
        if (trimmed.Length == 0 || NoLanguage.Contains(trimmed)) {
            return null;
        }
        return ToIso1.TryGetValue(trimmed, out var iso1) ? iso1 : trimmed;
    }
}
