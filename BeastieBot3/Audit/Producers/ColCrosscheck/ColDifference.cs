using BeastieBot3.Taxonomy;

// Buckets a ScientificNameDifference between two values (a higher-rank placement name, or a naming
// authority) into what the crosscheck reports care about:
//
//   Typo    - the two values are the same thing up to a likely data slip: a spelling difference, a
//             Unicode-encoding difference, a diacritic, or punctuation. These go to the "looks like
//             a spelling variant" reports because they are the actionable, high-precision cases.
//   Genuine - the two values are unrelated. For higher-rank placement this is a real
//             reorganisation (a family under a different order); it is kept in its own report.
//   Drop    - nothing worth reporting: identical, or differing only in whitespace or letter case.
//             IUCN stores higher-rank names in upper case as a house style, so a case-only
//             difference is not a data slip, and whitespace differences are covered by other
//             reports.
//
// Keeping this decision in one small, testable place means the placement and authority reports
// filter on exactly the same rule.

namespace BeastieBot3.Audit.Producers.ColCrosscheck;

internal static class ColDifference {
    internal enum Bucket {
        Drop,
        Typo,
        Genuine,
    }

    public static Bucket Classify(ScientificNameDifference.Result difference) => difference.Kind switch {
        ScientificNameDifference.Kind.Exact => Bucket.Drop,
        ScientificNameDifference.Kind.Whitespace => Bucket.Drop,
        ScientificNameDifference.Kind.Case => Bucket.Drop,
        // A mixed formatting difference (e.g. case plus spacing) is ambiguous; leaving it out keeps
        // the typo reports high-precision rather than surfacing house-style noise.
        ScientificNameDifference.Kind.Formatting => Bucket.Drop,
        ScientificNameDifference.Kind.Punctuation => Bucket.Typo,
        ScientificNameDifference.Kind.Unicode => Bucket.Typo,
        ScientificNameDifference.Kind.Diacritic => Bucket.Typo,
        ScientificNameDifference.Kind.Fuzzy => Bucket.Typo,
        ScientificNameDifference.Kind.Unrelated => Bucket.Genuine,
        _ => Bucket.Drop,
    };
}
