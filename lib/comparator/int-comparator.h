//  This source file was originally from:
//  https://github.com/cmu-db/bustub

/**
 * Function object return is > 0 if lhs > rhs, < 0 if lhs < rhs,
 * = 0 if lhs = rhs .
 */
class IntComparator {
public:
    inline auto operator()(const int lhs, const int rhs) const -> int {
        if (lhs < rhs) {
            return -1;
        }
        if (rhs < lhs) {
            return 1;
        }
        return 0;
    }
};
