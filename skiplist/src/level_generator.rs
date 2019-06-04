//! A skiplist is a list imlementation such that elements are always sorted, and the
//! insertion and deletion of elements are done in `O(log(n))`.
//!
//! Conceptually, a skiplist resembles something like:
//!
//! ```text
//! <head> ----------> [2] --------------------------------------------------> [9] ---------->
//! <head> ----------> [2] ------------------------------------[7] ----------> [9] ---------->
//! <head> ----------> [2] ----------> [4] ------------------> [7] ----------> [9] --> [10] ->
//! <head> --> [1] --> [2] --> [3] --> [4] --> [5] --> [6] --> [7] --> [8] --> [9] --> [10] ->
//! ```
//!
//! where we see that a node `[x]` will have `n` pointers to other nodes, where `n` represents
//! which level that node reaches.  The idea is that when a node needs to be found (say for
//! insertion), then you start at the highest level and move down.  This allows for many of the
//! lower-level nodes to be skipped thus making it faster.
//!
//! Each skiplist has an associated sorting function.  By default, the sorting function is
//! `|a, b| a.cmp(b)`, but any function is permitted, so long as it satisfies the following properties:
//! - It must be consistent:  `compare(a, b)` should always return the same result;
//! - It should be anti-symmetric:  If `compare(a, b) == Less`, then `compare(a, b) == Greater`.
//!   If this is not true, then some unexpected (and probably unsafe) behaviour may happen.
//!
//! Due to the nature of the skiplist being always sorted, it is not possible to get mutable
//! pointers to the elements of the skiplist as this could leave the skiplist in an inconsistent
//! state.  As a result, the `IndexMut` traits, `iter_mut`, and `get_mut` methods and traits are
//! not implemented.

extern crate rand;

use rand::distributions::{self, Sample};

// /////////////////////////////////////////////////////////////////////////////////////////////////
// Level Generator
// /////////////////////////////////////////////////////////////////////////////////////////////////

/// Upon the insertion of a new node in the list, the node is replicated to high levels with a
/// certain probability as determined by a `LevelGenerator`.
///
/// The `total()` reflects the total number of levels, and `random()` should produce an integer in
/// the range `[0, total)` with the desired probability distribution.
///
/// The most commonly used probability distribution is a geometrical distribution, whereby the
/// chance that a node occupies level `n` is `p` times as likely as occupying level `n-1`.
/// Typically, `p` is equal to 1/2, though over values can be used which will trade speed against
/// memory.
///
/// This trait is strictly speaking not necessary (hence not public), but if in the future there is
/// a need to allow for custom level generators then this trait is ready to go and it will just be
/// a matter of modifying the skiplist implementation.
pub trait LevelGenerator {
    fn random(&mut self) -> usize;
    fn total(&self) -> usize;
}

/// A level generator which will produce geometrically distributed numbers.
pub struct GeometricalLevelGenerator {
    total: usize,
    p: f64,
    unit_range: distributions::Range<f64>,
    rng: rand::XorShiftRng, // Fast generator
}

impl GeometricalLevelGenerator {
    /// Create a new GeometricalLevelGenerator with `total` number of levels, and `p` as the
    /// probability that a given node is present in the next level.
    ///
    /// # Panics
    ///
    /// `p` must be between 0 and 1 and will panic otherwise.  Similarly, `total` must be at
    /// greater or equal to 1.
    pub fn new(total: usize, p: f64) -> Self {
        if total == 0 {
            panic!("total must be non-zero.");
        }
        if p <= 0.0 || p >= 1.0 {
            panic!("p must be in (0, 1).");
        }
        GeometricalLevelGenerator {
            total: total,
            p: p,
            unit_range: distributions::Range::new(0.0f64, 1.0),
            rng: rand::XorShiftRng::new_unseeded(),
        }
    }
}

impl LevelGenerator for GeometricalLevelGenerator {
    fn random(&mut self) -> usize {
        let mut h = 0;
        let mut x = self.p;
        let f = 1.0 - self.unit_range.sample(&mut self.rng);
        while x > f && h + 1 < self.total {
            h += 1;
            x *= self.p
        }
        h
    }

    fn total(&self) -> usize {
        self.total
    }
}
