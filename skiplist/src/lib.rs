//! A skiplist is a way of storing elements in such a way that elements can be efficiently
//! accessed, inserted and removed, all in `O(log(n))` on average.
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
//! where we each node `[x]` has references to nodes further down the list, allowing the algorithm
//! to effectively skip ahead.
//!
//! The ordered skiplist has an associated sorting function which **must** be well-behaved.
//! Specifically, given some ordering function `f(a, b)`, it must satisfy the folowing properties:
//!
//! - Be well defined: `f(a, b)` should always return the same value
//! - Be anti-symmetric: `f(a, b) == Greater` iff `f(b, a) == Less` and `f(a, b) == Equal == f(b, a)`.
//! - By transitive: If `f(a, b) == Greater` and `f(b, c) == Greater` then `f(a, c) == Greater`.
//!
//! **Failure to satisfy these properties can result in unexpected behaviour at best, and at worst
//! will cause a segfault, null deref, or some other bad behaviour.**

#![cfg_attr(test, feature(test, collections_bound))]
#![allow(dead_code)]

extern crate rand;
#[cfg(test)]
extern crate test;

mod level_generator;
pub mod ordered_skiplist;
pub mod skiplist;
pub mod skipmap;
mod skipnode;

pub use ordered_skiplist::OrderedSkipList;
pub use skiplist::SkipList;
pub use skipmap::SkipMap;
