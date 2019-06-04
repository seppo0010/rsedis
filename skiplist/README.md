[Rust Skiplist](http://www.jpellis.me/projects/rust-skiplist) [![crates.io](https://img.shields.io/crates/v/skiplist.svg)](https://crates.io/crates/skiplist) [![crates.io](https://img.shields.io/crates/d/skiplist.svg)](https://crates.io/crates/skiplist) [![Build Status](https://img.shields.io/travis/JP-Ellis/rust-skiplist/master.svg)](https://travis-ci.org/JP-Ellis/rust-skiplist)
=============

A [skiplist](http://en.wikipedia.org/wiki/Skip_list) provides a way of storing
data with `log(i)` access, insertion and removal for an element in the `i`th position.

There are three kinds of collections defined here:
- **SkipList**  This behaves like nearly any other double-ended list.
- **OrderedSkipList**  Ensures that the elements are always sorted.  Still
  allows for access nodes at a given index.
- **SkipMap**  A map in which the keys are ordered.

Documentation can be found
[here](http://www.jpellis.me/rust-skiplist/skiplist/) and the cargo crate
can be found [here](https://crates.io/crates/skiplist).

The various `range` methods only work on the nightly version of Rust and
consequently are only enabled with the `unstable` feature.
