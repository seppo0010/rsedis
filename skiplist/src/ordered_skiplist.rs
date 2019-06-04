extern crate rand;
#[cfg(test)]
extern crate test;

use std::cmp::{self, Ordering};
#[cfg(feature = "unstable")]
use std::collections::Bound;
use std::default;
use std::fmt;
use std::hash::{self, Hash};
use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::ops;

use level_generator::{GeometricalLevelGenerator, LevelGenerator};
use skipnode::SkipNode;

// /////////////////////////////////////////////////////////////////////////////////////////////////
// OrderedSkipList
// /////////////////////////////////////////////////////////////////////////////////////////////////

/// The ordered skiplist provides a way of storing elements such that they are always
/// sorted and at the same time provides efficient way to access, insert and remove nodes.
/// Just like `SkipList`, it also provides access to indices.
///
/// By default, the OrderedSkipList uses the comparison function `a.partial_cmp(b).expect("Value
/// cannot be ordered")`.  This allows the list to handles all types which implement `Ord` and
/// `PartialOrd`, though it will panic if value which cannot be ordered is inserted (such as
/// `Float::nan()`).
///
/// The ordered skiplist has an associated sorting function which **must** be well-behaved.
/// Specifically, given some ordering function `f(a, b)`, it must satisfy the folowing properties:
///
/// - Be well defined: `f(a, b)` should always return the same value
/// - Be anti-symmetric: `f(a, b) == Greater` iff `f(b, a) == Less` and `f(a, b) == Equal == f(b,
///   a)`.
/// - By transitive: If `f(a, b) == Greater` and `f(b, c) == Greater` then `f(a, c) == Greater`.
///
/// **Failure to satisfy these properties can result in unexpected behaviour at best, and at worst
/// will cause a segfault, null deref, or some other bad behaviour.**
pub struct OrderedSkipList<T> {
    // Storage, this is not sorted
    head: Box<SkipNode<T>>,
    len: usize,
    level_generator: GeometricalLevelGenerator,
    compare: Box<Fn(&T, &T) -> Ordering>,
}

// ///////////////////////////////////////////////
// Inherent methods
// ///////////////////////////////////////////////

impl<T> OrderedSkipList<T>
where
    T: cmp::PartialOrd,
{
    /// Create a new skiplist with the default default comparison function of `|&a, &b|
    /// a.cmp(b).unwrap()` and the default number of 16 levels.  As a result, any element which
    /// cannot be ordered will cause insertion to panic.
    ///
    /// The comparison function can always be changed with `sort_by`, which has essentially no
    /// cost if done before inserting any elements.
    ///
    /// # Panic
    ///
    /// The default comparison function will cause a panic if an element is inserted which cannot
    /// be ordered (such as `Float::nan()`).
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist: OrderedSkipList<i64> = OrderedSkipList::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        let lg = GeometricalLevelGenerator::new(16, 1.0 / 2.0);
        OrderedSkipList {
            head: Box::new(SkipNode::head(lg.total())),
            len: 0,
            level_generator: lg,
            compare: (Box::new(|a: &T, b: &T| {
                a.partial_cmp(b).expect("Element cannot be ordered.")
            })) as Box<Fn(&T, &T) -> Ordering>,
        }
    }

    /// Constructs a new, empty skiplist with the optimal number of levels for the intended
    /// capacity.  Specifically, it uses `floor(log2(capacity))` number of levels, ensuring that
    /// only *a few* nodes occupy the highest level.
    ///
    /// It uses the default comparison function of `|&a, &b| a.cmp(b).unwrap()` and can be changed
    /// with `sort_by`.
    ///
    /// # Panic
    ///
    /// The default comparison function will cause a panic if an element is inserted which cannot
    /// be ordered (such as `Float::nan()`).
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::with_capacity(100);
    /// skiplist.extend(0..100);
    /// ```
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let levels = (capacity as f64).log2().floor() as usize;
        let lg = GeometricalLevelGenerator::new(levels, 1.0 / 2.0);
        OrderedSkipList {
            head: Box::new(SkipNode::head(lg.total())),
            len: 0,
            level_generator: lg,
            compare: (Box::new(|a: &T, b: &T| {
                a.partial_cmp(b).expect("Element cannot be ordered.")
            })) as Box<Fn(&T, &T) -> Ordering>,
        }
    }
}

impl<T> OrderedSkipList<T> {
    /// Create a new skiplist using the provided function in order to determine the ordering of
    /// elements within the list.  It will be generated with 16 levels.
    ///
    /// # Warning
    ///
    /// The sorting function which **must** be well-behaved.  Specifically, given some ordering
    /// function `f(a, b)`, it must satisfy the folowing properties:
    ///
    /// - Be well defined: `f(a, b)` should always return the same value
    /// - Be anti-symmetric: `f(a, b) == Greater` iff `f(b, a) == Less` and `f(a, b) == Equal == f(b,
    ///   a)`.
    /// - By transitive: If `f(a, b) == Greater` and `f(b, c) == Greater` then `f(a, c) == Greater`.
    ///
    /// **Failure to satisfy these properties can result in unexpected behaviour at best, and at worst
    /// will cause a segfault, null deref, or some other bad behaviour.**
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    /// use std::cmp::Ordering;
    ///
    /// // Store even number before odd ones and sort as usual within same parity group.
    /// let mut skiplist = unsafe { OrderedSkipList::with_comp(
    ///     |a: &u64, b: &u64|
    ///     if a%2 == b%2 {
    ///         a.cmp(b)
    ///     } else if a%2 == 0 {
    ///         Ordering::Less
    ///     } else {
    ///         Ordering::Greater
    ///     })};
    /// ```
    #[inline]
    pub unsafe fn with_comp<F>(f: F) -> Self
    where
        F: 'static + Fn(&T, &T) -> Ordering,
    {
        let lg = GeometricalLevelGenerator::new(16, 1.0 / 2.0);
        OrderedSkipList {
            head: Box::new(SkipNode::head(lg.total())),
            len: 0,
            level_generator: lg,
            compare: Box::new(f),
        }
    }

    /// Change the method which determines the ordering of the elements in the skiplist.
    ///
    /// # Panics
    ///
    /// This call will panic if the ordering of the elements will be changed as a result of this
    /// new comparison method.
    ///
    /// As a result, `sort_by` is best to call if the skiplist is empty or has just a single
    /// element and may panic with 2 or more elements.
    ///
    /// # Warning
    ///
    /// The sorting function which **must** be well-behaved.  Specifically, given some ordering
    /// function `f(a, b)`, it must satisfy the folowing properties:
    ///
    /// - Be well defined: `f(a, b)` should always return the same value
    /// - Be anti-symmetric: `f(a, b) == Greater` iff `f(b, a) == Less` and `f(a, b) == Equal == f(b,
    ///   a)`.
    /// - By transitive: If `f(a, b) == Greater` and `f(b, c) == Greater` then `f(a, c) == Greater`.
    ///
    /// **Failure to satisfy these properties can result in unexpected behaviour at best, and at worst
    /// will cause a segfault, null deref, or some other bad behaviour.**
    ///
    /// # Examples
    ///
    /// ```should_fail
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// unsafe { skiplist.sort_by(|a: &i64, b: &i64| b.cmp(a)) } // All good; skiplist empty.
    /// skiplist.insert(0);                                       // Would still be good here.
    /// skiplist.insert(10);
    /// unsafe { skiplist.sort_by(|a: &i64, b: &i64| a.cmp(b)) } // Panics; order would change.
    /// ```
    pub unsafe fn sort_by<F>(&mut self, f: F)
    where
        F: 'static + Fn(&T, &T) -> Ordering,
    {
        let mut node: *mut SkipNode<T> = mem::transmute_copy(&mut self.head);

        while let Some(next) = (*node).links[0] {
            if let (&Some(ref a), &Some(ref b)) = (&(*node).value, &(*next).value) {
                if f(a, b) == Ordering::Greater {
                    panic!("New ordering function cannot be used.");
                }
            }
            node = next;
        }

        self.compare = Box::new(f);
    }

    /// Clears the skiplist, removing all values.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// skiplist.clear();
    /// assert!(skiplist.is_empty());
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        unsafe {
            let node: *mut SkipNode<T> = mem::transmute_copy(&self.head);

            while let Some(ref mut next) = (*node).next {
                mem::replace(&mut (*node).next, mem::replace(&mut next.next, None));
            }
        }
        let new_head = Box::new(SkipNode::head(self.level_generator.total()));
        self.len = 0;
        mem::replace(&mut self.head, new_head);
    }

    /// Returns the number of elements in the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// assert_eq!(skiplist.len(), 10);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the skiplist contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// assert!(skiplist.is_empty());
    ///
    /// skiplist.insert(1);
    /// assert!(!skiplist.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Insert the element into the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    ///
    /// skiplist.insert(0);
    /// skiplist.insert(5);
    /// assert_eq!(skiplist.len(), 2);
    /// assert!(!skiplist.is_empty());
    /// ```
    pub fn insert(&mut self, value: T) {
        unsafe {
            self.len += 1;

            let mut new_node = Box::new(SkipNode::new(value, self.level_generator.random()));
            let new_node_ptr: *mut SkipNode<T> = mem::transmute_copy(&new_node);

            // At each level, `insert_node` moves down the list until it is just prior to where the node
            // will be inserted.  As this is parsed top-down, the link lengths can't yet be
            // adjusted and the insert nodes are stored in `insert_nodes`.
            let mut insert_node: *mut SkipNode<T> = mem::transmute_copy(&mut self.head);
            let mut insert_nodes: Vec<*mut SkipNode<T>> = Vec::with_capacity(new_node.level);

            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                // Move insert_node down until `next` is not less than the new node.
                while let Some(next) = (*insert_node).links[lvl] {
                    if let (&Some(ref a), &Some(ref b)) = (&(*next).value, &new_node.value) {
                        if (self.compare)(a, b) == Ordering::Less {
                            insert_node = next;
                            continue;
                        }
                    }
                    break;
                }
                // The node level is really just how many links it has.
                // If we've reached the node level, insert it in the links:
                // Before:    [0] ------------> [1]
                // After:     [0] --> [new] --> [1]
                if lvl <= new_node.level {
                    insert_nodes.push(insert_node);
                    new_node.links[lvl] = (*insert_node).links[lvl];
                    (*insert_node).links[lvl] = Some(new_node_ptr);
                } else {
                    (*insert_node).links_len[lvl] += 1;
                }
            }

            // We now parse the insert_nodes from bottom to top, and calculate (and adjust) link
            // lengths.
            for (lvl, &insert_node) in insert_nodes.iter().rev().enumerate() {
                if lvl == 0 {
                    (*insert_node).links_len[lvl] = if (*insert_node).is_head() { 0 } else { 1 };
                    new_node.links_len[lvl] = 1;
                } else {
                    let length = self
                        .link_length(insert_node, Some(new_node_ptr), lvl)
                        .unwrap();
                    new_node.links_len[lvl] = (*insert_node).links_len[lvl] - length + 1;
                    (*insert_node).links_len[lvl] = length;
                }
            }

            // Adjust `.prev`
            new_node.prev = Some(insert_node);
            if let Some(next) = (*new_node).links[0] {
                (*next).prev = Some(new_node_ptr);
            }

            // Move the ownerships around, inserting the new node.
            let tmp = mem::replace(&mut (*insert_node).next, Some(new_node));
            if let Some(ref mut node) = (*insert_node).next {
                node.next = tmp;
            }
        }
    }

    /// Provides a reference to the front element, or `None` if the skiplist is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// assert_eq!(skiplist.front(), None);
    ///
    /// skiplist.insert(1);
    /// skiplist.insert(2);
    /// assert_eq!(skiplist.front(), Some(&1));
    /// ```
    #[inline]
    pub fn front(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self[0])
        }
    }

    /// Provides a reference to the back element, or `None` if the skiplist is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// assert_eq!(skiplist.back(), None);
    ///
    /// skiplist.insert(1);
    /// skiplist.insert(2);
    /// assert_eq!(skiplist.back(), Some(&2));
    /// ```
    #[inline]
    pub fn back(&self) -> Option<&T> {
        let len = self.len();
        if len > 0 {
            Some(&self[len - 1])
        } else {
            None
        }
    }

    /// Provides a reference to the element at the given index, or `None` if the skiplist is empty
    /// or the index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// assert_eq!(skiplist.get(&0), None);
    /// skiplist.extend(0..10);
    /// assert_eq!(skiplist.get(&0), Some(&0));
    /// assert_eq!(skiplist.get(&10), None);
    /// ```
    #[inline]
    pub fn get(&self, index: &usize) -> Option<&T> {
        let len = self.len();
        if index < &len {
            Some(&self[*index])
        } else {
            None
        }
    }

    /// Removes the first element and returns it, or `None` if the sequence is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.insert(1);
    /// skiplist.insert(2);
    ///
    /// assert_eq!(skiplist.pop_front(), Some(1));
    /// assert_eq!(skiplist.pop_front(), Some(2));
    /// assert_eq!(skiplist.pop_front(), None);
    /// ```
    #[inline]
    pub fn pop_front(&mut self) -> Option<T> {
        if self.is_empty() {
            None
        } else {
            Some(self.remove_index(&0))
        }
    }

    /// Removes the last element and returns it, or `None` if the sequence is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.insert(1);
    /// skiplist.insert(2);
    ///
    /// assert_eq!(skiplist.pop_back(), Some(2));
    /// assert_eq!(skiplist.pop_back(), Some(1));
    /// assert_eq!(skiplist.pop_back(), None);
    /// ```
    #[inline]
    pub fn pop_back(&mut self) -> Option<T> {
        let len = self.len();
        if len > 0 {
            Some(self.remove_index(&(len - 1)))
        } else {
            None
        }
    }

    /// Returns true if the value is contained in the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// assert!(skiplist.contains(&4));
    /// assert!(!skiplist.contains(&15));
    /// ```
    pub fn contains(&self, value: &T) -> bool {
        unsafe {
            let mut node: *const SkipNode<T> = mem::transmute_copy(&self.head);

            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                while let Some(next) = (*node).links[lvl] {
                    if let Some(ref next_value) = (*next).value {
                        match (self.compare)(next_value, value) {
                            Ordering::Less => {
                                node = next;
                                continue;
                            }
                            Ordering::Equal => {
                                return true;
                            }
                            Ordering::Greater => {
                                break;
                            }
                        }
                    }
                }
            }
            return false;
        }
    }

    /// Removes and returns an element with the same value or None if there are no such values in
    /// the skiplist.
    ///
    /// If the skiplist contains multiple values with the desired value, the highest level one will
    /// be removed.  This will results in a deterioration in the skiplist's performance if the
    /// skiplist contains *many* duplicated values which are very frequently inserted and removed.
    /// In such circumstances, the slower `remove_first` method is preferred.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// assert_eq!(skiplist.remove(&4), Some(4)); // Removes the last one
    /// assert_eq!(skiplist.remove(&4), None); // No more '4' left
    /// ```
    pub fn remove(&mut self, value: &T) -> Option<T> {
        if self.len == 0 {
            return None;
        }

        unsafe {
            let mut node: *mut SkipNode<T> = mem::transmute_copy(&mut self.head);
            let mut return_node: Option<*mut SkipNode<T>> = None;
            let mut prev_nodes: Vec<*mut SkipNode<T>> =
                Vec::with_capacity(self.level_generator.total());

            // We don't know if the value we're looking for is even inside this list until we get
            // to the lowest level.  For this reason, we store where the returned node would be in
            // `prev_nodes` and if we find the desired node, we have reference to all the
            // appropriate nodes to modify.
            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                if let Some(return_node) = return_node {
                    while let Some(next) = (*node).links[lvl] {
                        if next == return_node {
                            prev_nodes.push(node);
                            break;
                        } else {
                            node = next;
                        }
                    }
                } else {
                    while let Some(next) = (*node).links[lvl] {
                        if let &Some(ref next_value) = &(*next).value {
                            match (self.compare)(next_value, value) {
                                Ordering::Less => {
                                    node = next;
                                    continue;
                                }
                                Ordering::Equal => {
                                    return_node = Some(next);
                                    prev_nodes.push(node);
                                    break;
                                }
                                Ordering::Greater => {
                                    prev_nodes.push(node);
                                    break;
                                }
                            }
                        }
                    }
                    // We have not yet found the node, and there are no further nodes at this
                    // level, so the return node (if present) is between `node` and tail.
                    if (*node).links[lvl].is_none() {
                        prev_nodes.push(node);
                        continue;
                    }
                }
            }

            // At this point, `return_node` contains a reference to the return node if it was
            // found, otherwise it is None.
            if let Some(return_node) = return_node {
                for (lvl, &prev_node) in prev_nodes.iter().rev().enumerate() {
                    if (*prev_node).links[lvl] == Some(return_node) {
                        (*prev_node).links[lvl] = (*return_node).links[lvl];
                        (*prev_node).links_len[lvl] += (*return_node).links_len[lvl] - 1;
                    } else {
                        (*prev_node).links_len[lvl] -= 1;
                    }
                }
                if let Some(next_node) = (*return_node).links[0] {
                    (*next_node).prev = (*return_node).prev;
                }
                self.len -= 1;
                mem::replace(
                    &mut (*(*return_node).prev.unwrap()).next,
                    mem::replace(&mut (*return_node).next, None),
                )
                .unwrap()
                .into_inner()
            } else {
                None
            }
        }
    }

    /// Removes and returns an element with the same value or None if there are no such values in
    /// the skiplist.
    ///
    /// If the skiplist contains multiple values with the desired value, the first one in the
    /// skiplist will be returned.  If the skiplist contains *many* duplicated values which are
    /// frequently inserted and removed, this method should be preferred over `remove`.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// for _ in 0..10 {
    ///     skiplist.extend(0..10);
    /// }
    /// assert_eq!(skiplist.remove(&15), None);
    /// for _ in 0..9 {
    ///     for i in 0..10 {
    ///         skiplist.remove_first(&i);
    ///     }
    /// }
    /// assert_eq!(skiplist.remove_first(&4), Some(4)); // Removes the last one
    /// assert_eq!(skiplist.remove_first(&4), None); // No more '4' left
    /// ```
    pub fn remove_first(&mut self, value: &T) -> Option<T> {
        if self.len == 0 {
            return None;
        }

        // This is essentially identical to `remove`, except for a slightly different logic to
        // determining the actual return node in the Ordering::Equal branch of the match statement.
        unsafe {
            let mut node: *mut SkipNode<T> = mem::transmute_copy(&mut self.head);
            let mut return_node: Option<*mut SkipNode<T>> = None;
            let mut prev_nodes: Vec<*mut SkipNode<T>> =
                Vec::with_capacity(self.level_generator.total());

            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                if let Some(return_node) = return_node {
                    while let Some(next) = (*node).links[lvl] {
                        if next == return_node {
                            prev_nodes.push(node);
                            break;
                        } else {
                            node = next;
                        }
                    }
                } else {
                    while let Some(next) = (*node).links[lvl] {
                        if let Some(ref next_value) = (*next).value {
                            match (self.compare)(next_value, value) {
                                Ordering::Less => {
                                    node = next;
                                    continue;
                                }
                                Ordering::Equal => {
                                    if let Some(ref prev_value) = (*(*next).prev.unwrap()).value {
                                        if (self.compare)(prev_value, next_value) == Ordering::Equal
                                        {
                                            prev_nodes.push(node);
                                            break;
                                        }
                                    }
                                    return_node = Some(next);
                                    prev_nodes.push(node);
                                    break;
                                }
                                Ordering::Greater => {
                                    prev_nodes.push(node);
                                    break;
                                }
                            }
                        }
                    }
                    if (*node).links[lvl].is_none() {
                        prev_nodes.push(node);
                        continue;
                    }
                }
            }

            if let Some(return_node) = return_node {
                for (lvl, &prev_node) in prev_nodes.iter().rev().enumerate() {
                    if (*prev_node).links[lvl] == Some(return_node) {
                        (*prev_node).links[lvl] = (*return_node).links[lvl];
                        (*prev_node).links_len[lvl] += (*return_node).links_len[lvl] - 1;
                    } else {
                        (*prev_node).links_len[lvl] -= 1;
                    }
                }
                if let Some(next_node) = (*return_node).links[0] {
                    (*next_node).prev = (*return_node).prev;
                }
                self.len -= 1;
                mem::replace(
                    &mut (*(*return_node).prev.unwrap()).next,
                    mem::replace(&mut (*return_node).next, None),
                )
                .expect("Popped node shouldn't be None.")
                .into_inner()
            } else {
                None
            }
        }
    }

    /// Removes and returns an element with the given index.
    ///
    /// # Panics
    ///
    /// Panics is the index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// assert_eq!(skiplist.remove_index(&4), 4);
    /// assert_eq!(skiplist.remove_index(&4), 5);
    /// ```
    pub fn remove_index(&mut self, index: &usize) -> T {
        unsafe {
            if index >= &self.len() {
                panic!("Index out of bounds.");
            } else {
                let mut node: *mut SkipNode<T> = mem::transmute_copy(&self.head);
                let mut return_node: *mut SkipNode<T> = mem::transmute_copy(&self.head);
                let mut index_sum = 0;
                let mut lvl = self.level_generator.total();
                while lvl > 0 {
                    lvl -= 1;
                    while &(index_sum + (*node).links_len[lvl]) < index {
                        index_sum += (*node).links_len[lvl];
                        node = (*node).links[lvl].unwrap();
                    }
                    // At this point, node has a reference to the either desired index or beyond it.
                    if &(index_sum + (*node).links_len[lvl]) == index {
                        if let Some(next) = (*node).links[lvl] {
                            return_node = next;
                            (*node).links[lvl] = (*next).links[lvl];
                            (*node).links_len[lvl] += (*next).links_len[lvl] - 1;
                        }
                    } else {
                        (*node).links_len[lvl] -= 1;
                    }
                }

                if let Some(next) = (*return_node).links[0] {
                    (*next).prev = (*return_node).prev;
                }
                self.len -= 1;
                mem::replace(
                    &mut (*(*return_node).prev.unwrap()).next,
                    mem::replace(&mut (*return_node).next, None),
                )
                .unwrap()
                .into_inner()
                .unwrap()
            }
        }
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all elements `e` such that `f(&e)` returns false.
    /// This method operates in place.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// skiplist.retain(|&x| x%2 == 0);
    /// ```
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        unsafe {
            let mut removed_nodes = Vec::new();

            // Since we have to check every element anyway, we parse this list bottom-up.  This
            // allows for link lengths to be adjusted on lvl 0 as appropriate and then calculated
            // on subsequent levels.
            for lvl in 0..self.level_generator.total() {
                let mut node: *mut SkipNode<T> = mem::transmute_copy(&mut self.head);
                loop {
                    // If next will be removed, we update links[lvl] to be that node's links[lvl],
                    // and we repeat until links[lvl] point to a node which will be retained.
                    if let Some(next) = (*node).links[lvl] {
                        if let Some(ref value) = (*next).value {
                            if !f(value) {
                                (*node).links[lvl] = (*next).links[lvl];
                                if lvl == 0 {
                                    removed_nodes.push(next);
                                }
                                continue;
                            }
                        }
                    }
                    // At this point, links[lvl] points to a node which we know will be retained
                    // (or None), so we update all the appropriate links.
                    (*node).links_len[lvl] =
                        self.link_length(node, (*node).links[lvl], lvl).unwrap();
                    // And finally proceed to the next node.
                    if let Some(next) = (*node).links[lvl] {
                        node = next;
                    } else {
                        break;
                    }
                }
            }

            self.len -= removed_nodes.len();
            // It now remains to adjust .prev and .next.
            for node in removed_nodes {
                if let Some(next) = (*node).links[0] {
                    (*next).prev = (*node).prev;
                }
                if let Some(prev) = (*node).prev {
                    mem::replace(&mut (*prev).next, mem::replace(&mut (*node).next, None));
                }
            }
        }
    }

    /// Removes all repeated elements in the skiplist using the skiplist's comparison function.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..5);
    /// skiplist.insert(3);
    /// skiplist.dedup();
    /// ```
    pub fn dedup(&mut self) {
        // This follows the same algorithm as `retain` initially to find the nodes to removed (on
        // lvl 0) and then on higher levels checks whether `next` is among the removed nodes.
        unsafe {
            let mut removed_nodes = Vec::new();

            // Since we have to check every element anyway, we parse this list bottom-up.  This
            // allows for link lengths to be adjusted on lvl 0 as appropriate and then calculated
            // on subsequent levels.
            for lvl in 0..self.level_generator.total() {
                let mut node: *mut SkipNode<T> = mem::transmute_copy(&mut self.head);
                loop {
                    // If next will be removed, we update links[lvl] to be that node's links[lvl],
                    // and we repeat until links[lvl] point to a node which will be retained.
                    if let Some(next) = (*node).links[lvl] {
                        if lvl == 0 {
                            if let (&Some(ref a), &Some(ref b)) = (&(*node).value, &(*next).value) {
                                if (self.compare)(a, b) == Ordering::Equal {
                                    (*node).links[lvl] = (*next).links[lvl];
                                    removed_nodes.push(next);
                                    continue;
                                }
                            }
                        } else {
                            let mut next_is_removed = false;
                            for &removed in &removed_nodes {
                                if next == removed {
                                    next_is_removed = true;
                                    break;
                                }
                            }
                            if next_is_removed {
                                (*node).links[lvl] = (*next).links[lvl];
                                continue;
                            }
                        }
                    }
                    // At this point, links[lvl] points to a node which we know will be retained
                    // (or None), so we update all the appropriate links.
                    (*node).links_len[lvl] =
                        self.link_length(node, (*node).links[lvl], lvl).unwrap();
                    // And finally proceed to the next node.
                    if let Some(next) = (*node).links[lvl] {
                        node = next;
                    } else {
                        break;
                    }
                }
            }

            self.len -= removed_nodes.len();
            // It now remains to adjust .prev and .next.
            for node in removed_nodes {
                if let Some(next) = (*node).links[0] {
                    (*next).prev = (*node).prev;
                }
                if let Some(prev) = (*node).prev {
                    mem::replace(&mut (*prev).next, mem::replace(&mut (*node).next, None));
                }
            }
        }
    }

    /// Get an owning iterator over the entries of the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// for i in skiplist.into_iter() {
    ///     println!("Value: {}", i);
    /// }
    /// ```
    pub fn into_iter(mut self) -> IntoIter<T> {
        IntoIter {
            head: unsafe { mem::transmute_copy(&mut self.head) },
            end: self.get_last() as *mut SkipNode<T>,
            size: self.len(),
            skiplist: self,
        }
    }

    /// Creates an iterator over the entries of the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// for i in skiplist.iter() {
    ///     println!("Value: {}", i);
    /// }
    /// ```
    pub fn iter(&self) -> Iter<T> {
        Iter {
            start: unsafe { mem::transmute_copy(&self.head) },
            end: self.get_last(),
            size: self.len(),
            _lifetime: PhantomData,
        }
    }

    /// Constructs a double-ended iterator over a sub-range of elements in the skiplist, starting
    /// at min, and ending at max. If min is `Unbounded`, then it will be treated as "negative
    /// infinity", and if max is `Unbounded`, then it will be treated as "positive infinity".  Thus
    /// range(Unbounded, Unbounded) will yield the whole collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::OrderedSkipList;
    /// use std::collections::Bound::{Included, Unbounded};
    ///
    /// let mut skiplist = OrderedSkipList::new();
    /// skiplist.extend(0..10);
    /// for i in skiplist.range(Included(&3), Included(&7)) {
    ///     println!("Value: {}", i);
    /// }
    /// assert_eq!(Some(&4), skiplist.range(Included(&4), Unbounded).next());
    /// ```
    #[cfg(feature = "unstable")]
    pub fn range(&self, min: Bound<&T>, max: Bound<&T>) -> Iter<T> {
        unsafe {
            // We have to find the start and end nodes.  We use `find_value`; if no node with the
            // given value is present, we are done.  If there is a node, we move to the adjacent
            // nodes until we are before (in the case of included) or at the last node (in the case
            // of exluded).
            let start = match min {
                Bound::Included(min) => {
                    let mut node = self.find_value(min);
                    if let Some(ref value) = (*node).value {
                        if (self.compare)(value, min) == Ordering::Equal {
                            while let Some(prev) = (*node).prev {
                                if let Some(ref value) = (*prev).value {
                                    if (self.compare)(value, min) == Ordering::Equal {
                                        node = prev;
                                        continue;
                                    }
                                }
                                break;
                            }
                            node = (*node).prev.unwrap();
                        }
                    }
                    node
                }
                Bound::Excluded(min) => {
                    let mut node = self.find_value(min);
                    while let Some(next) = (*node).links[0] {
                        if let Some(ref value) = (*next).value {
                            if (self.compare)(value, min) == Ordering::Equal {
                                node = next;
                                continue;
                            }
                        }
                        break;
                    }
                    node
                }
                Bound::Unbounded => mem::transmute_copy(&self.head),
            };
            let end = match max {
                Bound::Included(max) => {
                    let mut node = self.find_value(max);
                    if let Some(ref value) = (*node).value {
                        if (self.compare)(value, max) == Ordering::Equal {
                            while let Some(next) = (*node).links[0] {
                                if let Some(ref value) = (*next).value {
                                    if (self.compare)(value, max) == Ordering::Equal {
                                        node = next;
                                        continue;
                                    }
                                }
                                break;
                            }
                        }
                    }
                    node
                }
                Bound::Excluded(max) => {
                    let mut node = self.find_value(max);
                    if let Some(ref value) = (*node).value {
                        if (self.compare)(value, max) == Ordering::Equal {
                            while let Some(prev) = (*node).prev {
                                if let Some(ref value) = (*prev).value {
                                    if (self.compare)(value, max) == Ordering::Equal {
                                        node = prev;
                                        continue;
                                    }
                                }
                                break;
                            }
                            node = (*node).prev.unwrap();
                        }
                    }
                    node
                }
                Bound::Unbounded => self.get_last(),
            };
            match self.link_length(
                start as *mut SkipNode<T>,
                Some(end as *mut SkipNode<T>),
                cmp::min((*start).level, (*end).level) + 1,
            ) {
                Ok(l) => Iter {
                    start: start,
                    end: end,
                    size: l,
                    _lifetime: PhantomData,
                },
                Err(_) => Iter {
                    start: start,
                    end: start,
                    size: 0,
                    _lifetime: PhantomData,
                },
            }
        }
    }
}

// ///////////////////////////////////////////////
// Internal methods
// ///////////////////////////////////////////////

impl<T> OrderedSkipList<T> {
    /// Checks the integrity of the skiplist.
    fn check(&self) {
        unsafe {
            let mut node: *const SkipNode<T> = mem::transmute_copy(&self.head);
            assert!((*node).is_head() && (*node).value.is_none() && (*node).prev.is_none());

            let mut length_sum;
            for lvl in 0..self.level_generator.total() {
                length_sum = 0;
                node = mem::transmute_copy(&self.head);

                loop {
                    length_sum += (*node).links_len[lvl];
                    assert_eq!((*node).level + 1, (*node).links.len());
                    assert_eq!((*node).level + 1, (*node).links_len.len());
                    assert_eq!(
                        (*node).links_len[lvl],
                        self.link_length(node as *mut SkipNode<T>, (*node).links[lvl], lvl)
                            .unwrap()
                    );

                    if lvl == 0 {
                        assert!((*node).next.is_some() == (*node).links[lvl].is_some());

                        if let Some(prev) = (*node).prev {
                            assert_eq!((*prev).links[lvl], Some(node as *mut SkipNode<T>));
                            assert_eq!(node, mem::transmute_copy((*prev).next.as_ref().unwrap()));
                        }
                    }

                    if let Some(next) = (*node).links[lvl] {
                        assert!((*next).value.is_some());
                        node = next;
                    } else {
                        break;
                    }
                }
                assert_eq!(length_sum, self.len());
            }
        }
    }

    /// In order to find the number of nodes between two given nodes (or the node and the tail), we
    /// can count the link lengths at the level below (assuming that is correct).  For example, if
    /// we have:
    /// ```text
    /// n   : [0] -?-------------------> [4]
    /// n-1 : [0] -1-> [1] -3-> [3] -2-> [4]
    /// ```
    /// Then on level `n`, we know the length will be `1+3+2 = 6`.
    ///
    /// The `lvl` option specifies the level at which we desire to calculate the length and thus
    /// assumes that `lvl-1` is correct.  `lvl=0` is always guaranteed to be correct if all the
    /// `next[0]` links are in order since at level 0, all links lengths are 1.
    ///
    /// If the end node is not encountered, Err(false) is returned.
    fn link_length(
        &self,
        start: *mut SkipNode<T>,
        end: Option<*mut SkipNode<T>>,
        lvl: usize,
    ) -> Result<usize, bool> {
        unsafe {
            let mut length = 0;
            let mut node = start;
            if lvl == 0 {
                while Some(node) != end {
                    length += 1;
                    // Since the head node is not a node proper, the link between it and the next node
                    // (on level 0) is actual 0 hence the offset here.
                    if (*node).is_head() {
                        length -= 1;
                    }
                    match (*node).links[lvl] {
                        Some(ptr) => node = ptr,
                        None => break,
                    }
                }
            } else {
                while Some(node) != end {
                    length += (*node).links_len[lvl - 1];
                    match (*node).links[lvl - 1] {
                        Some(ptr) => node = ptr,
                        None => break,
                    }
                }
            }
            // Check that we actually have calculated the length to the end node we want.
            if let Some(end) = end {
                if node != end {
                    return Err(false);
                }
            }
            Ok(length)
        }
    }

    /// Returns the last node of the skiplist.
    fn get_last(&self) -> *const SkipNode<T> {
        unsafe {
            let mut node: *const SkipNode<T> = mem::transmute_copy(&self.head);

            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                while let Some(next) = (*node).links[lvl] {
                    node = next;
                }
            }
            node
        }
    }

    /// Returns the last node whose value is less than or equal the one specified.  If there are
    /// multiple nodes with the desired value, one of them at random will be returned.
    ///
    /// If the skiplist is empty or if the value being searched for is smaller than all the values
    /// contained in the skiplist, the head node will be returned.
    fn find_value(&self, value: &T) -> *const SkipNode<T> {
        unsafe {
            let mut node: *const SkipNode<T> = mem::transmute_copy(&self.head);

            // Start at the top (least-populated) level and work our way down.
            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                // We parse down the list until we get to a greater value; at that point, we move
                // to the next level down
                while let Some(next) = (*node).links[lvl] {
                    if let &Some(ref next_value) = &(*next).value {
                        match (self.compare)(next_value, value) {
                            Ordering::Less => node = next,
                            Ordering::Equal => {
                                node = next;
                                return node;
                            }
                            Ordering::Greater => break,
                        }
                    } else {
                        panic!("Encountered a value-less node.");
                    }
                }
            }

            node
        }
    }

    /// Gets a pointer to the node with the given index.
    ///
    /// # Panics
    ///
    /// Panics if the index given is out of bounds.
    fn get_index(&self, index: usize) -> *const SkipNode<T> {
        unsafe {
            if index >= self.len() {
                panic!("Index out of bounds.");
            } else {
                let mut node: *const SkipNode<T> = mem::transmute_copy(&self.head);

                let mut index_sum = 0;
                let mut lvl = self.level_generator.total();
                while lvl > 0 {
                    lvl -= 1;

                    while index_sum + (*node).links_len[lvl] <= index {
                        index_sum += (*node).links_len[lvl];
                        node = (*node).links[lvl].unwrap();
                    }
                }
                node
            }
        }
    }
}

impl<T> OrderedSkipList<T>
where
    T: fmt::Debug,
{
    /// Prints out the internal structure of the skiplist (for debugging purposes).
    fn debug_structure(&self) {
        unsafe {
            let mut node: *const SkipNode<T> = mem::transmute_copy(&self.head);
            let mut rows: Vec<_> = iter::repeat(String::new())
                .take(self.level_generator.total())
                .collect();

            loop {
                let value: String;
                if let &Some(ref v) = &(*node).value {
                    value = format!("> [{:?}]", v);
                } else {
                    value = format!("> []");
                }

                let max_str_len = format!("{} -{}-", value, (*node).links_len[(*node).level]).len();

                let mut lvl = self.level_generator.total();
                while lvl > 0 {
                    lvl -= 1;

                    let mut value_len: String;
                    if lvl <= (*node).level {
                        value_len = format!("{} -{}-", value, (*node).links_len[lvl]);
                    } else {
                        value_len = format!("{} -", value);
                    }
                    for _ in 0..(max_str_len - value_len.len()) {
                        value_len.push('-');
                    }

                    let mut dashes = String::new();
                    for _ in 0..value_len.len() {
                        dashes.push('-');
                    }

                    if lvl <= (*node).level {
                        rows[lvl].push_str(value_len.as_ref());
                    } else {
                        rows[lvl].push_str(dashes.as_ref());
                    }
                }

                if let Some(next) = (*node).links[0] {
                    node = next;
                } else {
                    break;
                }
            }

            for row in rows.iter().rev() {
                println!("{}", row);
            }
        }
    }
}

// ///////////////////////////////////////////////
// Trait implementation
// ///////////////////////////////////////////////

unsafe impl<T: Send> Send for OrderedSkipList<T> {}
unsafe impl<T: Sync> Sync for OrderedSkipList<T> {}

impl<T> ops::Drop for OrderedSkipList<T> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let node: *mut SkipNode<T> = mem::transmute_copy(&self.head);

            while let Some(ref mut next) = (*node).next {
                mem::replace(&mut (*node).next, mem::replace(&mut next.next, None));
            }
        }
    }
}

impl<T: PartialOrd> default::Default for OrderedSkipList<T> {
    fn default() -> OrderedSkipList<T> {
        OrderedSkipList::new()
    }
}

/// This implementation of PartialEq only checks that the *values* are equal; it does not check for
/// equivalence of other features (such as the ordering function and the node levels).
/// Furthermore, this uses `T`'s implementation of PartialEq and *does not* use the owning
/// skiplist's comparison function.
impl<A, B> cmp::PartialEq<OrderedSkipList<B>> for OrderedSkipList<A>
where
    A: cmp::PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &OrderedSkipList<B>) -> bool {
        self.len() == other.len() && self.iter().eq(other)
    }
    #[inline]
    fn ne(&self, other: &OrderedSkipList<B>) -> bool {
        self.len != other.len || self.iter().ne(other)
    }
}

impl<T> cmp::Eq for OrderedSkipList<T> where T: cmp::Eq {}

impl<A, B> cmp::PartialOrd<OrderedSkipList<B>> for OrderedSkipList<A>
where
    A: cmp::PartialOrd<B>,
{
    #[inline]
    fn partial_cmp(&self, other: &OrderedSkipList<B>) -> Option<Ordering> {
        self.iter().partial_cmp(other)
    }
}

impl<T> Ord for OrderedSkipList<T>
where
    T: cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &OrderedSkipList<T>) -> Ordering {
        self.iter().cmp(other)
    }
}

impl<T> Extend<T> for OrderedSkipList<T> {
    #[inline]
    fn extend<I: iter::IntoIterator<Item = T>>(&mut self, iterable: I) {
        let iterator = iterable.into_iter();
        for element in iterator {
            self.insert(element);
        }
    }
}

impl<T> ops::Index<usize> for OrderedSkipList<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        unsafe { (*self.get_index(index)).value.as_ref().unwrap() }
    }
}

impl<T> fmt::Debug for OrderedSkipList<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "["));

        for (i, entry) in self.iter().enumerate() {
            if i != 0 {
                try!(write!(f, ", "));
            }
            try!(write!(f, "{:?}", entry));
        }
        write!(f, "]")
    }
}

impl<T> fmt::Display for OrderedSkipList<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "["));

        for (i, entry) in self.iter().enumerate() {
            if i != 0 {
                try!(write!(f, ", "));
            }
            try!(write!(f, "{}", entry));
        }
        write!(f, "]")
    }
}

impl<T> iter::IntoIterator for OrderedSkipList<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        self.into_iter()
    }
}
impl<'a, T> iter::IntoIterator for &'a OrderedSkipList<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}
impl<'a, T> iter::IntoIterator for &'a mut OrderedSkipList<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<T> iter::FromIterator<T> for OrderedSkipList<T>
where
    T: PartialOrd,
{
    #[inline]
    fn from_iter<I>(iter: I) -> OrderedSkipList<T>
    where
        I: iter::IntoIterator<Item = T>,
    {
        let mut skiplist = OrderedSkipList::new();
        skiplist.extend(iter);
        skiplist
    }
}

impl<T: Hash> Hash for OrderedSkipList<T> {
    #[inline]
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        for elt in self {
            elt.hash(state);
        }
    }
}

// ///////////////////////////////////////////////
// Extra structs
// ///////////////////////////////////////////////

pub struct Iter<'a, T: 'a> {
    start: *const SkipNode<T>,
    end: *const SkipNode<T>,
    size: usize,
    _lifetime: PhantomData<&'a T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        unsafe {
            if self.start == self.end {
                return None;
            }
            if let Some(next) = (*self.start).links[0] {
                self.start = next;
                if self.size > 0 {
                    self.size -= 1;
                }
                return (*self.start).value.as_ref();
            }
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a, T> DoubleEndedIterator for Iter<'a, T> {
    fn next_back(&mut self) -> Option<&'a T> {
        unsafe {
            if self.end == self.start {
                return None;
            }
            if let Some(prev) = (*self.end).prev {
                let node = self.end;
                if prev as *const SkipNode<T> != self.start {
                    self.size -= 1;
                } else {
                    self.size = 0;
                }
                self.end = prev;
                return (*node).value.as_ref();
            }
            None
        }
    }
}

pub struct IntoIter<T> {
    skiplist: OrderedSkipList<T>,
    head: *mut SkipNode<T>,
    end: *mut SkipNode<T>,
    size: usize,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        unsafe {
            if let Some(next) = (*self.head).links[0] {
                for lvl in 0..self.skiplist.level_generator.total() {
                    if lvl <= (*next).level {
                        (*self.head).links[lvl] = (*next).links[lvl];
                        (*self.head).links_len[lvl] = (*next).links_len[lvl] - 1;
                    } else {
                        (*self.head).links_len[lvl] -= 1;
                    }
                }
                if let Some(next) = (*self.head).links[0] {
                    (*next).prev = Some(self.head);
                }
                self.skiplist.len -= 1;
                self.size -= 1;
                let popped_node = mem::replace(
                    &mut (*self.head).next,
                    mem::replace(&mut (*next).next, None),
                );
                popped_node.expect("Should have a node").value
            } else {
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        unsafe {
            if self.head == self.end {
                return None;
            }
            if let Some(prev) = (*self.end).prev {
                if prev as *const SkipNode<T> != self.head {
                    self.size -= 1;
                } else {
                    self.size = 0;
                }
                self.end = prev;
                (*self.end).links[0] = None;
                let node = mem::replace(&mut (*self.end).next, None);
                return node.unwrap().into_inner();
            }
            None
        }
    }
}

// /////////////////////////////////////////////////////////////////////////////////////////////////
// Tests and Benchmarks
// /////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::OrderedSkipList;
    use std::collections::Bound::{self, Excluded, Included, Unbounded};

    #[test]
    fn basic_small() {
        let mut sl: OrderedSkipList<i64> = OrderedSkipList::new();
        sl.check();
        assert_eq!(sl.remove(&1), None);
        sl.check();
        sl.insert(1);
        sl.check();
        assert_eq!(sl.remove(&1), Some(1));
        sl.check();
        sl.insert(1);
        sl.check();
        sl.insert(2);
        sl.check();
        assert_eq!(sl.remove(&1), Some(1));
        sl.check();
        assert_eq!(sl.remove(&2), Some(2));
        sl.check();
        assert_eq!(sl.remove(&1), None);
        sl.check();
    }

    #[test]
    fn basic_large() {
        let mut sl = OrderedSkipList::new();
        let size = 10_000;
        assert_eq!(sl.len(), 0);

        for i in 0..size {
            sl.insert(i);
            assert_eq!(sl.len(), i + 1);
        }
        sl.check();

        for i in 0..size {
            assert_eq!(sl.remove(&i), Some(i));
            assert_eq!(sl.len(), size - i - 1);
        }
        sl.check();
    }

    #[test]
    fn iter() {
        let size = 10000;

        let sl: OrderedSkipList<_> = (0..size).collect();

        fn test<T>(size: usize, mut iter: T)
        where
            T: Iterator<Item = usize>,
        {
            for i in 0..size {
                assert_eq!(iter.size_hint(), (size - i, Some(size - i)));
                assert_eq!(iter.next().unwrap(), i);
            }
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.next(), None);
        }
        test(size, sl.iter().map(|&i| i));
        test(size, sl.into_iter());
    }

    #[test]
    fn iter_rev() {
        let size = 10000;

        let sl: OrderedSkipList<_> = (0..size).collect();

        fn test<T>(size: usize, mut iter: T)
        where
            T: Iterator<Item = usize>,
        {
            for i in 0..size {
                assert_eq!(iter.size_hint(), (size - i, Some(size - i)));
                assert_eq!(iter.next().unwrap(), size - i - 1);
            }
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.next(), None);
        }
        test(size, sl.iter().rev().map(|&i| i));
        test(size, sl.into_iter().rev());
    }

    #[test]
    fn iter_mixed() {
        let size = 10000;

        let sl: OrderedSkipList<_> = (0..size).collect();

        fn test<T>(size: usize, mut iter: T)
        where
            T: Iterator<Item = usize> + DoubleEndedIterator,
        {
            for i in 0..size / 4 {
                assert_eq!(iter.size_hint(), (size - i * 2, Some(size - i * 2)));
                assert_eq!(iter.next().unwrap(), i);
                assert_eq!(iter.next_back().unwrap(), size - i - 1);
            }
            for i in size / 4..size * 3 / 4 {
                assert_eq!(iter.size_hint(), (size * 3 / 4 - i, Some(size * 3 / 4 - i)));
                assert_eq!(iter.next().unwrap(), i);
            }
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.next(), None);
        }
        test(size, sl.iter().map(|&i| i));
        test(size, sl.into_iter());
    }

    #[test]
    fn range_small() {
        let size = 5;

        let sl: OrderedSkipList<_> = (0..size).collect();

        let mut j = 0;
        for (&v, i) in sl.range(Included(&2), Unbounded).zip(2..size) {
            assert_eq!(v, i);
            j += 1;
        }
        assert_eq!(j, size - 2);
    }

    #[test]
    fn range_1000() {
        let size = 1000;
        let sl: OrderedSkipList<_> = (0..size).collect();

        fn test(sl: &OrderedSkipList<u32>, size: u32, min: Bound<&u32>, max: Bound<&u32>) {
            let mut values = sl.range(min, max).map(|&i| i);
            let mut expects = 0..size;

            for (v, e) in values.by_ref().zip(expects.by_ref()) {
                assert_eq!(v, e);
            }
            assert_eq!(values.next(), None);
            assert_eq!(expects.next(), None);
        }
        test(&sl, size, Included(&0), Excluded(&size));
        test(&sl, size, Unbounded, Excluded(&size));
        test(&sl, size, Included(&0), Included(&(size - 1)));
        test(&sl, size, Unbounded, Included(&(size - 1)));
        test(&sl, size, Included(&0), Unbounded);
        test(&sl, size, Unbounded, Unbounded);
    }

    #[test]
    fn range() {
        let size = 200;
        let sl: OrderedSkipList<_> = (0..size).collect();

        for i in 0..size {
            for j in 0..size {
                let mut values = sl.range(Included(&i), Included(&j)).map(|&i| i);
                let mut expects = i..(j + 1);

                for (v, e) in values.by_ref().zip(expects.by_ref()) {
                    assert_eq!(v, e);
                }
                assert_eq!(values.next(), None);
                assert_eq!(expects.next(), None);
            }
        }
    }

    #[test]
    fn index() {
        let size = 1000;
        let sl: OrderedSkipList<_> = (0..size).collect();

        for i in 0..size {
            assert_eq!(sl[i], i);
        }
    }

    #[test]
    fn dedup() {
        let size = 1000;
        let repeats = 10;

        let mut sl: OrderedSkipList<usize> = OrderedSkipList::new();
        for _ in 0..repeats {
            sl.extend(0..size);
        }
        {
            let mut iter = sl.iter();
            for i in 0..size {
                for _ in 0..repeats {
                    assert_eq!(iter.next(), Some(&i));
                }
            }
        }
        sl.dedup();
        sl.check();
        let mut iter = sl.iter();
        for i in 0..size {
            assert_eq!(iter.next(), Some(&i));
        }
    }

    #[test]
    fn retain() {
        let repeats = 10;
        let size = 1000;
        let mut sl: OrderedSkipList<usize> = OrderedSkipList::new();
        for _ in 0..repeats {
            sl.extend(0..size);
        }
        {
            let mut iter = sl.iter();
            for i in 0..size {
                for _ in 0..repeats {
                    assert_eq!(iter.next(), Some(&i));
                }
            }
        }
        sl.retain(|&x| x % 5 == 0);
        sl.check();
        assert_eq!(sl.len(), repeats * size / 5);

        {
            let mut iter = sl.iter();
            for i in 0..size / 5 {
                for _ in 0..repeats {
                    assert_eq!(iter.next(), Some(&(i * 5)));
                }
            }
        }
        sl.retain(|&_| false);
        sl.check();
        assert!(sl.is_empty());
    }

    #[test]
    fn remove_index() {
        let size = 100;

        for i in 0..size {
            let mut sl: OrderedSkipList<_> = (0..size).collect();
            assert_eq!(sl.remove_index(&i), i);
            assert_eq!(sl.len(), size - 1);
        }

        let mut sl: OrderedSkipList<_> = (0..size).collect();
        for i in 0..size {
            assert_eq!(sl.remove_index(&0), i);
            assert_eq!(sl.len(), size - i - 1);
            sl.check();
        }
        assert!(sl.is_empty());
    }

    #[test]
    fn pop() {
        let size = 1000;
        let mut sl: OrderedSkipList<_> = (0..size).collect();
        for i in 0..size / 2 {
            assert_eq!(sl.pop_front(), Some(i));
            assert_eq!(sl.pop_back(), Some(size - i - 1));
            assert_eq!(sl.len(), size - 2 * (i + 1));
            sl.check();
        }
        assert!(sl.is_empty());
    }
}

#[cfg(test)]
mod bench {
    extern crate rand;

    use super::*;

    use rand::{weak_rng, Rng};
    use test::{black_box, Bencher};

    #[bench]
    fn index(b: &mut Bencher) {
        let size = 100_000;
        let sl: OrderedSkipList<_> = (0..size).collect();

        b.iter(|| {
            for i in 0..size {
                assert_eq!(sl[i], i);
            }
        });
    }

    fn bench_insert(b: &mut Bencher, base: usize, inserts: usize) {
        let mut sl: OrderedSkipList<u32> = OrderedSkipList::with_capacity(base + inserts);
        let mut rng = weak_rng();

        for _ in 0..base {
            sl.insert(rng.gen());
        }

        b.iter(|| {
            for _ in 0..inserts {
                sl.insert(rng.gen());
            }
        });
    }

    #[bench]
    pub fn insert_0_20(b: &mut Bencher) {
        bench_insert(b, 0, 20);
    }

    #[bench]
    pub fn insert_0_1000(b: &mut Bencher) {
        bench_insert(b, 0, 1_000);
    }

    #[bench]
    pub fn insert_0_100000(b: &mut Bencher) {
        bench_insert(b, 0, 100_000);
    }

    #[bench]
    pub fn insert_100000_20(b: &mut Bencher) {
        bench_insert(b, 100_000, 20);
    }

    fn bench_iter(b: &mut Bencher, size: usize) {
        let mut sl: OrderedSkipList<usize> = OrderedSkipList::with_capacity(size);
        let mut rng = weak_rng();

        for _ in 0..size {
            sl.insert(rng.gen());
        }

        b.iter(|| {
            for entry in &sl {
                black_box(entry);
            }
        });
    }

    #[bench]
    pub fn iter_20(b: &mut Bencher) {
        bench_iter(b, 20);
    }

    #[bench]
    pub fn iter_1000(b: &mut Bencher) {
        bench_iter(b, 1000);
    }

    #[bench]
    pub fn iter_100000(b: &mut Bencher) {
        bench_iter(b, 100000);
    }
}
