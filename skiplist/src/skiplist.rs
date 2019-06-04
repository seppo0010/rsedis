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
// SkipList
// /////////////////////////////////////////////////////////////////////////////////////////////////

/// SkipList provides a way of storing elements and provides efficient way to access, insert and
/// remove nodes.
pub struct SkipList<T> {
    // Storage, this is not sorted
    head: Box<SkipNode<T>>,
    len: usize,
    level_generator: GeometricalLevelGenerator,
}

// ///////////////////////////////////////////////
// Inherent methods
// ///////////////////////////////////////////////

impl<T> SkipList<T> {
    /// Create a new skiplist with the default number of 16 levels.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist: SkipList<i64> = SkipList::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        let lg = GeometricalLevelGenerator::new(16, 1.0 / 2.0);
        SkipList {
            head: Box::new(SkipNode::head(lg.total())),
            len: 0,
            level_generator: lg,
        }
    }

    /// Constructs a new, empty skiplist with the optimal number of levels for the intended
    /// capacity.  Specifically, it uses `floor(log2(capacity))` number of levels, ensuring that
    /// only *a few* nodes occupy the highest level.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::with_capacity(100);
    /// skiplist.extend(0..100);
    /// ```
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let levels = (capacity as f64).log2().floor() as usize;
        let lg = GeometricalLevelGenerator::new(levels, 1.0 / 2.0);
        SkipList {
            head: Box::new(SkipNode::head(lg.total())),
            len: 0,
            level_generator: lg,
        }
    }

    /// Clears the skiplist, removing all values.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
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
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
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
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// assert!(skiplist.is_empty());
    ///
    /// skiplist.push_back(1);
    /// assert!(!skiplist.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Insert the element into the skiplist at the given index, shifting all subsequent nodes down.
    ///
    /// # Panics
    ///
    /// Panics if the insert index is greater than the length of the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    ///
    /// skiplist.insert(0, 0);
    /// skiplist.insert(5, 1);
    /// assert_eq!(skiplist.len(), 2);
    /// assert!(!skiplist.is_empty());
    /// ```
    pub fn insert(&mut self, value: T, index: usize) {
        if index > self.len() {
            panic!("Index out of bounds.");
        }
        unsafe {
            self.len += 1;

            let mut new_node = Box::new(SkipNode::new(value, self.level_generator.random()));
            let new_node_ptr: *mut SkipNode<T> = mem::transmute_copy(&new_node);

            // At each level, `node` moves down the list until it is just prior to where the node
            // will be inserted.  As this is parsed top-down, the link lengths can't yet be
            // adjusted and the insert nodes are stored in `insert_nodes`.
            let mut node: *mut SkipNode<T> = mem::transmute_copy(&mut self.head);
            let mut insert_nodes: Vec<*mut SkipNode<T>> = Vec::with_capacity(new_node.level);

            let mut index_sum = 0;
            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                // Move insert_node down until `next` is not less than the new node.
                while let Some(next) = (*node).links[lvl] {
                    if index_sum + (*node).links_len[lvl] < index {
                        index_sum += (*node).links_len[lvl];
                        node = next;
                        continue;
                    } else {
                        break;
                    }
                }
                // The node level is really just how many links it has.
                // If we've reached the node level, insert it in the links:
                // Before:    [0] ------------> [1]
                // After:     [0] --> [new] --> [1]
                if lvl <= new_node.level {
                    insert_nodes.push(node);
                    new_node.links[lvl] = (*node).links[lvl];
                    (*node).links[lvl] = Some(new_node_ptr);
                } else {
                    (*node).links_len[lvl] += 1;
                }
            }

            // We now parse the insert_nodes from bottom to top, and calculate (and adjust) link
            // lengths.
            for (lvl, &node) in insert_nodes.iter().rev().enumerate() {
                if lvl == 0 {
                    (*node).links_len[lvl] = if (*node).is_head() { 0 } else { 1 };
                    new_node.links_len[lvl] = 1;
                } else {
                    let length = self.link_length(node, Some(new_node_ptr), lvl).unwrap();
                    new_node.links_len[lvl] = (*node).links_len[lvl] - length + 1;
                    (*node).links_len[lvl] = length;
                }
            }

            // Adjust `.prev`
            new_node.prev = Some(node);
            if let Some(next) = (*new_node).links[0] {
                (*next).prev = Some(new_node_ptr);
            }

            // Move the ownerships around, inserting the new node.
            let tmp = mem::replace(&mut (*node).next, Some(new_node));
            if let Some(ref mut node) = (*node).next {
                node.next = tmp;
            }
        }
    }

    /// Insert the element into the front of the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.push_front(1);
    /// skiplist.push_front(2);
    /// ```
    pub fn push_front(&mut self, value: T) {
        self.insert(value, 0);
    }

    /// Insert the element into the back of the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.push_back(1);
    /// skiplist.push_back(2);
    /// ```
    pub fn push_back(&mut self, value: T) {
        let len = self.len();
        self.insert(value, len);
    }

    /// Provides a reference to the front element, or `None` if the skiplist is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// assert_eq!(skiplist.front(), None);
    ///
    /// skiplist.push_back(1);
    /// skiplist.push_back(2);
    /// assert_eq!(skiplist.front(), Some(&1));
    /// ```
    #[inline]
    pub fn front(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            self.get(0)
        }
    }

    /// Provides a mutable reference to the front element, or `None` if the skiplist is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// assert_eq!(skiplist.front(), None);
    ///
    /// skiplist.push_back(1);
    /// skiplist.push_back(2);
    /// assert_eq!(skiplist.front_mut(), Some(&mut 1));
    /// ```
    #[inline]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        if self.is_empty() {
            None
        } else {
            self.get_mut(0)
        }
    }

    /// Provides a reference to the back element, or `None` if the skiplist is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// assert_eq!(skiplist.back(), None);
    ///
    /// skiplist.push_back(1);
    /// skiplist.push_back(2);
    /// assert_eq!(skiplist.back(), Some(&2));
    /// ```
    #[inline]
    pub fn back(&self) -> Option<&T> {
        let len = self.len();
        if len > 0 {
            self.get(len - 1)
        } else {
            None
        }
    }

    /// Provides a reference to the back element, or `None` if the skiplist is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// assert_eq!(skiplist.back(), None);
    ///
    /// skiplist.push_back(1);
    /// skiplist.push_back(2);
    /// assert_eq!(skiplist.back_mut(), Some(&mut 2));
    /// ```
    #[inline]
    pub fn back_mut(&mut self) -> Option<&mut T> {
        let len = self.len();
        if len > 0 {
            self.get_mut(len - 1)
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
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// assert_eq!(skiplist.get(0), None);
    /// skiplist.extend(0..10);
    /// assert_eq!(skiplist.get(0), Some(&0));
    /// assert_eq!(skiplist.get(10), None);
    /// ```
    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        let len = self.len();
        if index < len {
            unsafe { (*self.get_index(index)).value.as_ref() }
        } else {
            None
        }
    }

    /// Provides a mutable reference to the element at the given index, or `None` if the skiplist
    /// is empty or the index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// assert_eq!(skiplist.get_mut(0), None);
    /// skiplist.extend(0..10);
    /// assert_eq!(skiplist.get_mut(0), Some(&mut 0));
    /// assert_eq!(skiplist.get_mut(10), None);
    /// ```
    #[inline]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let len = self.len();
        if index < len {
            unsafe {
                (*(self.get_index(index) as *mut SkipNode<T>))
                    .value
                    .as_mut()
            }
        } else {
            None
        }
    }

    /// Removes the first element and returns it, or `None` if the sequence is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.push_back(1);
    /// skiplist.push_back(2);
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
            Some(self.remove(0))
        }
    }

    /// Removes the last element and returns it, or `None` if the sequence is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.push_back(1);
    /// skiplist.push_back(2);
    ///
    /// assert_eq!(skiplist.pop_back(), Some(2));
    /// assert_eq!(skiplist.pop_back(), Some(1));
    /// assert_eq!(skiplist.pop_back(), None);
    /// ```
    #[inline]
    pub fn pop_back(&mut self) -> Option<T> {
        let len = self.len();
        if len > 0 {
            Some(self.remove(len - 1))
        } else {
            None
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
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.extend(0..10);
    /// assert_eq!(skiplist.remove(4), 4);
    /// assert_eq!(skiplist.remove(4), 5);
    /// ```
    pub fn remove(&mut self, index: usize) -> T {
        unsafe {
            if index >= self.len() {
                panic!("Index out of bounds.");
            } else {
                let mut node: *mut SkipNode<T> = mem::transmute_copy(&self.head);
                let mut return_node: *mut SkipNode<T> = mem::transmute_copy(&self.head);
                let mut index_sum = 0;
                let mut lvl = self.level_generator.total();
                while lvl > 0 {
                    lvl -= 1;
                    while index_sum + (*node).links_len[lvl] < index {
                        index_sum += (*node).links_len[lvl];
                        node = (*node).links[lvl].unwrap();
                    }
                    // At this point, node has a reference to the either desired index or beyond it.
                    if index_sum + (*node).links_len[lvl] == index {
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
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
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

    /// Get an owning iterator over the entries of the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
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
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
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

    /// Creates an mutable iterator over the entries of the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.extend(0..10);
    /// for i in skiplist.iter_mut() {
    ///     println!("Value: {}", i);
    /// }
    /// ```
    pub fn iter_mut(&self) -> IterMut<T> {
        IterMut {
            start: unsafe { mem::transmute_copy(&self.head) },
            end: self.get_last() as *mut SkipNode<T>,
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
    /// use skiplist::SkipList;
    /// use std::collections::Bound::{Included, Unbounded};
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.extend(0..10);
    /// for i in skiplist.range(Included(3), Included(7)) {
    ///     println!("Value: {}", i);
    /// }
    /// assert_eq!(Some(&4), skiplist.range(Included(4), Unbounded).next());
    /// ```
    #[cfg(feature = "unstable")]
    pub fn range(&self, min: Bound<usize>, max: Bound<usize>) -> Iter<T> {
        unsafe {
            // We have to find the start and end nodes.  We use `find_value`; if no node with the
            // given value is present, we are done.  If there is a node, we move to the adjacent
            // nodes until we are before (in the case of included) or at the last node (in the case
            // of exluded).
            let start = match min {
                Bound::Included(min) => (*self.get_index(min)).prev.unwrap() as *const SkipNode<T>,
                Bound::Excluded(min) => self.get_index(min),
                Bound::Unbounded => mem::transmute_copy(&self.head),
            };
            let end = match max {
                Bound::Included(max) => self.get_index(max),
                Bound::Excluded(max) => {
                    if max == self.len() {
                        self.get_index(max - 1)
                    } else {
                        (*self.get_index(max)).prev.unwrap() as *const SkipNode<T>
                    }
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

    /// Constructs a mutable double-ended iterator over a sub-range of elements in the skiplist,
    /// starting at min, and ending at max. If min is `Unbounded`, then it will be treated as
    /// "negative infinity", and if max is `Unbounded`, then it will be treated as "positive
    /// infinity".  Thus range(Unbounded, Unbounded) will yield the whole collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    /// use std::collections::Bound::{Included, Unbounded};
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.extend(0..10);
    /// for i in skiplist.range_mut(Included(3), Included(7)) {
    ///     println!("Value: {}", i);
    /// }
    /// assert_eq!(Some(&mut 4), skiplist.range_mut(Included(4), Unbounded).next());
    /// ```
    #[cfg(feature = "unstable")]
    pub fn range_mut(&mut self, min: Bound<usize>, max: Bound<usize>) -> IterMut<T> {
        unsafe {
            // We have to find the start and end nodes.  We use `find_value`; if no node with the
            // given value is present, we are done.  If there is a node, we move to the adjacent
            // nodes until we are before (in the case of included) or at the last node (in the case
            // of exluded).
            let start = match min {
                Bound::Included(min) => (*self.get_index(min)).prev.unwrap(),
                Bound::Excluded(min) => self.get_index(min) as *mut SkipNode<T>,
                Bound::Unbounded => mem::transmute_copy(&self.head),
            };
            let end = match max {
                Bound::Included(max) => self.get_index(max) as *mut SkipNode<T>,
                Bound::Excluded(max) => {
                    if max == self.len() {
                        self.get_index(max - 1) as *mut SkipNode<T>
                    } else {
                        (*self.get_index(max)).prev.unwrap()
                    }
                }
                Bound::Unbounded => self.get_last() as *mut SkipNode<T>,
            };
            match self.link_length(
                start as *mut SkipNode<T>,
                Some(end as *mut SkipNode<T>),
                cmp::min((*start).level, (*end).level) + 1,
            ) {
                Ok(l) => IterMut {
                    start: start,
                    end: end,
                    size: l,
                    _lifetime: PhantomData,
                },
                Err(_) => IterMut {
                    start: start,
                    end: start,
                    size: 0,
                    _lifetime: PhantomData,
                },
            }
        }
    }
}

impl<T> SkipList<T>
where
    T: PartialEq,
{
    /// Returns true if the value is contained in the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.extend(0..10);
    /// assert!(skiplist.contains(&4));
    /// assert!(!skiplist.contains(&15));
    /// ```
    pub fn contains(&self, value: &T) -> bool {
        let mut node = &self.head;
        while let Some(ref next) = node.next {
            if node.value.as_ref() == Some(value) {
                return true;
            } else {
                node = next;
            }
        }
        false
    }

    /// Removes all consecutive repeated elements in the skiplist.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipList;
    ///
    /// let mut skiplist = SkipList::new();
    /// skiplist.push_back(0);
    /// skiplist.push_back(0);
    /// assert_eq!(skiplist.len(), 2);
    /// skiplist.dedup();
    /// assert_eq!(skiplist.len(), 1);
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
                                if a == b {
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
}

// ///////////////////////////////////////////////
// Internal methods
// ///////////////////////////////////////////////

impl<T> SkipList<T> {
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

impl<T> SkipList<T>
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

unsafe impl<T: Send> Send for SkipList<T> {}
unsafe impl<T: Sync> Sync for SkipList<T> {}

impl<T> ops::Drop for SkipList<T> {
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

impl<T: PartialOrd> default::Default for SkipList<T> {
    fn default() -> SkipList<T> {
        SkipList::new()
    }
}

/// This implementation of PartialEq only checks that the *values* are equal; it does not check for
/// equivalence of other features (such as the ordering function and the node levels).
/// Furthermore, this uses `T`'s implementation of PartialEq and *does not* use the owning
/// skiplist's comparison function.
impl<A, B> cmp::PartialEq<SkipList<B>> for SkipList<A>
where
    A: cmp::PartialEq<B>,
{
    #[inline]
    fn eq(&self, other: &SkipList<B>) -> bool {
        self.len() == other.len() && self.iter().eq(other)
    }
    #[inline]
    fn ne(&self, other: &SkipList<B>) -> bool {
        self.len != other.len || self.iter().eq(other)
    }
}

impl<T> cmp::Eq for SkipList<T> where T: cmp::Eq {}

impl<A, B> cmp::PartialOrd<SkipList<B>> for SkipList<A>
where
    A: cmp::PartialOrd<B>,
{
    #[inline]
    fn partial_cmp(&self, other: &SkipList<B>) -> Option<Ordering> {
        self.iter().partial_cmp(other)
    }
}

impl<T> Ord for SkipList<T>
where
    T: cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &SkipList<T>) -> Ordering {
        self.iter().cmp(other)
    }
}

impl<T> Extend<T> for SkipList<T> {
    #[inline]
    fn extend<I: iter::IntoIterator<Item = T>>(&mut self, iterable: I) {
        let iterator = iterable.into_iter();
        for element in iterator {
            self.push_back(element);
        }
    }
}

impl<T> ops::Index<usize> for SkipList<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        unsafe { (*self.get_index(index)).value.as_ref().unwrap() }
    }
}

impl<T> fmt::Debug for SkipList<T>
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

impl<T> fmt::Display for SkipList<T>
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

impl<T> iter::IntoIterator for SkipList<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        self.into_iter()
    }
}
impl<'a, T> iter::IntoIterator for &'a SkipList<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}
impl<'a, T> iter::IntoIterator for &'a mut SkipList<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<T> iter::FromIterator<T> for SkipList<T>
where
    T: PartialOrd,
{
    #[inline]
    fn from_iter<I>(iter: I) -> SkipList<T>
    where
        I: iter::IntoIterator<Item = T>,
    {
        let mut skiplist = SkipList::new();
        skiplist.extend(iter);
        skiplist
    }
}

impl<T: Hash> Hash for SkipList<T> {
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

pub struct IterMut<'a, T: 'a> {
    start: *mut SkipNode<T>,
    end: *mut SkipNode<T>,
    size: usize,
    _lifetime: PhantomData<&'a T>,
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<&'a mut T> {
        unsafe {
            if self.start == self.end {
                return None;
            }
            if let Some(next) = (*self.start).links[0] {
                self.start = next;
                self.size -= 1;
                return (*self.start).value.as_mut();
            }
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a, T> DoubleEndedIterator for IterMut<'a, T> {
    fn next_back(&mut self) -> Option<&'a mut T> {
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
                return (*node).value.as_mut();
            }
            None
        }
    }
}
pub struct IntoIter<T> {
    skiplist: SkipList<T>,
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
    use super::SkipList;
    use std::collections::Bound::{self, Excluded, Included, Unbounded};

    #[test]
    fn basic_small() {
        let mut sl: SkipList<i64> = SkipList::new();
        sl.check();
        sl.insert(1, 0);
        sl.check();
        assert_eq!(sl.remove(0), 1);
        sl.check();
        sl.insert(1, 0);
        sl.check();
        sl.insert(2, 1);
        sl.check();
        assert_eq!(sl.remove(0), 1);
        sl.check();
        assert_eq!(sl.remove(0), 2);
        sl.check();
    }

    #[test]
    fn basic_large() {
        let mut sl = SkipList::new();
        let size = 500;
        assert_eq!(sl.len(), 0);

        for i in 0..size {
            sl.insert(i, i);
            assert_eq!(sl.len(), i + 1);
        }
        sl.check();

        for i in 0..size {
            assert_eq!(sl.remove(0), i);
            assert_eq!(sl.len(), size - i - 1);
        }
        sl.check();

        for i in 0..size {
            sl = (0..size).collect();
            assert_eq!(sl.remove(i), i);
        }
    }

    #[test]
    fn iter() {
        let size = 10000;

        let sl: SkipList<_> = (0..size).collect();

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
        test(size, sl.iter_mut().map(|&mut i| i));
        test(size, sl.into_iter());
    }

    #[test]
    fn iter_rev() {
        let size = 10000;

        let sl: SkipList<_> = (0..size).collect();

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
        test(size, sl.iter_mut().rev().map(|&mut i| i));
        test(size, sl.into_iter().rev());
    }

    #[test]
    fn iter_mixed() {
        let size = 10000;

        let sl: SkipList<_> = (0..size).collect();

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
        test(size, sl.iter_mut().map(|&mut i| i));
        test(size, sl.into_iter());
    }

    #[test]
    fn range_small() {
        let size = 5;

        let sl: SkipList<_> = (0..size).collect();

        let mut j = 0;
        for (&v, i) in sl.range(Included(2), Unbounded).zip(2..size) {
            assert_eq!(v, i);
            j += 1;
        }
        assert_eq!(j, size - 2);
    }

    #[test]
    fn range_1000() {
        let size = 1000;
        let sl: SkipList<_> = (0..size).collect();

        fn test(sl: &SkipList<usize>, size: usize, min: Bound<usize>, max: Bound<usize>) {
            let mut values = sl.range(min, max).map(|&i| i);
            let mut expects = 0..size;

            for (v, e) in values.by_ref().zip(expects.by_ref()) {
                assert_eq!(v, e);
            }
            assert_eq!(values.next(), None);
            assert_eq!(expects.next(), None);
        }
        test(&sl, size, Included(0), Excluded(size));
        test(&sl, size, Unbounded, Excluded(size));
        test(&sl, size, Included(0), Included(size - 1));
        test(&sl, size, Unbounded, Included(size - 1));
        test(&sl, size, Included(0), Unbounded);
        test(&sl, size, Unbounded, Unbounded);
    }

    #[test]
    fn range() {
        let size = 200;
        let sl: SkipList<_> = (0..size).collect();

        for i in 0..size {
            for j in 0..size {
                let mut values = sl.range(Included(i), Included(j)).map(|&i| i);
                let mut expects = i..(j + 1);

                for (v, e) in values.by_ref().zip(expects.by_ref()) {
                    assert_eq!(v, e);
                }
                assert_eq!(values.next(), None);
                assert_eq!(expects.next(), None);
            }
        }

        for i in 0..size {
            for j in 0..size {
                let mut values = sl.range(Included(i), Included(j)).rev().map(|&i| i);
                let mut expects = (i..(j + 1)).rev();

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
        let sl: SkipList<_> = (0..size).collect();

        for i in 0..size {
            assert_eq!(sl[i], i);
        }
    }

    #[test]
    fn dedup() {
        let size = 1000;
        let repeats = 10;

        let mut sl: SkipList<usize> = SkipList::new();
        for i in 0..size {
            for _ in 0..repeats {
                sl.insert(i, i * repeats);
            }
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

        let mut sl: SkipList<usize> = SkipList::new();
        for i in 0..size {
            for _ in 0..repeats {
                sl.insert(i, i * repeats);
            }
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
    fn pop() {
        let size = 1000;
        let mut sl: SkipList<_> = (0..size).collect();
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
        let sl: SkipList<_> = (0..size).collect();

        b.iter(|| {
            for i in 0..size {
                assert_eq!(sl[i], i);
            }
        });
    }

    fn bench_push_front(b: &mut Bencher, base: usize, inserts: usize) {
        let mut sl: SkipList<u32> = SkipList::with_capacity(base + inserts);
        let mut rng = weak_rng();

        for _ in 0..base {
            sl.push_front(rng.gen());
        }

        b.iter(|| {
            for _ in 0..inserts {
                sl.push_front(rng.gen());
            }
        });
    }

    fn bench_push_back(b: &mut Bencher, base: usize, inserts: usize) {
        let mut sl: SkipList<u32> = SkipList::with_capacity(base + inserts);
        let mut rng = weak_rng();

        for _ in 0..base {
            sl.push_back(rng.gen());
        }

        b.iter(|| {
            for _ in 0..inserts {
                sl.push_back(rng.gen());
            }
        });
    }

    #[bench]
    pub fn push_front_0_20(b: &mut Bencher) {
        bench_push_front(b, 0, 20);
    }

    #[bench]
    pub fn push_front_0_1000(b: &mut Bencher) {
        bench_push_front(b, 0, 1_000);
    }

    #[bench]
    pub fn push_front_0_100000(b: &mut Bencher) {
        bench_push_front(b, 0, 100_000);
    }

    #[bench]
    pub fn push_front_100000_20(b: &mut Bencher) {
        bench_push_front(b, 100_000, 20);
    }

    #[bench]
    pub fn push_back_0_20(b: &mut Bencher) {
        bench_push_back(b, 0, 20);
    }

    #[bench]
    pub fn push_back_0_1000(b: &mut Bencher) {
        bench_push_back(b, 0, 1_000);
    }

    #[bench]
    pub fn push_back_0_100000(b: &mut Bencher) {
        bench_push_back(b, 0, 100_000);
    }

    #[bench]
    pub fn push_back_100000_20(b: &mut Bencher) {
        bench_push_back(b, 100_000, 20);
    }

    fn bench_iter(b: &mut Bencher, size: usize) {
        let mut sl: SkipList<usize> = SkipList::with_capacity(size);
        let mut rng = weak_rng();

        for _ in 0..size {
            sl.push_back(rng.gen());
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
