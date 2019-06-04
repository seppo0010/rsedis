extern crate rand;
#[cfg(test)]
extern crate test;

use std::borrow::Borrow;
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

// /////////////////////////////////////////////////////////////////////////////////////////////////
// SkipNode
// /////////////////////////////////////////////////////////////////////////////////////////////////

/// Implementation of each skipmap node, containing the key and value, the immediately previous
/// and next nodes, and also a list of links to nodes further down the list.
///
/// The `next` owns the next node.
#[derive(Clone, Debug)]
struct SkipNode<K, V> {
    // key and value should never be None, with the sole exception being the head node.
    key: Option<K>,
    value: Option<V>,
    // how high the node reaches.  This should be equal to the vector length.
    level: usize,
    // The immediately next element (and owns that next node).
    next: Option<Box<SkipNode<K, V>>>,
    // The immediately previous element.
    prev: Option<*mut SkipNode<K, V>>,
    // Vector of links to the next node at the respective level.  This vector *must* be of length
    // `self.level + 1`.  links[0] stores a pointer to the same node as next.
    links: Vec<Option<*mut SkipNode<K, V>>>,
    // The corresponding length of each link
    links_len: Vec<usize>,
}

// ///////////////////////////////////////////////
// Inherent methods
// ///////////////////////////////////////////////

impl<K, V> SkipNode<K, V> {
    /// Create a new head node.
    fn head(total_levels: usize) -> Self {
        SkipNode {
            key: None,
            value: None,
            level: total_levels - 1,
            next: None,
            prev: None,
            links: iter::repeat(None).take(total_levels).collect(),
            links_len: iter::repeat(0).take(total_levels).collect(),
        }
    }

    /// Create a new SkipNode with the given value.  `prev` and `next` will all be `None`.
    fn new(key: K, value: V, level: usize) -> Self {
        SkipNode {
            key: Some(key),
            value: Some(value),
            level: level,
            next: None,
            prev: None,
            links: iter::repeat(None).take(level + 1).collect(),
            links_len: iter::repeat(0).take(level + 1).collect(),
        }
    }

    /// Consumes the node returning the value it contains.
    fn into_inner(self) -> Option<(K, V)> {
        if self.key.is_some() {
            Some((self.key.unwrap(), self.value.unwrap()))
        } else {
            None
        }
    }

    /// Returns `true` is the node is a head-node.
    fn is_head(&self) -> bool {
        self.prev.is_none()
    }

    /// Returns `true` is the node is a tail-node.
    fn is_tail(&self) -> bool {
        self.next.is_none()
    }
}

// ///////////////////////////////////////////////
// Trait implementation
// ///////////////////////////////////////////////

impl<K, V> fmt::Display for SkipNode<K, V>
where
    K: fmt::Display,
    V: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let (&Some(ref k), &Some(ref v)) = (&self.key, &self.value) {
            write!(f, "({}, {})", k, v)
        } else {
            Ok(())
        }
    }
}

// /////////////////////////////////////////////////////////////////////////////////////////////////
// SkipMap
// /////////////////////////////////////////////////////////////////////////////////////////////////

/// The skipmap provides a way of storing element pairs such that they keys are always sorted
/// whilst at the same time providing efficient way to access, insert and removes nodes.
///
/// A particular node can be accessed through the matching key, and since the keys are always
/// sorted, it is also possible to access key-value pairs by index.
///
/// Note that mutable references to keys are not available at all as this could result in a node
/// being left out of the proper ordering.
pub struct SkipMap<K, V> {
    // Storage, this is not sorted
    head: Box<SkipNode<K, V>>,
    len: usize,
    level_generator: GeometricalLevelGenerator,
}

// ///////////////////////////////////////////////
// Inherent methods
// ///////////////////////////////////////////////

impl<K, V> SkipMap<K, V>
where
    K: cmp::Ord,
{
    /// Create a new skipmap with the default number of 16 levels.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap: SkipMap<i64, String> = SkipMap::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        let lg = GeometricalLevelGenerator::new(16, 1.0 / 2.0);
        SkipMap {
            head: Box::new(SkipNode::head(lg.total())),
            len: 0,
            level_generator: lg,
        }
    }

    /// Constructs a new, empty skipmap with the optimal number of levels for the intended
    /// capacity.  Specifically, it uses `floor(log2(capacity))` number of levels, ensuring that
    /// only *a few* nodes occupy the highest level.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::with_capacity(100);
    /// skipmap.extend((0..100).map(|x| (x, x)));
    /// ```
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let levels = (capacity as f64).log2().floor() as usize;
        let lg = GeometricalLevelGenerator::new(levels, 1.0 / 2.0);
        SkipMap {
            head: Box::new(SkipNode::head(lg.total())),
            len: 0,
            level_generator: lg,
        }
    }

    /// Insert the element into the skipmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    ///
    /// skipmap.insert(1, "Hello");
    /// skipmap.insert(2, "World");
    /// assert_eq!(skipmap.len(), 2);
    /// assert!(!skipmap.is_empty());
    /// ```
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        unsafe {
            let mut node: *mut SkipNode<K, V> = mem::transmute_copy(&mut self.head);
            let mut existing_node: Option<*mut SkipNode<K, V>> = None;
            let mut prev_nodes: Vec<*mut SkipNode<K, V>> =
                Vec::with_capacity(self.level_generator.total());

            // We don't know if the value we're looking for is even inside this list until we get
            // to the lowest level.  For this reason, we store where the returned node would be in
            // `prev_nodes` and if we find the desired node, we have reference to all the
            // appropriate nodes to modify.
            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;
                if let Some(existing_node) = existing_node {
                    while let Some(next) = (*node).links[lvl] {
                        if next == existing_node {
                            prev_nodes.push(node);
                            break;
                        } else {
                            node = next;
                            continue;
                        }
                    }
                } else {
                    while let Some(next) = (*node).links[lvl] {
                        if let Some(ref next_key) = (*next).key {
                            match next_key.cmp(&key) {
                                Ordering::Less => {
                                    node = next;
                                    continue;
                                }
                                Ordering::Equal => {
                                    existing_node = Some(next);
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

            // At this point, `existing_node` contains a reference to the node with the same key if
            // it was found, otherwise it is None.
            if let Some(existing_node) = existing_node {
                mem::replace(&mut (*existing_node).value, Some(value))
            } else {
                let mut new_node =
                    Box::new(SkipNode::new(key, value, self.level_generator.random()));
                let new_node_ptr: *mut SkipNode<K, V> = mem::transmute_copy(&new_node);

                for (lvl, &prev_node) in prev_nodes.iter().rev().enumerate() {
                    if lvl <= new_node.level {
                        new_node.links[lvl] = (*prev_node).links[lvl];
                        (*prev_node).links[lvl] = Some(new_node_ptr);

                        if lvl == 0 {
                            new_node.prev = Some(prev_node);
                            if let Some(next) = new_node.links[lvl] {
                                (*next).prev = Some(new_node_ptr);
                            }
                            new_node.links_len[lvl] = 1;
                        } else {
                            let length = self
                                .link_length(prev_node, Some(new_node_ptr), lvl)
                                .unwrap();
                            new_node.links_len[lvl] = (*prev_node).links_len[lvl] - length + 1;
                            (*prev_node).links_len[lvl] = length;
                        }
                    } else {
                        (*prev_node).links_len[lvl] += 1;
                    }
                }

                // Move the ownerships around, inserting the new node.
                let prev_node = (*new_node_ptr).prev.unwrap();
                let tmp = mem::replace(&mut (*prev_node).next, Some(new_node));
                if let Some(ref mut node) = (*prev_node).next {
                    node.next = tmp;
                }
                self.len += 1;
                None
            }
        }
    }
}

impl<K, V> SkipMap<K, V> {
    /// Clears the skipmap, removing all values.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// skipmap.clear();
    /// assert!(skipmap.is_empty());
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        unsafe {
            let node: *mut SkipNode<K, V> = mem::transmute_copy(&self.head);

            while let Some(ref mut next) = (*node).next {
                mem::replace(&mut (*node).next, mem::replace(&mut next.next, None));
            }
        }
        let new_head = Box::new(SkipNode::head(self.level_generator.total()));
        self.len = 0;
        mem::replace(&mut self.head, new_head);
    }

    /// Returns the number of elements in the skipmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// assert_eq!(skipmap.len(), 10);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the skipmap contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// assert!(skipmap.is_empty());
    ///
    /// skipmap.insert(1, "Rust");
    /// assert!(!skipmap.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Provides a reference to the front element, or `None` if the skipmap is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// assert_eq!(skipmap.front(), None);
    ///
    /// skipmap.insert(1, "Hello");
    /// skipmap.insert(2, "World");
    /// assert_eq!(skipmap.front(), Some((&1, &"Hello")));
    /// ```
    #[inline]
    pub fn front(&self) -> Option<(&K, &V)> {
        if self.is_empty() {
            None
        } else {
            let node = self.get_index(0);
            unsafe {
                Some((
                    (*node).key.as_ref().unwrap(),
                    (*node).value.as_ref().unwrap(),
                ))
            }
        }
    }

    /// Provides a mutable reference to the front element, or `None` if the skipmap is empty.
    ///
    /// The reference to the key remains immutable as the keys must remain sorted.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// assert_eq!(skipmap.front(), None);
    ///
    /// skipmap.insert(1, "Hello");
    /// skipmap.insert(2, "World");
    /// assert_eq!(skipmap.front_mut(), Some((&1, &mut "Hello")));
    /// ```
    #[inline]
    pub fn front_mut(&self) -> Option<(&K, &mut V)> {
        if self.is_empty() {
            None
        } else {
            let node = self.get_index(0) as *mut SkipNode<K, V>;
            unsafe {
                Some((
                    (*node).key.as_ref().unwrap(),
                    (*node).value.as_mut().unwrap(),
                ))
            }
        }
    }

    /// Provides a reference to the back element, or `None` if the skipmap is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// assert_eq!(skipmap.back(), None);
    ///
    /// skipmap.insert(1, "Hello");
    /// skipmap.insert(2, "World");
    /// assert_eq!(skipmap.back(), Some((&2, &"World")));
    /// ```
    #[inline]
    pub fn back(&self) -> Option<(&K, &V)> {
        let len = self.len();
        if len > 0 {
            let node = self.get_index(len - 1);
            unsafe {
                Some((
                    (*node).key.as_ref().unwrap(),
                    (*node).value.as_ref().unwrap(),
                ))
            }
        } else {
            None
        }
    }

    /// Provides a reference to the back element, or `None` if the skipmap is empty.
    ///
    /// The reference to the key remains immutable as the keys must remain sorted.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// assert_eq!(skipmap.back(), None);
    ///
    /// skipmap.insert(1, "Hello");
    /// skipmap.insert(2, "World");
    /// assert_eq!(skipmap.back_mut(), Some((&2, &mut "World")));
    /// ```
    #[inline]
    pub fn back_mut(&mut self) -> Option<(&K, &mut V)> {
        let len = self.len();
        if len > 0 {
            let node = self.get_index(len - 1) as *mut SkipNode<K, V>;
            unsafe {
                Some((
                    (*node).key.as_ref().unwrap(),
                    (*node).value.as_mut().unwrap(),
                ))
            }
        } else {
            None
        }
    }

    /// Provides a reference to the element at the given index, or `None` if the skipmap is empty
    /// or the index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// assert_eq!(skipmap.get(&0), None);
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// assert_eq!(skipmap.get(&0), Some(&0));
    /// assert_eq!(skipmap.get(&10), None);
    /// ```
    #[inline]
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        unsafe {
            let mut node: *const SkipNode<K, V> = mem::transmute_copy(&self.head);
            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                while let Some(next) = (*node).links[lvl] {
                    if let Some(ref next_key) = (*next).key {
                        match next_key.borrow().cmp(key) {
                            Ordering::Less => {
                                node = next;
                                continue;
                            }
                            Ordering::Equal => {
                                return (*next).value.as_ref();
                            }
                            Ordering::Greater => break,
                        }
                    }
                }
            }
            None
        }
    }

    /// Provides a reference to the element at the given index, or `None` if the skipmap is empty
    /// or the index is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// assert_eq!(skipmap.get(&0), None);
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// assert_eq!(skipmap.get_mut(&0), Some(&mut 0));
    /// assert_eq!(skipmap.get_mut(&10), None);
    ///
    /// match skipmap.get_mut(&0) {
    ///     Some(x) => *x = 100,
    ///     None => (),
    /// }
    /// assert_eq!(skipmap.get(&0), Some(&100));
    /// ```
    #[inline]
    pub fn get_mut<Q: ?Sized>(&self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        unsafe {
            let mut node: *const SkipNode<K, V> = mem::transmute_copy(&self.head);
            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                while let Some(next) = (*node).links[lvl] {
                    if let Some(ref next_key) = (*next).key {
                        match next_key.borrow().cmp(key) {
                            Ordering::Less => {
                                node = next;
                                continue;
                            }
                            Ordering::Equal => return (*next).value.as_mut(),
                            Ordering::Greater => break,
                        }
                    }
                }
            }
            None
        }
    }

    /// Removes the first element and returns it, or `None` if the sequence is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.insert(1, "Hello");
    /// skipmap.insert(2, "World");
    ///
    /// assert_eq!(skipmap.pop_front(), Some((1, "Hello")));
    /// assert_eq!(skipmap.pop_front(), Some((2, "World")));
    /// assert_eq!(skipmap.pop_front(), None);
    /// ```
    #[inline]
    pub fn pop_front(&mut self) -> Option<(K, V)> {
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
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.insert(1, "Hello");
    /// skipmap.insert(2, "World");
    ///
    /// assert_eq!(skipmap.pop_back(), Some((2, "World")));
    /// assert_eq!(skipmap.pop_back(), Some((1, "Hello")));
    /// assert_eq!(skipmap.pop_back(), None);
    /// ```
    #[inline]
    pub fn pop_back(&mut self) -> Option<(K, V)> {
        let len = self.len();
        if len > 0 {
            Some(self.remove_index(&(len - 1)))
        } else {
            None
        }
    }

    /// Returns true if the value is contained in the skipmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// assert!(skipmap.contains_key(&4));
    /// assert!(!skipmap.contains_key(&15));
    /// ```
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        unsafe {
            let mut node: *mut SkipNode<K, V> = mem::transmute_copy(&self.head);

            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                while let Some(next) = (*node).links[lvl] {
                    if let Some(ref next_key) = (*next).key {
                        match next_key.borrow().cmp(key) {
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
            false
        }
    }

    /// Removes and returns an element with the same value or None if there are no such values in
    /// the skipmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// assert_eq!(skipmap.remove(&4), Some(4)); // Removes the last one
    /// assert_eq!(skipmap.remove(&4), None);    // No more '4' left
    /// ```
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        if self.len == 0 {
            return None;
        }

        unsafe {
            let mut node: *mut SkipNode<K, V> = mem::transmute_copy(&mut self.head);
            let mut return_node: Option<*mut SkipNode<K, V>> = None;
            let mut prev_nodes: Vec<*mut SkipNode<K, V>> =
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
                    // We have not yet found the node, and there are no further nodes at this
                    // level, so the return node (if present) is between `node` and tail.
                    if (*node).links[lvl].is_none() {
                        prev_nodes.push(node);
                        continue;
                    }
                    while let Some(next) = (*node).links[lvl] {
                        if let Some(ref next_key) = (*next).key {
                            match next_key.borrow().cmp(key) {
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
                Some(
                    mem::replace(
                        &mut (*(*return_node).prev.unwrap()).next,
                        mem::replace(&mut (*return_node).next, None),
                    )
                    .unwrap()
                    .into_inner()
                    .unwrap()
                    .1,
                )
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
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// assert_eq!(skipmap.remove_index(&4), (4, 4));
    /// assert_eq!(skipmap.remove_index(&4), (5, 5));
    /// ```
    pub fn remove_index(&mut self, index: &usize) -> (K, V) {
        unsafe {
            if index >= &self.len() {
                panic!("Index out of bounds.");
            } else {
                let mut node: *mut SkipNode<K, V> = mem::transmute_copy(&self.head);
                let mut return_node: *mut SkipNode<K, V> = mem::transmute_copy(&self.head);
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

    /// Get an owning iterator over the entries of the skipmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// for (k, v) in skipmap.into_iter() {
    ///     println!("Key {}, Value: {}", k, v);
    /// }
    /// ```
    pub fn into_iter(mut self) -> IntoIter<K, V> {
        IntoIter {
            head: unsafe { mem::transmute_copy(&mut self.head) },
            end: self.get_last() as *mut SkipNode<K, V>,
            size: self.len(),
            skipmap: self,
        }
    }

    /// Creates an iterator over the entries of the skipmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// for (k, v) in skipmap.iter() {
    ///     println!("Key: {}, Value: {}", k, v);
    /// }
    /// ```
    pub fn iter(&self) -> Iter<K, V> {
        Iter {
            start: unsafe { mem::transmute_copy(&self.head) },
            end: self.get_last(),
            size: self.len(),
            _lifetime_k: PhantomData,
            _lifetime_v: PhantomData,
        }
    }

    /// Creates an mutable iterator over the entries of the skipmap.
    ///
    /// The keys cannot be modified as they must remain in order.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// for (k, v) in skipmap.iter_mut() {
    ///     println!("Key: {}, Value: {}", k, v);
    /// }
    /// ```
    pub fn iter_mut(&self) -> IterMut<K, V> {
        IterMut {
            start: unsafe { mem::transmute_copy(&self.head) },
            end: self.get_last() as *mut SkipNode<K, V>,
            size: self.len(),
            _lifetime_k: PhantomData,
            _lifetime_v: PhantomData,
        }
    }

    /// Creates an iterator over the keys of the skipmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// for k in skipmap.keys() {
    ///     println!("Key: {}", k);
    /// }
    /// ```
    pub fn keys(&self) -> Keys<K, V> {
        Keys {
            start: unsafe { mem::transmute_copy(&self.head) },
            end: self.get_last(),
            size: self.len(),
            _lifetime_k: PhantomData,
        }
    }

    /// Creates an iterator over the values of the skipmap.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// for v in skipmap.values() {
    ///     println!("Value: {}", v);
    /// }
    /// ```
    pub fn values(&self) -> Values<K, V> {
        Values {
            start: unsafe { mem::transmute_copy(&self.head) },
            end: self.get_last(),
            size: self.len(),
            _lifetime_v: PhantomData,
        }
    }

    /// Constructs a double-ended iterator over a sub-range of elements in the skipmap, starting
    /// at min, and ending at max. If min is `Unbounded`, then it will be treated as "negative
    /// infinity", and if max is `Unbounded`, then it will be treated as "positive infinity".  Thus
    /// range(Unbounded, Unbounded) will yield the whole collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use skiplist::SkipMap;
    /// use std::collections::Bound::{Included, Unbounded};
    ///
    /// let mut skipmap = SkipMap::new();
    /// skipmap.extend((0..10).map(|x| (x, x)));
    /// for (k, v) in skipmap.range(Included(&3), Included(&7)) {
    ///     println!("Key: {}, Value: {}", k, v);
    /// }
    /// assert_eq!(Some((&4, &4)), skipmap.range(Included(&4), Unbounded).next());
    /// ```
    #[cfg(feature = "unstable")]
    pub fn range<Q>(&self, min: Bound<&Q>, max: Bound<&Q>) -> Iter<K, V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        unsafe {
            let start = match min {
                Bound::Included(min) => {
                    let mut node = self.find_key(min);
                    if let Some(ref key) = (*node).key {
                        if key.borrow() == min {
                            node = (*node).prev.unwrap();
                        }
                    }
                    node
                }
                Bound::Excluded(min) => self.find_key(min),
                Bound::Unbounded => mem::transmute_copy(&self.head),
            };
            let end = match max {
                Bound::Included(max) => self.find_key(max),
                Bound::Excluded(max) => {
                    let mut node = self.find_key(max);
                    if let Some(ref key) = (*node).key {
                        if key.borrow() == max {
                            node = (*node).prev.unwrap();
                        }
                    }
                    node
                }
                Bound::Unbounded => self.get_last(),
            };
            match self.link_length(
                start as *mut SkipNode<K, V>,
                Some(end as *mut SkipNode<K, V>),
                cmp::min((*start).level, (*end).level) + 1,
            ) {
                Err(_) => Iter {
                    start: start,
                    end: start,
                    size: 0,
                    _lifetime_k: PhantomData,
                    _lifetime_v: PhantomData,
                },
                Ok(l) => Iter {
                    start: start,
                    end: end,
                    size: l,
                    _lifetime_k: PhantomData,
                    _lifetime_v: PhantomData,
                },
            }
        }
    }
}

// ///////////////////////////////////////////////
// Internal methods
// ///////////////////////////////////////////////

impl<K, V> SkipMap<K, V> {
    /// Checks the integrity of the skipmap.
    fn check(&self) {
        unsafe {
            let mut node: *const SkipNode<K, V> = mem::transmute_copy(&self.head);
            assert!((*node).is_head() == (*node).key.is_none());
            assert!((*node).key.is_none() == (*node).value.is_none());
            assert!((*node).value.is_none() == (*node).prev.is_none());

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
                        self.link_length(node as *mut SkipNode<K, V>, (*node).links[lvl], lvl)
                            .unwrap()
                    );

                    if lvl == 0 {
                        assert!((*node).next.is_some() == (*node).links[lvl].is_some());

                        if let Some(prev) = (*node).prev {
                            assert_eq!((*prev).links[lvl], Some(node as *mut SkipNode<K, V>));
                            assert_eq!(node, mem::transmute_copy((*prev).next.as_ref().unwrap()));
                        }
                    }

                    if let Some(next) = (*node).links[lvl] {
                        assert!((*next).key.is_some());
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
        start: *mut SkipNode<K, V>,
        end: Option<*mut SkipNode<K, V>>,
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

    /// Returns the last node of the skipmap.
    fn get_last(&self) -> *const SkipNode<K, V> {
        unsafe {
            let mut node: *const SkipNode<K, V> = mem::transmute_copy(&self.head);

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
    /// If the skipmap is empty or if the value being searched for is smaller than all the values
    /// contained in the skipmap, the head node will be returned.
    fn find_key<Q: ?Sized>(&self, key: &Q) -> *const SkipNode<K, V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        unsafe {
            let mut node: *const SkipNode<K, V> = mem::transmute_copy(&self.head);

            // Start at the top (least-populated) level and work our way down.
            let mut lvl = self.level_generator.total();
            while lvl > 0 {
                lvl -= 1;

                // We parse down the list until we get to a greater value; at that point, we move
                // to the next level down
                while let Some(next) = (*node).links[lvl] {
                    if let &Some(ref next_key) = &(*next).key {
                        match next_key.borrow().cmp(key) {
                            Ordering::Less => node = next,
                            Ordering::Equal => return next,
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
    fn get_index(&self, index: usize) -> *const SkipNode<K, V> {
        unsafe {
            if index >= self.len() {
                panic!("Index out of bounds.");
            } else {
                let mut node: *const SkipNode<K, V> = mem::transmute_copy(&self.head);

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

impl<K, V> SkipMap<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    /// Prints out the internal structure of the skipmap (for debugging purposes).
    fn debug_structure(&self) {
        unsafe {
            let mut node: *const SkipNode<K, V> = mem::transmute_copy(&self.head);
            let mut rows: Vec<_> = iter::repeat(String::new())
                .take(self.level_generator.total())
                .collect();

            loop {
                let value: String;
                if let (&Some(ref k), &Some(ref v)) = (&(*node).key, &(*node).value) {
                    value = format!("> ({:?}, {:?})", k, v);
                } else {
                    value = format!("> ()");
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

unsafe impl<K: Send, V: Send> Send for SkipMap<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for SkipMap<K, V> {}

impl<K, V> ops::Drop for SkipMap<K, V> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            let node: *mut SkipNode<K, V> = mem::transmute_copy(&self.head);

            while let Some(ref mut next) = (*node).next {
                mem::replace(&mut (*node).next, mem::replace(&mut next.next, None));
            }
        }
    }
}

impl<K: Ord, V> default::Default for SkipMap<K, V> {
    fn default() -> SkipMap<K, V> {
        SkipMap::new()
    }
}

/// This implementation of PartialEq only checks that the *values* are equal; it does not check for
/// equivalence of other features (such as the ordering function and the node levels).
/// Furthermore, this uses `T`'s implementation of PartialEq and *does not* use the owning
/// skipmap's comparison function.
impl<AK, AV, BK, BV> cmp::PartialEq<SkipMap<BK, BV>> for SkipMap<AK, AV>
where
    AK: cmp::PartialEq<BK>,
    AV: cmp::PartialEq<BV>,
{
    #[inline]
    fn eq(&self, other: &SkipMap<BK, BV>) -> bool {
        self.len() == other.len()
            && self.iter().map(|x| x.0).eq(other.iter().map(|x| x.0))
            && self.iter().map(|x| x.1).eq(other.iter().map(|x| x.1))
    }
    #[inline]
    fn ne(&self, other: &SkipMap<BK, BV>) -> bool {
        self.len() == other.len()
            || self.iter().map(|x| x.0).ne(other.iter().map(|x| x.0))
            || self.iter().map(|x| x.1).ne(other.iter().map(|x| x.1))
    }
}

impl<K, V> cmp::Eq for SkipMap<K, V>
where
    K: cmp::Eq,
    V: cmp::Eq,
{
}

impl<AK, AV, BK, BV> cmp::PartialOrd<SkipMap<BK, BV>> for SkipMap<AK, AV>
where
    AK: cmp::PartialOrd<BK>,
    AV: cmp::PartialOrd<BV>,
{
    #[inline]
    fn partial_cmp(&self, other: &SkipMap<BK, BV>) -> Option<Ordering> {
        match self
            .iter()
            .map(|x| x.0)
            .partial_cmp(other.iter().map(|x| x.0))
        {
            None => None,
            Some(Ordering::Less) => Some(Ordering::Less),
            Some(Ordering::Greater) => Some(Ordering::Greater),
            Some(Ordering::Equal) => self
                .iter()
                .map(|x| x.1)
                .partial_cmp(other.iter().map(|x| x.1)),
        }
        // match iter::order::partial_cmp(self.iter().map(|x| x.0), other.iter().map(|x| x.0)) {
        //     None => None,
        //     Some(Ordering::Less) => Some(Ordering::Less),
        //     Some(Ordering::Greater) => Some(Ordering::Greater),
        //     Some(Ordering::Equal) => iter::order::partial_cmp(self.iter().map(|x| x.1), other.iter().map(|x| x.1))
        // }
    }
}

impl<K, V> Ord for SkipMap<K, V>
where
    K: cmp::Ord,
    V: cmp::Ord,
{
    #[inline]
    fn cmp(&self, other: &SkipMap<K, V>) -> Ordering {
        self.iter().cmp(other)
    }
}

impl<K, V> Extend<(K, V)> for SkipMap<K, V>
where
    K: Ord,
{
    #[inline]
    fn extend<I: iter::IntoIterator<Item = (K, V)>>(&mut self, iterable: I) {
        let iterator = iterable.into_iter();
        for element in iterator {
            self.insert(element.0, element.1);
        }
    }
}

impl<'a, K, V> ops::Index<usize> for SkipMap<K, V> {
    type Output = V;

    fn index(&self, index: usize) -> &V {
        unsafe { (*self.get_index(index)).value.as_ref().unwrap() }
    }
}

impl<'a, K, V> ops::IndexMut<usize> for SkipMap<K, V> {
    fn index_mut(&mut self, index: usize) -> &mut V {
        unsafe {
            (*(self.get_index(index) as *mut SkipNode<K, V>))
                .value
                .as_mut()
                .unwrap()
        }
    }
}

impl<K, V> fmt::Debug for SkipMap<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "["));

        for (i, (k, v)) in self.iter().enumerate() {
            if i != 0 {
                try!(write!(f, ", "));
            }
            try!(write!(f, "({:?}, {:?})", k, v));
        }
        write!(f, "]")
    }
}

impl<K, V> fmt::Display for SkipMap<K, V>
where
    K: fmt::Display,
    V: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "["));

        for (i, (k, v)) in self.iter().enumerate() {
            if i != 0 {
                try!(write!(f, ", "));
            }
            try!(write!(f, "({}, {})", k, v));
        }
        write!(f, "]")
    }
}

impl<K, V> iter::IntoIterator for SkipMap<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        self.into_iter()
    }
}
impl<'a, K, V> iter::IntoIterator for &'a SkipMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Iter<'a, K, V> {
        self.iter()
    }
}
impl<'a, K, V> iter::IntoIterator for &'a mut SkipMap<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = IterMut<'a, K, V>;

    fn into_iter(self) -> IterMut<'a, K, V> {
        self.iter_mut()
    }
}

impl<K, V> iter::FromIterator<(K, V)> for SkipMap<K, V>
where
    K: Ord,
{
    #[inline]
    fn from_iter<I>(iter: I) -> SkipMap<K, V>
    where
        I: iter::IntoIterator<Item = (K, V)>,
    {
        let mut skipmap = SkipMap::new();
        skipmap.extend(iter);
        skipmap
    }
}

impl<K: Hash, V: Hash> Hash for SkipMap<K, V> {
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

pub struct Iter<'a, K: 'a, V: 'a> {
    start: *const SkipNode<K, V>,
    end: *const SkipNode<K, V>,
    size: usize,
    _lifetime_k: PhantomData<&'a K>,
    _lifetime_v: PhantomData<&'a V>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        unsafe {
            if self.start == self.end {
                return None;
            }
            if let Some(next) = (*self.start).links[0] {
                self.start = next;
                if self.size > 0 {
                    self.size -= 1;
                }
                return Some((
                    (*self.start).key.as_ref().unwrap(),
                    (*self.start).value.as_ref().unwrap(),
                ));
            }
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a, K, V> DoubleEndedIterator for Iter<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a V)> {
        unsafe {
            if self.end == self.start {
                return None;
            }
            if let Some(prev) = (*self.end).prev {
                let node = self.end;
                if prev as *const SkipNode<K, V> != self.start {
                    self.size -= 1;
                } else {
                    self.size = 0;
                }
                self.end = prev;
                if (*node).key.is_some() {
                    return Some((
                        (*node).key.as_ref().unwrap(),
                        (*node).value.as_ref().unwrap(),
                    ));
                }
            }
            None
        }
    }
}

pub struct IterMut<'a, K: 'a, V: 'a> {
    start: *mut SkipNode<K, V>,
    end: *mut SkipNode<K, V>,
    size: usize,
    _lifetime_k: PhantomData<&'a K>,
    _lifetime_v: PhantomData<&'a V>,
}

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
    type Item = (&'a K, &'a mut V);

    fn next(&mut self) -> Option<(&'a K, &'a mut V)> {
        unsafe {
            if self.start == self.end {
                return None;
            }
            if let Some(next) = (*self.start).links[0] {
                self.start = next;
                self.size -= 1;
                return Some((
                    (*self.start).key.as_ref().unwrap(),
                    (*self.start).value.as_mut().unwrap(),
                ));
            }
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a, K, V> DoubleEndedIterator for IterMut<'a, K, V> {
    fn next_back(&mut self) -> Option<(&'a K, &'a mut V)> {
        unsafe {
            if self.end == self.start {
                return None;
            }
            if let Some(prev) = (*self.end).prev {
                let node = self.end;
                if prev as *const SkipNode<K, V> != self.start {
                    self.size -= 1;
                } else {
                    self.size = 0;
                }
                self.end = prev;
                if (*node).key.is_some() {
                    return Some((
                        (*node).key.as_ref().unwrap(),
                        (*node).value.as_mut().unwrap(),
                    ));
                }
            }
            None
        }
    }
}

pub struct IntoIter<K, V> {
    skipmap: SkipMap<K, V>,
    head: *mut SkipNode<K, V>,
    end: *mut SkipNode<K, V>,
    size: usize,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        unsafe {
            if let Some(next) = (*self.head).links[0] {
                for lvl in 0..self.skipmap.level_generator.total() {
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
                self.skipmap.len -= 1;
                self.size -= 1;
                mem::replace(
                    &mut (*self.head).next,
                    mem::replace(&mut (*next).next, None),
                )
                .unwrap()
                .into_inner()
            } else {
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<K, V> DoubleEndedIterator for IntoIter<K, V> {
    fn next_back(&mut self) -> Option<(K, V)> {
        unsafe {
            if self.head == self.end {
                return None;
            }
            if let Some(prev) = (*self.end).prev {
                if prev as *const SkipNode<K, V> != self.head {
                    self.size -= 1;
                } else {
                    self.size = 0;
                }
                self.end = prev;
                (*self.end).links[0] = None;
                return mem::replace(&mut (*self.end).next, None)
                    .unwrap()
                    .into_inner();
            }
            None
        }
    }
}

pub struct Keys<'a, K: 'a, V> {
    start: *const SkipNode<K, V>,
    end: *const SkipNode<K, V>,
    size: usize,
    _lifetime_k: PhantomData<&'a K>,
}

impl<'a, K, V> Iterator for Keys<'a, K, V> {
    type Item = &'a K;

    fn next(&mut self) -> Option<&'a K> {
        unsafe {
            if self.start == self.end {
                return None;
            }
            if let Some(next) = (*self.start).links[0] {
                self.start = next;
                if self.size > 0 {
                    self.size -= 1;
                } else {
                    self.size = 0;
                }
                return (*self.start).key.as_ref();
            }
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}

impl<'a, K, V> DoubleEndedIterator for Keys<'a, K, V> {
    fn next_back(&mut self) -> Option<&'a K> {
        unsafe {
            if self.end == self.start {
                return None;
            }
            if let Some(prev) = (*self.end).prev {
                let node = self.end;
                if prev as *const SkipNode<K, V> != self.start {
                    self.size -= 1;
                } else {
                    self.size = 0;
                }
                self.end = prev;
                return (*node).key.as_ref();
            }
            None
        }
    }
}

pub struct Values<'a, K, V: 'a> {
    start: *const SkipNode<K, V>,
    end: *const SkipNode<K, V>,
    size: usize,
    _lifetime_v: PhantomData<&'a V>,
}

impl<'a, K, V> Iterator for Values<'a, K, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<&'a V> {
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

impl<'a, K, V> DoubleEndedIterator for Values<'a, K, V> {
    fn next_back(&mut self) -> Option<&'a V> {
        unsafe {
            if self.end == self.start {
                return None;
            }
            if let Some(prev) = (*self.end).prev {
                let node = self.end;
                if prev as *const SkipNode<K, V> != self.start {
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

// /////////////////////////////////////////////////////////////////////////////////////////////////
// Tests and Benchmarks
// /////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::SkipMap;
    use std::collections::Bound::{self, Excluded, Included, Unbounded};

    #[test]
    fn basic_small() {
        let mut sm: SkipMap<i64, i64> = SkipMap::new();
        sm.check();
        assert_eq!(sm.remove(&1), None);
        sm.check();
        assert_eq!(sm.insert(1, 0), None);
        sm.check();
        assert_eq!(sm.insert(1, 5), Some(0));
        sm.check();
        assert_eq!(sm.remove(&1), Some(5));
        sm.check();
        assert_eq!(sm.insert(1, 10), None);
        sm.check();
        assert_eq!(sm.insert(2, 20), None);
        sm.check();
        assert_eq!(sm.remove(&1), Some(10));
        sm.check();
        assert_eq!(sm.remove(&2), Some(20));
        sm.check();
        assert_eq!(sm.remove(&1), None);
        sm.check();
    }

    #[test]
    fn basic_large() {
        let mut sm = SkipMap::new();
        let size = 10_000;
        assert_eq!(sm.len(), 0);

        for i in 0..size {
            sm.insert(i, i * 10);
            assert_eq!(sm.len(), i + 1);
        }
        sm.check();

        for i in 0..size {
            assert_eq!(sm.remove(&i), Some(i * 10));
            assert_eq!(sm.len(), size - i - 1);
        }
        sm.check();
    }

    #[test]
    fn iter() {
        let size = 10000;

        let sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();

        fn test<T>(size: usize, mut iter: T)
        where
            T: Iterator<Item = (usize, usize)>,
        {
            for i in 0..size {
                assert_eq!(iter.size_hint(), (size - i, Some(size - i)));
                assert_eq!(iter.next().unwrap(), (i, i));
            }
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.next(), None);
        }
        test(size, sm.iter().map(|(&a, &b)| (a, b)));
        test(size, sm.iter_mut().map(|(&a, &mut b)| (a, b)));
        test(size, sm.into_iter());
    }

    #[test]
    fn iter_rev() {
        let size = 1000;

        let sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();

        fn test<T>(size: usize, mut iter: T)
        where
            T: Iterator<Item = (usize, usize)>,
        {
            for i in 0..size {
                assert_eq!(iter.size_hint(), (size - i, Some(size - i)));
                assert_eq!(iter.next().unwrap(), (size - i - 1, size - i - 1));
            }
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.next(), None);
        }
        test(size, sm.iter().rev().map(|(&a, &b)| (a, b)));
        test(size, sm.iter_mut().rev().map(|(&a, &mut b)| (a, b)));
        test(size, sm.into_iter().rev());
    }

    #[test]
    fn iter_mixed() {
        let size = 1000;

        let sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();

        fn test<T>(size: usize, mut iter: T)
        where
            T: Iterator<Item = (usize, usize)> + DoubleEndedIterator,
        {
            for i in 0..size / 4 {
                assert_eq!(iter.size_hint(), (size - i * 2, Some(size - i * 2)));
                assert_eq!(iter.next().unwrap(), (i, i));
                assert_eq!(iter.next_back().unwrap(), (size - i - 1, size - i - 1));
            }
            for i in size / 4..size * 3 / 4 {
                assert_eq!(iter.size_hint(), (size * 3 / 4 - i, Some(size * 3 / 4 - i)));
                assert_eq!(iter.next().unwrap(), (i, i));
            }
            assert_eq!(iter.size_hint(), (0, Some(0)));
            assert_eq!(iter.next(), None);
        }
        test(size, sm.iter().map(|(&a, &b)| (a, b)));
        test(size, sm.iter_mut().map(|(&a, &mut b)| (a, b)));
        test(size, sm.into_iter());
    }

    #[test]
    fn range_small() {
        let size = 5;

        let sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();

        let mut j = 0;
        for ((&k, &v), i) in sm.range(Included(&2), Unbounded).zip(2..size) {
            assert_eq!(k, i);
            assert_eq!(v, i);
            j += 1;
        }
        assert_eq!(j, size - 2);
    }

    #[test]
    fn range_1000() {
        let size = 1000;
        let sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();

        fn test(sm: &SkipMap<u32, u32>, size: u32, min: Bound<&u32>, max: Bound<&u32>) {
            let mut values = sm.range(min, max).map(|(&a, &b)| (a, b));
            let mut expects = 0..size;

            for ((k, v), e) in values.by_ref().zip(expects.by_ref()) {
                assert_eq!(k, e);
                assert_eq!(v, e);
            }
            assert_eq!(values.next(), None);
            assert_eq!(expects.next(), None);
        }
        test(&sm, size, Included(&0), Excluded(&size));
        test(&sm, size, Unbounded, Excluded(&size));
        test(&sm, size, Included(&0), Included(&(size - 1)));
        test(&sm, size, Unbounded, Included(&(size - 1)));
        test(&sm, size, Included(&0), Unbounded);
        test(&sm, size, Unbounded, Unbounded);
    }

    #[test]
    fn range() {
        let size = 200;
        let sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();

        for i in 0..size {
            for j in 0..size {
                let mut values = sm.range(Included(&i), Included(&j)).map(|(&a, &b)| (a, b));
                let mut expects = i..(j + 1);

                for ((k, v), e) in values.by_ref().zip(expects.by_ref()) {
                    assert_eq!(k, e);
                    assert_eq!(v, e);
                }
                assert_eq!(values.next(), None);
                assert_eq!(expects.next(), None);
            }
        }

        // let mut values = sm.range(Included(&10), Included(&5)).map(|(&a, &b)| (a, b));
        // assert_eq!(values.next(), None);
    }

    #[test]
    fn index() {
        let size = 1000;
        let sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();

        for i in 0..size {
            assert_eq!(sm[i], i);
        }
    }

    #[test]
    fn remove_index() {
        let size = 100;

        for i in 0..size {
            let mut sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();
            assert_eq!(sm.remove_index(&i), (i, i));
            assert_eq!(sm.len(), size - 1);
        }

        let mut sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();
        for i in 0..size {
            assert_eq!(sm.remove_index(&0), (i, i));
            assert_eq!(sm.len(), size - i - 1);
            sm.check();
        }
        assert!(sm.is_empty());
    }

    #[test]
    fn pop() {
        let size = 1000;
        let mut sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();
        for i in 0..size / 2 {
            assert_eq!(sm.pop_front(), Some((i, i)));
            assert_eq!(sm.pop_back(), Some((size - i - 1, size - i - 1)));
            assert_eq!(sm.len(), size - 2 * (i + 1));
            sm.check();
        }
        assert!(sm.is_empty());
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
        let sm: SkipMap<_, _> = (0..size).map(|x| (x, x)).collect();

        b.iter(|| {
            for i in 0..size {
                assert_eq!(sm[i], i);
            }
        });
    }

    fn bench_insert(b: &mut Bencher, base: usize, inserts: usize) {
        let mut sm: SkipMap<u32, u32> = SkipMap::with_capacity(base + inserts);
        let mut rng = weak_rng();

        for _ in 0..base {
            sm.insert(rng.gen(), rng.gen());
        }

        b.iter(|| {
            for _ in 0..inserts {
                sm.insert(rng.gen(), rng.gen());
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
        let mut sm: SkipMap<usize, usize> = SkipMap::with_capacity(size);
        let mut rng = weak_rng();

        for _ in 0..size {
            sm.insert(rng.gen(), rng.gen());
        }

        b.iter(|| {
            for entry in &sm {
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
