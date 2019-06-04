use std::fmt;
use std::iter;

// /////////////////////////////////////////////////////////////////////////////////////////////////
// SkipNode
// /////////////////////////////////////////////////////////////////////////////////////////////////

/// SkipNodes are make up the SkipList.  The SkipList owns the first head-node
/// (which has no value) and each node has ownership of the next node through
/// `next`.
///
/// The node has a `level` which corresponds to how 'high' the node reaches, and
/// this should be equal to the length of the vector of links.  There is a
/// corresponding vector of link lengths which is used to reach a particular index.
///
/// Lastly, each node contains a link to the immediately previous node in case
/// one needs to parse the list backwards.
///
/// In cases where the value is not applicable, `None` should be used.  In
/// particular, as there is no tail node, the value of `next` in the last node
/// should be `None`.
#[derive(Clone, Debug)]
pub struct SkipNode<V> {
    // key and value should never be None, with the sole exception being the head node.
    pub value: Option<V>,
    // how high the node reaches.  This should be equal to the vector length.
    pub level: usize,
    // The immediately next element (and owns that next node).
    pub next: Option<Box<SkipNode<V>>>,
    // The immediately previous element.
    pub prev: Option<*mut SkipNode<V>>,
    // Vector of links to the next node at the respective level.  This vector *must* be of length
    // `self.level + 1`.  links[0] stores a pointer to the same node as next.
    pub links: Vec<Option<*mut SkipNode<V>>>,
    // The corresponding length of each link
    pub links_len: Vec<usize>,
}

// ///////////////////////////////////////////////
// Inherent methods
// ///////////////////////////////////////////////

impl<V> SkipNode<V> {
    /// Create a new head node.
    pub fn head(total_levels: usize) -> Self {
        SkipNode {
            value: None,
            level: total_levels - 1,
            next: None,
            prev: None,
            links: iter::repeat(None).take(total_levels).collect(),
            links_len: iter::repeat(0).take(total_levels).collect(),
        }
    }

    /// Create a new SkipNode with the given value.  The values of `prev` and
    /// `next` will all be `None` and have to be adjusted.
    pub fn new(value: V, level: usize) -> Self {
        SkipNode {
            value: Some(value),
            level: level,
            next: None,
            prev: None,
            links: iter::repeat(None).take(level + 1).collect(),
            links_len: iter::repeat(0).take(level + 1).collect(),
        }
    }

    /// Consumes the node returning the value it contains.
    pub fn into_inner(self) -> Option<V> {
        if self.value.is_some() {
            Some(self.value.unwrap())
        } else {
            None
        }
    }

    /// Returns `true` is the node is a head-node.
    pub fn is_head(&self) -> bool {
        self.prev.is_none()
    }

    /// Returns `true` is the node is a tail-node.
    pub fn is_tail(&self) -> bool {
        self.next.is_none()
    }
}

// ///////////////////////////////////////////////
// Trait implementation
// ///////////////////////////////////////////////

impl<V> fmt::Display for SkipNode<V> where V: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let &Some(ref v) = &self.value {
            write!(f, "{}", v)
        } else {
            Ok(())
        }
    }
}
