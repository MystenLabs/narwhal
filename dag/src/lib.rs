// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use arc_swap::ArcSwap;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use rayon::prelude::*;
use std::{
    ops::Deref,
    sync::{Arc, Weak},
};

pub mod bft;
pub mod node_dag;

/// Reference-counted pointers to a Node
#[derive(Debug)]
pub struct NodeRef<T>(Arc<Node<T>>);

// reimplemented to avoid a clone bound on T
impl<T> Clone for NodeRef<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> std::hash::Hash for NodeRef<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        (Arc::as_ptr(&self.0)).hash(state)
    }
}

impl<T> PartialEq<NodeRef<T>> for NodeRef<T> {
    fn eq(&self, other: &NodeRef<T>) -> bool {
        Arc::as_ptr(&self.0) == Arc::as_ptr(&other.0)
    }
}

impl<T> Eq for NodeRef<T> {}

// The NodeRef is just a wrapper around a smart pointer (only here to define reference equality
// when inserting in a collection).
impl<T> Deref for NodeRef<T> {
    type Target = Arc<Node<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<Arc<Node<T>>> for NodeRef<T> {
    fn from(pointer: Arc<Node<T>>) -> Self {
        NodeRef(pointer)
    }
}

/// Non reference-counted pointers to a Node
pub type WeakNodeRef<T> = Weak<Node<T>>;

impl<T> From<Node<T>> for NodeRef<T> {
    fn from(node: Node<T>) -> Self {
        NodeRef(Arc::new(node))
    }
}

/// The Dag node, aka vertex.
#[derive(Debug)]
pub struct Node<T> {
    /// The antecedents of the Node, aka the edges of the DAG in association list form.
    parents: ArcSwap<Vec<NodeRef<T>>>,
    /// Whether the node is "empty" in some sense: the nodes have a value payload on top of the connections they form.
    /// An "empty" node can be reclaimed in ways that preserve the connectedness of the graph.
    compressible: OnceCell<()>,
    /// The value payload of the node
    value: T,
}

impl<T: Sync + Send + std::fmt::Debug> Node<T> {
    /// Create a new DAG leaf node that contains the given value.
    pub fn new_leaf(value: T, compressible: bool) -> Self {
        Self::new(value, compressible, Vec::default())
    }

    /// Create a new DAG inner node that contains the given value and points to the given parents.
    pub fn new(value: T, compressible: bool, parents: Vec<NodeRef<T>>) -> Self {
        let once_cell = {
            let cell = OnceCell::new();
            if compressible {
                let _ = cell.set(());
            }
            cell
        };
        Self {
            parents: ArcSwap::from_pointee(parents),
            compressible: once_cell,
            value,
        }
    }

    /// Return the value payload of the node
    ///
    /// # Examples
    ///
    /// ```
    /// use dag::Node;
    ///
    /// let node = Node::new_leaf(1, false);
    /// assert_eq!(*node.value(), 1);
    /// ```
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Is the node parent-less?
    ///
    /// # Examples
    ///
    /// ```
    /// use dag::Node;
    ///
    /// let node = Node::new_leaf(1, false);
    /// assert_eq!(node.is_leaf(), true);
    /// ```
    pub fn is_leaf(&self) -> bool {
        self.parents.load().is_empty()
    }

    /// Is the node compressible?
    ///
    /// # Examples
    ///
    /// ```
    /// use dag::Node;
    ///
    /// let node = Node::new_leaf(1, true);
    /// assert_eq!(node.is_compressible(), true);
    /// ```
    pub fn is_compressible(&self) -> bool {
        self.compressible.get().is_some()
    }

    // What's the maximum distance from this to a leaf?
    #[cfg(test)]
    fn height(&self) -> usize {
        if self.is_leaf() {
            1
        } else {
            let max_p_heights = self
                .parents
                .load()
                .iter()
                .map(|p| p.height())
                .max()
                .unwrap_or(1);
            max_p_heights + 1
        }
    }

    /// Get the parent nodes in a [`Vec`]. Note the "parents" are in the reverse of the usual tree structure.
    ///
    /// If this node is a leaf node, this function returns [`Vec::empty()`].
    fn raw_parents_snapshot(&self) -> Vec<NodeRef<T>> {
        self.parents.load().to_vec()
    }

    // A trivial node is one whose parents are all incompressible (or a leaf)
    fn is_trivial(&self) -> bool {
        self.parents
            .load()
            .iter()
            .all(|p| p.is_leaf() || !p.is_compressible())
    }

    /// Compress the path from this node to the next incompressible layer of the DAG.
    /// Returns the parents of the node.
    ///
    /// After path compression, one of these three conditions holds:
    /// * This node is a leaf node;
    /// * This node has only incompressible parents, and keeps them;
    /// * This node has compressible parents, and after path compression, they are replaced by their closest incompressible ancestors.
    pub fn parents(&self) -> Vec<NodeRef<T>> {
        // Quick check to bail the trivial situations out in which:
        // * `self` is itself a leaf node;
        // * The parent nodes of `self` are all incompressible node.
        //
        // In any of the two cases above, we don't have to do anything.
        if self.is_trivial() {
            return self.raw_parents_snapshot();
        }

        let mut res: Vec<NodeRef<T>> = Vec::new();
        // Do the path compression.
        let (compressibles, incompressibles): (Vec<NodeRef<T>>, Vec<NodeRef<T>>) = self
            .raw_parents_snapshot()
            .into_iter()
            .partition(|p| p.is_compressible());

        res.extend(incompressibles);
        // First, compress the path from the parent to some incompressible nodes. After this step, the parents of the
        // parent node should be incompressible.
        let new_parents: Vec<_> = compressibles
            .par_iter()
            .flat_map_iter(|parent| {
                // there are no cycles!
                let these_new_parents: Vec<NodeRef<T>> = { parent.parents() };

                // parent is compressed: it's now trivial
                debug_assert!(parent.is_trivial(), "{:?} is not trivial!", parent);
                // we report its parents to the final parents result, enacting the path compression
                these_new_parents
            })
            .collect();
        res.extend(new_parents);

        let res: Vec<NodeRef<T>> = res.into_iter().unique_by(|arc| Arc::as_ptr(arc)).collect();
        self.parents.store(Arc::new(res));
        debug_assert!(self.is_trivial());
        self.raw_parents_snapshot()
    }
}

/// Returns a Breadth-first search of the DAG, as an iterator of [`NodeRef`]
/// This is expected to be used in conjunction with a [`NodeDag<T>`], walking the graph from one of its heads.
///
pub fn bfs<T: Sync + Send + std::fmt::Debug>(
    initial: NodeRef<T>,
) -> impl Iterator<Item = NodeRef<T>> {
    bft::Bft::new(initial, |node| node.parents().into_iter())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    prop_compose! {
        pub fn arb_leaf_node()(
            value in any::<u64>(),
            compressible in any::<bool>(),
        ) -> Node<u64> {
            Node::new_leaf(value, compressible)
        }
    }

    prop_compose! {
        pub fn arb_inner_node(prior_round: Vec<NodeRef<u64>>)(
            // this is a 50% inclusion rate, in production we'd shoot for > 67%
            picks in prop::collection::vec(any::<bool>(), prior_round.len()..=prior_round.len()),
            value in any::<u64>(),
            compressible in any::<bool>(),
        ) -> Node<u64> {
            let parents = prior_round.iter().zip(picks).flat_map(|(parent, pick)| if pick { Some(parent.clone()) } else { None }).collect();
            Node::new(value, compressible, parents)
        }
    }

    prop_compose! {
        pub fn next_round(prior_round: Vec<NodeRef<u64>>)(
            nodes in { let n = prior_round.len(); prop::collection::vec(arb_inner_node(prior_round), n..=n) }
        ) -> Vec<NodeRef<u64>> {
            nodes.into_iter().map(|node| node.into()).collect()
        }
    }

    pub fn arb_dag_complete(
        authorities: usize,
        rounds: usize,
    ) -> impl Strategy<Value = Vec<NodeRef<u64>>> {
        let initial_round =
            prop::collection::vec(arb_leaf_node().no_shrink(), authorities..=authorities)
                .prop_map(|nodevec| nodevec.into_iter().map(|node| node.into()).collect());

        initial_round.prop_recursive(
            rounds as u32,                 // max rounds level deep
            (authorities * rounds) as u32, // max authorities nodes total
            authorities as u32,            // authorities nodes per round
            move |inner| inner.prop_flat_map(next_round),
        )
    }

    proptest! {
        #[test]
        fn test_dag_sanity_check(
            dag in arb_dag_complete(10, 10)
        ) {
            assert!(dag.len() <= 10);
            assert!(dag.iter().all(|node| node.height() <= 10));
            assert!(dag.iter().all(|node| node.raw_parents_snapshot().len() <= 10));
        }

        #[test]
        fn test_path_compression(
            dag in arb_dag_complete(10, 100)
        ) {
            let first = dag.first().unwrap();
            let initial_height = first.height();
            let _parents = first.parents();
            let final_height = first.height();
            assert!(final_height <= initial_height);
            assert!(first.is_trivial())
        }

        #[test]
        fn test_path_compression_bfs(
            dag in arb_dag_complete(10, 100)
        ) {
            let first = dag.first().unwrap();
            let iter = bfs(first.clone());
            // The first nodemay end up compressible
            let mut is_first = true;
            for node in iter {
                if !is_first {
                assert!(node.is_leaf()|| !node.is_compressible())
                }
                is_first = false;
            }

        }

    }
}
