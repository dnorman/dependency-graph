//! A Depdency Graph capable of topological iteration over cyclic dependencies and phantom dependencies.
//!
//! `DependencyGrap<K,P>` allows you to:
//!
//! * Insert a Payload enumerated by Key
//! * Reference other dependent keys (which may or may not be present)
//! * Insert dependent payloads after their dependees have already referenced them
//! * Produce non-cyclic topological iterators over the potentially cyclic graph
//! 
//! TODOs;
//! * Lock-free concurrency
//! * Iterators reflect midstream graph changes for items topologically ascendent/descendent of present iteration

type NodeId = usize;

struct Node<K,P> {
    key: K,
    refcount: usize,
    state: NodeState<P>
}
enum NodeState<P>{
    Phantom(),
    Resident {
        payload:  P,
        relations: Vec<Option<NodeId>>,
    }
}


pub struct DependencyGraph<K,P> {
    nodes: Vec<Option<Node<K,P>>>,
    vacancies: Vec<NodeId>,
}

impl<K,P> Node<K,P> {
    fn new(key: K, payload: P) -> Self {
        Node {
            key: key,
            refcount: 0,
            state: NodeState::Resident {
                payload: payload,
                relations: Vec::new()
            }
        }
    }
}

impl<K,P> DependencyGraph<K,P> {
    pub fn new() -> DependencyGraph<K,P> {
        DependencyGraph {
            nodes: Vec::with_capacity(30),
            vacancies: Vec::with_capacity(30),
        }
    }

    /// Returns the number of elements in the `DependencyGraph`.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns true if the `DependencyGraph` contains no entries.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn keys(&self) -> Vec<K> {
        self.nodes
            .iter()
            .filter_map(|i| {
                if let &Some(ref Node) = i {
                    Some(Node.key)
                } else {
                    None
                }
            })
            .collect()
    }
    pub fn get(&mut self, key: K) -> Option<P> where K: PartialEq, P: Clone {
        if let Some(&Some(ref mut Node)) =
             self.nodes.iter().find(|i| {
                if let &&Some(ref it) = i {
                    it.key == key
                } else {
                    false
                }
            }) {
            Node.payload.clone()
        } else {
            None
        }
    }

    /// Update the payload for a given subject. The previous payload is summarily overwritten.
    /// Any mrh.apply to the previous payload must be done externally, if desired
    /// relation_links must similarly be pre-calculated
    pub fn insert(&mut self, key: K, payload: P, dependencies: Vec<K>)  where K: PartialEq, P: Clone {
        let node_id = {
            self.assert_node(key)
        };
        if let Some(ref mut Node) = self.nodes[node_id] {
            Node.payload = Some(payload);
        }

        for link in dependencies {
            self.set_relation(node_id, link);
        }
    }
    pub fn remove(&mut self, key: K ) where K: PartialEq, P: Clone  {
        if let Some(node_id) = self.nodes.iter().position(|i| {
            if let &Some(ref it) = i {
                it.key == key
            } else {
                false
            }
        }) {
            let mut full_remove = false;
            let mut relations = Vec::new();
            let decrement;
            let nodes_len = self.nodes.len();

            {
                if let Some(ref mut Node) = self.nodes[node_id] {
                    decrement = 0 - (Node.refcount + 1);
                    for relation in Node.relations.iter() {
                        if let Some(rel_node_id) = *relation {
                            relations.push(rel_node_id);
                        }
                    }
                
                    Node.relations.clear();

                    if Node.refcount == 0 {
                        // If nobody points to me, we can fully bail out
                        full_remove = true;
                    }else{
                        // otherwise just remove the payload that we intend to remove
                        Node.payload = None;
                    }
                }else{
                    panic!("sanity error");
                }

                if full_remove {
                    self.nodes[node_id] = None;
                    self.vacancies.push(node_id);
                }
            }

            // no payload means we're not pointing to these anymore, at least not within the context manager
            for rel_node_id in relations {
                let mut removed = vec![false; nodes_len];
                self.increment(rel_node_id, decrement, &mut removed);
            }

        }

    }

    /// Creates or returns a DependencyGraph Node for a given key
    fn assert_node(&mut self, key: K) -> NodeId where K: PartialEq, P: Clone {
        if let Some(node_id) = self.nodes.iter().position(|i| {
            if let &Some(ref it) = i {
                it.key == key
            } else {
                false
            }
        }) {
            node_id
        } else {
            let Node = Node::new(key, None);

            if let Some(node_id) = self.vacancies.pop() {
                self.nodes[node_id] = Some(Node);
                node_id
            } else {
                self.nodes.push(Some(Node));
                self.nodes.len() - 1
            }

        }
    }

    fn set_relation(&mut self, node_id: NodeId, dependency: RelationLink) {

        // let Node = &self.nodes[node_id];
        // retrieve existing relation by SlotId as the vec offset
        // Some(&Some()) due to empty vec slot vs None relation (logically equivalent)
        let mut remove = None;
        {
            let Node = {
                if let Some(ref Node) = self.nodes[node_id] {
                    Node
                } else {
                    panic!("sanity error. set relation on Node that does not exist")
                }
            };

            if let Some(&Some(rel_node_id)) = Node.relations.get(link.slot_id as usize) {
                // relation exists

                let decrement;
                {
                    if let &Some(ref rel_Node) = &self.nodes[rel_node_id] {

                        // no change. bail out. do not increment or decrement
                        if Some(rel_Node.key) == link.key {
                            return;
                        }

                        decrement = 0 - (1 + Node.refcount);
                    } else {
                        panic!("sanity error. relation node_id located, but not found in nodes")
                    }
                }

                remove = Some((rel_node_id, decrement));
            };
        }


        // ruh roh, we're different. Have to back out the old relation
        // (a little friendly sparring with the borrow checker :-x )
        if let Some((rel_node_id, decrement)) = remove {
            let mut removed = vec![false; self.nodes.len()];
            {
                self.increment(rel_node_id, decrement, &mut removed)
            };
            // Node.relations[link.slot_id] MUST be set below
        }

        if let Some(key) = link.key {
            let new_rel_node_id = {
                self.assert_node(key)
            };

            let increment;
            {
                if let &mut Some(ref mut Node) = &mut self.nodes[node_id] {
                    while Node.relations.len() <= link.slot_id as usize { 
                        Node.relations.push(None);
                    }

                    Node.relations[link.slot_id as usize] = Some(new_rel_node_id);
                    increment = 1 + Node.refcount;
                } else {
                    panic!("sanity error. relation just set")
                }
            };

            let mut added = vec![false; self.nodes.len()];
            self.increment(new_rel_node_id, increment, &mut added);
        } else {
            // sometimes this will be unnecessary, but it's essential to overwrite a Some() if it's there
            if let &mut Some(ref mut Node) = &mut self.nodes[node_id] {
                while Node.relations.len() <= link.slot_id as usize { 
                    Node.relations.push(None);
                }

                Node.relations[link.slot_id as usize] = None;

            } else {
                panic!("sanity error. relation Node not found in nodes")
            }
        }
    }
    fn increment(&mut self, node_id: NodeId, increment: isize, seen: &mut Vec<bool>) {
        // Avoid traversing cycles
        if Some(&true) == seen.get(node_id) {
            return; // dejavu! Bail out
        }
        seen[node_id] = true;

        let relations: Vec<NodeId>;

        {
            if let &mut Some(ref mut Node) = &mut self.nodes[node_id] {
                Node.refcount += increment;
                assert!(Node.refcount >= 0,
                        "sanity error. refcount below zero");

                relations = Node.relations.iter().filter_map(|r| *r).collect();
            } else {
                panic!("sanity error. increment for node_id");
            }
        };

        for rel_node_id in relations {
            self.increment(rel_node_id, increment, seen);
        }

    }
    pub fn subject_payload_iter(&self) -> SubjectpayloadIter {
        SubjectpayloadIter::new(&self.nodes)
    }
}

// pub struct Subjectpayload {
//     pub key: K,
//     pub payload: MemoRefpayload,
//     pub from_keys: Vec<K>,
//     pub to_keys: Vec<K>,
//     pub refcount: usize,
// }

// pub struct SubjectpayloadIter<K,P> {
//     sorted: Vec<Subjectpayload>,
// }
// impl <K,P> Iterator for SubjectpayloadIter<K,P> {
//     type Item = Subjectpayload;

//     fn next(&mut self) -> Option<Subjectpayload> {
//         self.sorted.pop()
//     }
// }

// impl<K,P> SubjectpayloadIter<K,P> {
//     fn new(nodes: &Vec<Option<Node<K,P>>>) -> Self {
//         // TODO: make this respond to context changes while we're mid-iteration.
//         // Approach A: switch Vec<Node> to Arc<Vec<Option<Node>>> and avoid slot reclamation until the iter is complete
//         // Approach B: keep Vec<Node> sorted (DESC) by refcount, and reset the increment whenever the sort changes

//         // FOR now, taking the low road
//         // Vec<(usize, MemoRefpayload, Vec<K>)>
//         let mut subject_payloads: Vec<Subjectpayload> = nodes.iter()
//             .filter_map(|i| {
//                 if let &Some(ref Node) = i {
//                     if let Some(ref payload) = Node.payload {

//                         let relation_keys: Vec<K> = Node.relations
//                             .iter()
//                             .filter_map(|maybe_node_id| {
//                                 if let &Some(node_id) = maybe_node_id {
//                                     if let Some(ref Node) = nodes[node_id] {
//                                         Some(Node.key)
//                                     } else {
//                                         panic!("sanity error, subject_payload_iter")
//                                     }
//                                 } else {
//                                     None
//                                 }
//                             })
//                             .collect();

//                         return Some(Subjectpayload {
//                             key: Node.key,
//                             refcount: Node.refcount as usize,
//                             payload: payload.clone(),
//                             from_keys: vec![],
//                             to_keys: relation_keys,
//                         });
//                     }
//                 }
//                 None
//             })
//             .collect();

//         // Ascending sort here, because the iterator is using pop
//         // TODO: be sure to reverse this later if we switch to incremental calculation
//         subject_payloads.sort_by(|a, b| a.refcount.cmp(&b.refcount));

//         SubjectpayloadIter { sorted: subject_payloads }
//     }
// }

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use super::DependencyGraph;

    #[test]
    fn basic() {
        let mut graph = DependencyGraph::new();
        graph.insert("A", "Alpha",   vec![]);
        graph.insert("B", "Bravo",   vec!["A"]);
        graph.insert("C", "Charlie", vec!["B"]);
        graph.insert("D", "Delta",   vec!["C"]);

        let mut iter = graph.iter();
        assert_eq!("A", iter.next().expect("should be present").key);
        assert_eq!("B", iter.next().expect("should be present").key);
        assert_eq!("C", iter.next().expect("should be present").key);
        assert_eq!("D", iter.next().expect("should be present").key);
        assert!(iter.next().is_none(), "should have ended");
    }

    #[test]
    fn belated() {
        let mut graph = DependencyGraph::new();
        graph.insert("A", "Alpha",   vec!["D"]);
        graph.insert("B", "Bravo",   vec!["A"]);
        graph.insert("C", "Charlie", vec!["B"]);
        graph.insert("D", "Delta",   vec![]);

        let mut iter = graph.iter();
        assert_eq!("D", iter.next().expect("should be present").key);
        assert_eq!("A", iter.next().expect("should be present").key);
        assert_eq!("B", iter.next().expect("should be present").key);
        assert_eq!("C", iter.next().expect("should be present").key);
        assert!(iter.next().is_none(), "should have ended");
    }

    #[test]
    fn dual_indegree_zero() {
        let mut graph = DependencyGraph::new();
        graph.insert("A", "Alpha",   vec![]);
        graph.insert("B", "Bravo",   vec!["A"]);
        graph.insert("C", "Charlie", vec![]);
        graph.insert("D", "Delta",   vec!["C"]);

        let mut iter = graph.iter();
        assert_eq!("C", iter.next().expect("should be present").key);
        assert_eq!("A", iter.next().expect("should be present").key);
        assert_eq!("D", iter.next().expect("should be present").key);
        assert_eq!("B", iter.next().expect("should be present").key);
        // Arguably ACBD, CDAB, and ABCD are topologically equivalent

        assert!(iter.next().is_none(), "should have ended");
    }

    #[test]
    fn repoint_relation() {

        let mut graph = DependencyGraph::new();
        // B -> A
        // D -> C
        // Then:
        // B -> D
        // Thus:
        // B -> D -> C
        // A
        graph.insert("A", "Alpha",   vec![]);
        graph.insert("B", "Bravo",   vec!["A"]);
        graph.insert("C", "Charlie", vec![]);
        graph.insert("D", "Delta",   vec!["C"]);
        graph.insert("B", "Bravo",   vec!["D"]);

        let mut iter = graph.iter();
        assert_eq!("C", iter.next().expect("should be present").key);
        assert_eq!("D", iter.next().expect("should be present").key);
        assert_eq!("B", iter.next().expect("should be present").key);
        assert_eq!("A", iter.next().expect("should be present").key);
        assert!(iter.next().is_none(), "iter should have ended");
    }
    #[test]
    fn remove() {

        let mut graph = DependencyGraph::new();
        graph.insert("A", "Alpha",   vec![]);
        graph.insert("B", "Bravo",   vec!["A"]);
        graph.insert("C", "Charlie", vec!["B"]);
        graph.insert("D", "Delta",   vec!["C"]);

        graph.remove("B");

        // Was: D -> C -> B -> A
        // Now: D -> C, A

        let mut iter = graph.iter();
        assert_eq!("C", iter.next().expect("should be present").key);
        assert_eq!("D", iter.next().expect("should be present").key);
        assert_eq!("A", iter.next().expect("should be present").key);
        assert!(iter.next().is_none(), "should have ended");
    }
}
