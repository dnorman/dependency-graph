//! A Directed Graph capable of topological iteration, with cyclic and phantom dependencies.
//!
//! `DependencyGraph<K,V,E>` allows you to:
//!
//! * Insert a Value for a given Vertex key
//! * Record directed edges, each with its own measure(value) to another Vertex, which may or may not be present in the graph
//! * Insert payloads after their dependees have already referenced them
//! * Produce non-cyclic topological iterators over the potentially cyclic graph
//! 
//! TODOs;
//! * Discontinue use of Arc in favor of custom Arc implementation, to avoid double counting
//! * Lock-free concurrency
//! * Iterators reflect midstream graph changes for items topologically ascendent/descendent of present iteration

use std::sync::{Mutex,RwLock,Arc};

struct Vertex<K,V,E> {
    key: Mutex<Option<K>>,
    refcount: Mutex<usize>,
    state: Mutex<VertexState<K,V,E>>
}

struct Edge<K,V,E> {
    measure: Option<E>,
    dest_vertex: Arc<Vertex<K,V,E>>
}
enum VertexState<K,V,E>{
    Phantom,
    Resident {
        value: V,
        edges: Vec<Edge<K,V,E>>,
    }
}

#[derive(Clone)]
pub struct DependencyGraph<K,V,M> {
    vertex_vec: Arc<Mutex<Vec<Arc<Vertex<K,V,M>>>>>,
}

impl<K,V,E> Vertex<K,V,E> {
    fn new(key: K, value: V) -> Self {
        Vertex {
            key: Mutex::new(Some(key)),
            refcount: Mutex::new(0),
            state: Mutex::new(VertexState::Resident {
                value: value,
                edges: Vec::new()
            })
        }
    }

    /// Find the dest vertex, or create using a given VertexState
    /// Either way increment its refcount
    /// For the time being, this refcount is redundant with that of the Arc. This will be remedied later.
    fn assert (key: K, vertex_vec: &mut Vec<Arc<Vertex<K,V,E>>>, default_state: VertexState<K,V,E>) -> Arc<Self>
        where K: PartialEq+Ord {
        //match vertex_vec.binary_search_by(|n| n.key.lock().unwrap().cmp(&Some(key)) ) {
        match vertex_vec.iter().find(|n| *n.key.lock().unwrap() == Some(key) ) {
                //Ok(i) => {
                Some(vertex) => {
                    //let vertex = vertex_vec[i].clone();
                    vertex.increment();
                    vertex.clone()
                }
                //Err(i) => {
                None => {
                    // Seaerch for an empty slot
                    match vertex_vec.iter().find(|n| *n.key.lock().unwrap() == None) {
                        Some(vertex) => {
                            // Found one
                            *vertex.key.lock().unwrap() = Some(key);
                            *vertex.refcount.lock().unwrap() = 1;
                            *vertex.state.lock().unwrap() = default_state;
                            vertex.clone()
                        },
                        None => {
                            // No empty slots, just insert
                            let vertex = Arc::new(Vertex{
                                key: Mutex::new(Some(key)),
                                refcount: Mutex::new(1),
                                state: Mutex::new(default_state)
                            });;
                            vertex_vec.push(vertex.clone());
                            //vertex_vec.insert(i, vertex.clone());

                            vertex
                        }
                    }
                }
            }
    }
    fn increment(&self) {
        *self.refcount.lock().unwrap() += 1;
    }
    fn decrement (&self) {
        let refcount = self.refcount.lock().unwrap();
        *refcount -= 1;
        if *refcount == 0 {
            if let VertexState::Phantom = *self.state.lock().unwrap() {
                *self.key.lock().unwrap() = None;
            }
        }
    }
}

impl<K,V,E> VertexState<K,V,E> {
    //fn new (all_vertexes: MutexGuard<Vec>, value: E){
    //} 
}

impl <K,V,E> Edge<K,V,E>{
    fn new (dest_key: K, measure: Option<E>, vertex_vec: &mut Vec<Arc<Vertex<K,V,E>>>) -> Self 
        where K: Ord {
            Edge{ 
                measure,
                dest_vertex: Vertex::assert( dest_key, vertex_vec, VertexState::Phantom )
            }
    }
}
impl <K,V,E> Drop for Edge<K,V,E> {
    fn drop (&mut self) {
        // Droping this edge, decrement the dest_vertex refcount
        self.dest_vertex.decrement()
    }
}

impl<K,V,E> DependencyGraph<K,V,E> {
    pub fn new() -> DependencyGraph<K,V,E> {
        DependencyGraph {
            vertex_vec: Arc::new(Mutex::new(Vec::with_capacity(30))),
        }
    }

    /// Insert a value and Vec of dependencies for a given key. If the Graph already had this key, the value is updated.
    /// Dependencies which are not already inserted will be created as phantom Vertexs.
    pub fn insert(&mut self, key: K, value: V, edge_tuples: Vec<(K,Option<E>)>)
        where K: PartialEq+Ord, V: Clone {
        let vertex_vec = self.vertex_vec.lock().unwrap();

        let edges = edge_tuples.drain(..).map(|(k,m)| Edge::new(k, m, &mut *vertex_vec) ).collect();
        let vertex = Vertex::assert( key, &mut *vertex_vec, VertexState::Phantom );
        *vertex.state.lock().unwrap() = VertexState::Resident{
            value,
            edges: edges
        };
    }
    pub fn remove(&mut self, key: K ) where K: PartialEq, V: Clone  {
        unimplemented!()
    }

    pub fn iter(&self) -> TopoIter<K,V,E> {
        TopoIter::new(self.vertex_vec.clone())
    }
    // /// Returns true if the `DependencyGraph` contains no entries.
    // #[allow(dead_code)]
    // pub fn is_empty(&self) -> bool {
    //     unimplemented!()
    // }

    // pub fn keys(&self) -> Vec<K> {
    //     self.Vertexs
    //         .iter()
    //         .filter_map(|i| {
    //             if let &Some(ref Vertex) = i {
    //                 Some(Vertex.key)
    //             } else {
    //                 None
    //             }
    //         })
    //         .collect()
    // }
    // pub fn get(&mut self, key: K) -> Option<V> where K: PartialEq, P: Clone {
    //     if let Some(&Some(ref mut Vertex)) =
    //          self.Vertexs.iter().find(|i| {
    //             if let &&Some(ref it) = i {
    //                 it.key == key
    //             } else {
    //                 false
    //             }
    //         }) {
    //         Vertex.clone()
    //     } else {
    //         None
    //     }
    // }
    // pub fn remove(&mut self, key: K ) where K: PartialEq, P: Clone  {
    //     if let Some(Vertex_id) = self.Vertexs.iter().position(|i| {
    //         if let &Some(ref it) = i {
    //             it.key == key
    //         } else {
    //             false
    //         }
    //     }) {
    //         let mut full_remove = false;
    //         let mut relations = Vec::new();
    //         let decrement;
    //         let Vertexs_len = self.Vertexs.len();

    //         {
    //             if let Some(ref mut Vertex) = self.Vertexs[Vertex_id] {
    //                 decrement = 0 - (Vertex.refcount + 1);
    //                 for relation in Vertex.relations.iter() {
    //                     if let Some(rel_Vertex_id) = *relation {
    //                         relations.push(rel_Vertex_id);
    //                     }
    //                 }
                
    //                 Vertex.relations.clear();

    //                 if Vertex.refcount == 0 {
    //                     // If nobody points to me, we can fully bail out
    //                     full_remove = true;
    //                 }else{
    //                     // otherwise just remove the payload that we intend to remove
    //                     Vertex.payload = None;
    //                 }
    //             }else{
    //                 panic!("sanity error");
    //             }

    //             if full_remove {
    //                 self.Vertexs[Vertex_id] = None;
    //                 self.vacancies.push(Vertex_id);
    //             }
    //         }

    //         // no payload means we're not pointing to these anymore, at least not within the context manager
    //         for rel_Vertex_id in relations {
    //             let mut removed = vec![false; Vertexs_len];
    //             self.increment(rel_Vertex_id, decrement, &mut removed);
    //         }

    //     }

    // }

    // /// Creates or returns a DependencyGraph Vertex for a given key
    // fn assert_Vertex(&mut self, key: K) -> VertexId where K: PartialEq, P: Clone {
    //     if let Some(Vertex_id) = self.Vertexs.iter().position(|i| {
    //         if let &Some(ref it) = i {
    //             it.key == key
    //         } else {
    //             false
    //         }
    //     }) {
    //         Vertex_id
    //     } else {
    //         let Vertex = Vertex::new(key, None);

    //         if let Some(Vertex_id) = self.vacancies.pop() {
    //             self.Vertexs[Vertex_id] = Some(Vertex);
    //             Vertex_id
    //         } else {
    //             self.Vertexs.push(Some(Vertex));
    //             self.Vertexs.len() - 1
    //         }

    //     }
    // }

    // fn set_relation(&mut self, Vertex_id: VertexId, dependency: RelationLink) {

    //     // let Vertex = &self.Vertexs[Vertex_id];
    //     // retrieve existing relation by SlotId as the vec offset
    //     // Some(&Some()) due to empty vec slot vs None relation (logically equivalent)
    //     let mut remove = None;
    //     {
    //         let Vertex = {
    //             if let Some(ref Vertex) = self.Vertexs[Vertex_id] {
    //                 Vertex
    //             } else {
    //                 panic!("sanity error. set relation on Vertex that does not exist")
    //             }
    //         };

    //         if let Some(&Some(rel_Vertex_id)) = Vertex.relations.get(link.slot_id as usize) {
    //             // relation exists

    //             let decrement;
    //             {
    //                 if let &Some(ref rel_Vertex) = &self.Vertexs[rel_Vertex_id] {

    //                     // no change. bail out. do not increment or decrement
    //                     if Some(rel_Vertex.key) == link.key {
    //                         return;
    //                     }

    //                     decrement = 0 - (1 + Vertex.refcount);
    //                 } else {
    //                     panic!("sanity error. relation Vertex_id located, but not found in Vertexs")
    //                 }
    //             }

    //             remove = Some((rel_Vertex_id, decrement));
    //         };
    //     }


    //     // ruh roh, we're different. Have to back out the old relation
    //     // (a little friendly sparring with the borrow checker :-x )
    //     if let Some((rel_Vertex_id, decrement)) = remove {
    //         let mut removed = vec![false; self.Vertexs.len()];
    //         {
    //             self.increment(rel_Vertex_id, decrement, &mut removed)
    //         };
    //         // Vertex.relations[link.slot_id] MUST be set below
    //     }

    //     if let Some(key) = link.key {
    //         let new_rel_Vertex_id = {
    //             self.assert_Vertex(key)
    //         };

    //         let increment;
    //         {
    //             if let &mut Some(ref mut Vertex) = &mut self.Vertexs[Vertex_id] {
    //                 while Vertex.relations.len() <= link.slot_id as usize { 
    //                     Vertex.relations.push(None);
    //                 }

    //                 Vertex.relations[link.slot_id as usize] = Some(new_rel_Vertex_id);
    //                 increment = 1 + Vertex.refcount;
    //             } else {
    //                 panic!("sanity error. relation just set")
    //             }
    //         };

    //         let mut added = vec![false; self.Vertexs.len()];
    //         self.increment(new_rel_Vertex_id, increment, &mut added);
    //     } else {
    //         // sometimes this will be unnecessary, but it's essential to overwrite a Some() if it's there
    //         if let &mut Some(ref mut Vertex) = &mut self.Vertexs[Vertex_id] {
    //             while Vertex.relations.len() <= link.slot_id as usize { 
    //                 Vertex.relations.push(None);
    //             }

    //             Vertex.relations[link.slot_id as usize] = None;

    //         } else {
    //             panic!("sanity error. relation Vertex not found in Vertexs")
    //         }
    //     }
    // }
    // fn increment(&mut self, Vertex_id: VertexId, increment: isize, seen: &mut Vec<bool>) {
    //     // Avoid traversing cycles
    //     if Some(&true) == seen.get(Vertex_id) {
    //         return; // dejavu! Bail out
    //     }
    //     seen[Vertex_id] = true;

    //     let relations: Vec<VertexId>;

    //     {
    //         if let &mut Some(ref mut Vertex) = &mut self.Vertexs[Vertex_id] {
    //             Vertex.refcount += increment;
    //             assert!(Vertex.refcount >= 0,
    //                     "sanity error. refcount below zero");

    //             relations = Vertex.relations.iter().filter_map(|r| *r).collect();
    //         } else {
    //             panic!("sanity error. increment for Vertex_id");
    //         }
    //     };

    //     for rel_Vertex_id in relations {
    //         self.increment(rel_Vertex_id, increment, seen);
    //     }

    // }
}

pub struct TopoIter<K,V,E> {
    visited: Vec<K>,
    vertex_vec: Arc<Mutex<Vec<Arc<Vertex<K,V,E>>>>>
}

impl <K,V,E> Iterator for TopoIter<K,V,E> {
    type Item = Vertex<K,V,E>;

    fn next(&mut self) -> Option<Vertex> {
        self.sorted.pop()
    }
}

impl<K,V,E> TopoIter<K,V,E> {
     fn new(vertex_vec: Arc<Mutex<Vec<Arc<Vertex<K,V,E>>>>>) -> Self {
         TopoIter{
             visited: Vec::new(),
             vertex_vec: vertex_vec
         }
     }
}
//         // TODO: make this respond to context changes while we're mid-iteration.
//         // Approach A: switch Vec<Vertex> to Arc<Vec<Option<Vertex>>> and avoid slot reclamation until the iter is complete
//         // Approach B: keep Vec<Vertex> sorted (DESC) by refcount, and reset the increment whenever the sort changes

//         // FOR now, taking the low road
//         // Vec<(usize, MemoRefpayload, Vec<K>)>
//         let mut subject_payloads: Vec<Subjectpayload> = Vertexs.iter()
//             .filter_map(|i| {
//                 if let &Some(ref Vertex) = i {
//                     if let Some(ref payload) = Vertex.payload {

//                         let relation_keys: Vec<K> = Vertex.relations
//                             .iter()
//                             .filter_map(|maybe_Vertex_id| {
//                                 if let &Some(Vertex_id) = maybe_Vertex_id {
//                                     if let Some(ref Vertex) = Vertexs[Vertex_id] {
//                                         Some(Vertex.key)
//                                     } else {
//                                         panic!("sanity error, subject_payload_iter")
//                                     }
//                                 } else {
//                                     None
//                                 }
//                             })
//                             .collect();

//                         return Some(Subjectpayload {
//                             key: Vertex.key,
//                             refcount: Vertex.refcount as usize,
//                             value: V.clone(),
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
        graph.insert("B", "Bravo",   vec![("A",None)];
        graph.insert("C", "Charlie", vec![("B",None)];
        graph.insert("D", "Delta",   vec![("C",None)];

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
        graph.insert("A", "Alpha",   vec![("D",None)]);
        graph.insert("B", "Bravo",   vec![("A",None)]);
        graph.insert("C", "Charlie", vec![("B",None)]);
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
        graph.insert("B", "Bravo",   vec![("A",None)]);
        graph.insert("C", "Charlie", vec![]);
        graph.insert("D", "Delta",   vec![("C",None)]);

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
        graph.insert("B", "Bravo",   vec![("A",None)]);
        graph.insert("C", "Charlie", vec![]);
        graph.insert("D", "Delta",   vec![("C",None)]);
        graph.insert("B", "Bravo",   vec![("D",None)]);

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
        graph.insert("B", "Bravo",   vec![("A",None)]);
        graph.insert("C", "Charlie", vec![("B",None)]);
        graph.insert("D", "Delta",   vec![("C",None)]);

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
