#![warn(bad_style, missing_docs,
        unused, unused_extern_crates, unused_import_braces,
        unused_qualifications, unused_results)]

type NodeId = usize;
struct Node<K,P> {
    key: K,
    refcount: isize,
    payload: Option<P>,
    relations: Vec<Option<NodeId>>,
}

pub struct DependencyGraph<K,T> {
    nodes: Vec<Option<Node<K,T>>>,
    vacancies: Vec<NodeId>,
}

impl<K,P> Node<K,P> {
    fn new(key: K, maybe_payload: Option<P>) -> Self {
        Node {
            key: key,
            payload: maybe_payload,
            refcount: 0,
            relations: Vec::new(),
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
    pub fn get_payload(&mut self, key: K) -> Option<P> where K: PartialEq, P: Clone {
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
    pub fn set_payload(&mut self, key: K, dependencies: Vec<K>, payload: P)  where K: PartialEq, P: Clone {
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
    pub fn remove_payload(&mut self, key: K )  where K: PartialEq, P: Clone  {
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

pub struct Subjectpayload {
    pub key: K,
    pub payload: MemoRefpayload,
    pub from_keys: Vec<K>,
    pub to_keys: Vec<K>,
    pub refcount: usize,
}

pub struct SubjectpayloadIter<K,P> {
    sorted: Vec<Subjectpayload>,
}
impl <K,P> Iterator for SubjectpayloadIter<K,P> {
    type Item = Subjectpayload;

    fn next(&mut self) -> Option<Subjectpayload> {
        self.sorted.pop()
    }
}
impl<K,P> SubjectpayloadIter<K,P> {
    fn new(nodes: &Vec<Option<Node<K,P>>>) -> Self {
        // TODO: make this respond to context changes while we're mid-iteration.
        // Approach A: switch Vec<Node> to Arc<Vec<Option<Node>>> and avoid slot reclamation until the iter is complete
        // Approach B: keep Vec<Node> sorted (DESC) by refcount, and reset the increment whenever the sort changes

        // FOR now, taking the low road
        // Vec<(usize, MemoRefpayload, Vec<K>)>
        let mut subject_payloads: Vec<Subjectpayload> = nodes.iter()
            .filter_map(|i| {
                if let &Some(ref Node) = i {
                    if let Some(ref payload) = Node.payload {

                        let relation_keys: Vec<K> = Node.relations
                            .iter()
                            .filter_map(|maybe_node_id| {
                                if let &Some(node_id) = maybe_node_id {
                                    if let Some(ref Node) = nodes[node_id] {
                                        Some(Node.key)
                                    } else {
                                        panic!("sanity error, subject_payload_iter")
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect();

                        return Some(Subjectpayload {
                            key: Node.key,
                            refcount: Node.refcount as usize,
                            payload: payload.clone(),
                            from_keys: vec![],
                            to_keys: relation_keys,
                        });
                    }
                }
                None
            })
            .collect();

        // Ascending sort here, because the iterator is using pop
        // TODO: be sure to reverse this later if we switch to incremental calculation
        subject_payloads.sort_by(|a, b| a.refcount.cmp(&b.refcount));

        SubjectpayloadIter { sorted: subject_payloads }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use super::DependencyGraph;

    #[test]
    fn context_manager_basic() {
        let mut manager = DependencyGraph::new();

        let payload1 = slab.new_memo_basic_noparent(Some(1),
                                     MemoBody::FullyMaterialized {
                                         v: HashMap::new(),
                                         r: RelationSlotSubjectpayload::empty(),
                                     })
            .to_payload();
        manager.set_subject_payload(1, payload1.project_all_relation_links(&slab), payload1.clone());

        let payload2 = slab.new_memo_basic_noparent(Some(2),
                                     MemoBody::FullyMaterialized {
                                         v: HashMap::new(),
                                         r: RelationSlotSubjectpayload::single(0, 1, payload1),
                                     })
            .to_payload();
        manager.set_subject_payload(2, payload2.project_all_relation_links(&slab), payload2.clone());

        let payload3 = slab.new_memo_basic_noparent(Some(3),
                                     MemoBody::FullyMaterialized {
                                         v: HashMap::new(),
                                         r: RelationSlotSubjectpayload::single(0, 2, payload2),
                                     })
            .to_payload();
        manager.set_subject_payload(3, payload3.project_all_relation_links(&slab), payload3.clone());

        let payload4 = slab.new_memo_basic_noparent(Some(4),
                                     MemoBody::FullyMaterialized {
                                         v: HashMap::new(),
                                         r: RelationSlotSubjectpayload::single(0, 3, payload3),
                                     })
            .to_payload();
        manager.set_subject_payload(4, payload4.project_all_relation_links(&slab), payload4);

        let mut iter = manager.subject_payload_iter();
        assert_eq!(1, iter.next().expect("iter result 1 should be present").key);
        assert_eq!(2, iter.next().expect("iter result 2 should be present").key);
        assert_eq!(3, iter.next().expect("iter result 3 should be present").key);
        assert_eq!(4, iter.next().expect("iter result 4 should be present").key);
        assert!(iter.next().is_none(), "iter should have ended");
    }

    #[test]
    fn context_manager_dual_indegree_zero() {
        let net = Network::create_new_system();
        let slab = Slab::new(&net);
        let mut manager = DependencyGraph::new();

        // Subject 1 is pointing to nooobody
        let payload1 = slab.new_memo_basic_noparent(Some(1), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::empty() }).to_payload();
        manager.set_subject_payload(1, payload1.project_all_relation_links(&slab), payload1.clone());

        // Subject 2 slot 0 is pointing to Subject 1
        let payload2 = slab.new_memo_basic_noparent(Some(2), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::single(0, 1, payload1.clone()) }).to_payload();
        manager.set_subject_payload(2, payload2.project_all_relation_links(&slab), payload2.clone());

        //Subject 3 slot 0 is pointing to nobody
        let payload3 = slab.new_memo_basic_noparent(Some(3), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::empty() }).to_payload();
        manager.set_subject_payload(3, payload3.project_all_relation_links(&slab), payload3.clone());

        // Subject 4 slot 0 is pointing to Subject 3
        let payload4 = slab.new_memo_basic_noparent(Some(4), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::single(0, 3, payload3.clone()) }).to_payload();
        manager.set_subject_payload(4, payload4.project_all_relation_links(&slab), payload4);


        // 2[0] -> 1
        // 4[0] -> 3
        let mut iter = manager.subject_payload_iter();
        // for subject_payload in iter {
        //     println!("{} is {}", subject_payload.key, subject_payload.refcount );
        // }
        assert_eq!(3, iter.next().expect("iter result 3 should be present").key);
        assert_eq!(1, iter.next().expect("iter result 1 should be present").key);
        assert_eq!(4, iter.next().expect("iter result 4 should be present").key);
        assert_eq!(2, iter.next().expect("iter result 2 should be present").key);
        assert!(iter.next().is_none(), "iter should have ended");
    }
        #[test]
    fn context_manager_repoint_relation() {
        let net = Network::create_new_system();
        let slab = Slab::new(&net);
        let mut manager = DependencyGraph::new();

        // Subject 1 is pointing to nooobody
        let payload1 = slab.new_memo_basic_noparent(Some(1), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::empty() }).to_payload();
        manager.set_subject_payload(1, payload1.project_all_relation_links(&slab), payload1.clone());

        // Subject 2 slot 0 is pointing to Subject 1
        let payload2 = slab.new_memo_basic_noparent(Some(2), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::single(0, 1, payload1.clone()) }).to_payload();
        manager.set_subject_payload(2, payload2.project_all_relation_links(&slab), payload2.clone());

        //Subject 3 slot 0 is pointing to nobody
        let payload3 = slab.new_memo_basic_noparent(Some(3), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::empty() }).to_payload();
        manager.set_subject_payload(3, payload3.project_all_relation_links(&slab), payload3.clone());

        // Subject 4 slot 0 is pointing to Subject 3
        let payload4 = slab.new_memo_basic_noparent(Some(4), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::single(0, 3, payload3.clone()) }).to_payload();
        manager.set_subject_payload(4, payload4.project_all_relation_links(&slab), payload4.clone());

        // Repoint Subject 2 slot 0 to subject 4
        let payload2_b = slab.new_memo_basic(Some(2), payload2, MemoBody::Relation(RelationSlotSubjectpayload::single(0,4,payload4) )).to_payload();
        manager.set_subject_payload(4, payload2_b.project_all_relation_links(&slab), payload2_b);


        // 2[0] -> 1
        // 4[0] -> 3
        // Then:
        // 2[0] -> 4
        
        let mut iter = manager.subject_payload_iter();
        // for subject_payload in iter {
        //     println!("{} is {}", subject_payload.key, subject_payload.refcount );
        // }
        assert_eq!(1, iter.next().expect("iter result 1 should be present").key);
        assert_eq!(4, iter.next().expect("iter result 4 should be present").key);
        assert_eq!(3, iter.next().expect("iter result 3 should be present").key);
        assert_eq!(2, iter.next().expect("iter result 2 should be present").key);
        assert!(iter.next().is_none(), "iter should have ended");
    }
    #[test]
    fn context_manager_remove() {
        let net = Network::create_new_system();
        let slab = Slab::new(&net);
        let mut manager = DependencyGraph::new();

        // Subject 1 is pointing to nooobody
        let payload1 = slab.new_memo_basic_noparent(Some(1), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::empty() }).to_payload();
        manager.set_subject_payload(1, payload1.project_all_relation_links(&slab), payload1.clone());

        // Subject 2 slot 0 is pointing to Subject 1
        let payload2 = slab.new_memo_basic_noparent(Some(2), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::single(0, 1, payload1.clone()) }).to_payload();
        manager.set_subject_payload(2, payload2.project_all_relation_links(&slab), payload2.clone());

        //Subject 3 slot 0 is pointing to Subject 2
        let payload3 = slab.new_memo_basic_noparent(Some(3), MemoBody::FullyMaterialized { v: HashMap::new(), r: RelationSlotSubjectpayload::single(0, 2, payload2.clone()) }).to_payload();
        manager.set_subject_payload(3, payload3.project_all_relation_links(&slab), payload3.clone());


        // 2[0] -> 1
        // 3[0] -> 2
        // Subject 1 should have refcount = 2

        manager.remove_subject_payload(2);
        
        let mut iter = manager.subject_payload_iter();
        // for subject_payload in iter {
        //     println!("{} is {}", subject_payload.key, subject_payload.refcount );
        // }
        assert_eq!(3, iter.next().expect("iter result 3 should be present").key);
        assert_eq!(1, iter.next().expect("iter result 1 should be present").key);
        assert!(iter.next().is_none(), "iter should have ended");
    }
}
