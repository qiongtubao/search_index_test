use tantivy::query::{Query, Weight, Scorer, Explanation, BooleanQuery, RangeQuery, BooleanWeight, BitSetDocSet, Intersection, ConstScorer, TermScorer, VecDocSet};
use tantivy::{Searcher, TantivyError, SegmentReader, DocSet, Term, InvertedIndexReader, DocId, SkipResult, BitSet};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::postings::SegmentPostings;
use std::sync::{Arc, RwLock};
use tantivy::directory::DirectoryClone;
use serde_json::de::ParserNumber::U64;
use std::borrow::BorrowMut;
use std::rc::Rc;
use std::time::SystemTime;
use tantivy::termdict::{TermDictionary, TermStreamer};
use std::collections::{Bound, BTreeMap, HashMap, BinaryHeap};
use std::fmt;

#[derive(Clone, Debug)]
pub struct CatQuery {
    query: BooleanQuery,
    field: Field,
    left: u64,
    right: u64,
    limit: usize
}
impl CatQuery {
    pub fn new(query: BooleanQuery, field: Field, left: u64, right: u64, limit: usize) -> Self {
        CatQuery {
            query,
            field,
            left,
            right,
            limit
        }
    }
}

impl Query for CatQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<Weight>, TantivyError> {
        Ok(Box::new(CatWeight {
            weight: self.query.weight(searcher, scoring_enabled)?,
            field: self.field,
            left: self.left,
            right: self.right,
            limit: self.limit
        }))
    }
}

struct CatWeight {
    weight: Box<dyn Weight>,
    field: Field,
    left: u64,
    right: u64,
    limit: usize
}
//impl DocSet for Rc<dyn DocSet> {
//    fn advance(&mut self) -> bool {
//        self.borrow_mut().advance()
//    }
//
//    fn doc(&self) -> u32 {
//        unimplemented!()
//    }
//
//    fn size_hint(&self) -> u32 {
//        unimplemented!()
//    }
//
//    fn get_name(&mut self) -> &'static str {
//        unimplemented!()
//    }
//}
#[derive(Clone)]
struct ArcVecDocSet {
    vec_doc_set:Rc<RwLock<VecDocSet>>,
}
impl DocSet for ArcVecDocSet {
    fn advance(&mut self) -> bool {
        self.vec_doc_set.write().expect("").advance()
    }

    fn doc(&self) -> u32 {
        self.vec_doc_set.read().expect("").doc()
    }

    fn size_hint(&self) -> u32 {
        self.vec_doc_set.read().expect("").size_hint()
    }

    fn get_name(&mut self) -> &'static str {
        self.vec_doc_set.write().expect("").get_name()

    }
}
//fn intersection_all(left: &mut DocSet, right: &mut Vec<DocSet>) -> bool {
//
//    return false;
//}
fn intersection(left: &mut DocSet, right: &mut DocSet) -> bool {
    if !left.advance() {
        return false;
    }
    let mut candidate = left.doc();
    if right.doc() == candidate {
        return true;
    }
    loop {
        match right.skip_next(candidate) {
            SkipResult::Reached => {
                return true;
            }
            SkipResult::OverStep => {
                candidate = right.doc();
            }
            SkipResult::End => {
                return false;
            }
        }
        match left.skip_next(candidate) {
            SkipResult::Reached => {
                return true;
            }
            SkipResult::OverStep => {
                candidate = left.doc();
            }
            SkipResult::End => {
                return false;
            }
        }
    }
}



impl CatWeight {
    fn term_range<'a>(&self, term_dict: &'a TermDictionary) -> TermStreamer<'a> {
        use std::collections::Bound::*;
        let mut term_stream_builder = term_dict.range();
        term_stream_builder = match Bound::Included(Term::from_field_u64(self.field, self.left).value_bytes().to_owned()) {
            Included(ref term_val) => term_stream_builder.ge(term_val),
            Excluded(ref term_val) => term_stream_builder.gt(term_val),
            Unbounded => term_stream_builder,
        };
        term_stream_builder = match Bound::Included(Term::from_field_u64(self.field, self.right).value_bytes().to_owned()) {
            Included(ref term_val) => term_stream_builder.le(term_val),
            Excluded(ref term_val) => term_stream_builder.lt(term_val),
            Unbounded => term_stream_builder,
        };
        term_stream_builder.into_stream()
    }
    fn scorer1(&self, reader: &SegmentReader) -> Result<Box<Scorer>, TantivyError> {
        let inverted_index = reader.inverted_index(self.field);
//        let fieldnorm_reader = reader.get_fieldnorms_reader(field);
        let mut scorer = self.weight.scorer(reader)?;
        let mut doc_vec = vec![];
        let mut v = vec![];
//        println!("{:?}", scorer.size_hint());
        scorer.for_each(&mut |doc, score| {
            v.push(doc);
        });
        let mut num = 0;
        for i in self.left..self.right {
            let term = Term::from_field_u64(self.field, i);
            if let Some(mut right) = inverted_index.read_postings(&term, IndexRecordOption::Basic) {
                let array :Vec<Box<dyn DocSet>> = vec![Box::new(VecDocSet::from(v.clone())), Box::new(right)];
                let mut intersection_scorer = Intersection::new(array);
                let start_time = SystemTime::now();
                while intersection_scorer.advance() {
                    doc_vec.push(intersection_scorer.doc());
                    num = num + 1;
                    if num >= self.limit {
                        return Ok(Box::new(ConstScorer::new(VecDocSet::from(doc_vec))));
                    }
                }
                println!("run time {:?}",SystemTime::now().duration_since(start_time).expect("??"));

            }
        }
        Ok(Box::new(ConstScorer::new(VecDocSet::from(doc_vec))))
    }
    fn scorer2(&self, reader: &SegmentReader) -> Result<Box<Scorer>, TantivyError> {
        let inverted_index = reader.inverted_index(self.field);
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);

        let term_dict = inverted_index.terms();
        let mut term_range = self.term_range(term_dict);
        while term_range.advance() {
            let term_info = term_range.value();
            let mut block_segment_postings = inverted_index
                .read_block_postings_from_terminfo(term_info, IndexRecordOption::Basic);
            while block_segment_postings.advance() {
                for &doc in block_segment_postings.docs() {
                    doc_bitset.insert(doc);
                }
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        let mut scorer = self.weight.scorer(reader)?;
        let array :Vec<Box<dyn DocSet>> = vec![Box::new(doc_bitset), Box::new(scorer)];
        Ok(Box::new(ConstScorer::new(Intersection::new(array))))
    }
    fn scorer3(&self, reader: &SegmentReader) ->  Result<Box<Scorer>, TantivyError> {
        let inverted_index = reader.inverted_index(self.field);
        let max_doc = reader.max_doc();
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        for i in self.left..self.right {
            let term = Term::from_field_u64(self.field, i);
            if let Some(mut right) = inverted_index.read_postings(&term, IndexRecordOption::Basic) {
                while right.advance() {
                    doc_bitset.insert(right.doc());
                }
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        let mut scorer = self.weight.scorer(reader)?;
        let array :Vec<Box<dyn DocSet>> = vec![Box::new(doc_bitset), Box::new(scorer)];
        Ok(Box::new(ConstScorer::new(Intersection::new(array))))
    }
    //3s
    fn scorer4(&self, reader: &SegmentReader) -> Result<Box<Scorer>, TantivyError> {
        let mut scorer = self.weight.scorer(reader)?;
        let inverted_index = reader.inverted_index(self.field);
        let max_doc = reader.max_doc();

        let mut doc_bitset = BitSet::with_max_value(max_doc);
        let mut btree_map = BTreeMap::new();
//        println!("{:?}", scorer.size_hint());
        scorer.for_each(&mut |doc, score| {
            btree_map.insert(doc, score);
        });
        for i in self.left..self.right {
            let term = Term::from_field_u64(self.field, i);
            if let Some(mut right) = inverted_index.read_postings(&term, IndexRecordOption::Basic) {
                while right.advance() {
                    let doc_id = right.doc();
                    if btree_map.contains_key(&doc_id) {
                        doc_bitset.insert(doc_id);
                    }

                }
            }
        }
        let doc_bitset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(doc_bitset)))
    }
    fn scorer5(&self, reader: &SegmentReader) -> Result<Box<Scorer>, TantivyError> {
        let mut scorer = self.weight.scorer(reader)?;
        let inverted_index = reader.inverted_index(self.field);
        let max_doc = reader.max_doc();
        let mut doc_bitmap = BitSet::with_max_value(max_doc);
        while scorer.advance() {
            doc_bitmap.insert(scorer.doc());
        }
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        for i in self.left..self.right {
            let term = Term::from_field_u64(self.field, i);
            if let Some(mut right) = inverted_index.read_postings(&term, IndexRecordOption::Basic) {
                while right.advance() {
                    if doc_bitmap.contains(right.doc()) {
                       doc_bitset.insert(right.doc())
                    }
                }
            }
        }
        Ok(Box::new(ConstScorer::new(BitSetDocSet::from(doc_bitset))))
    }
}


impl Weight for CatWeight {
    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>, TantivyError> {
        self.scorer2(reader)
    }


    fn explain(&self, reader: &SegmentReader, doc: u32) -> Result<Explanation, TantivyError> {
        let mut scorer = self.scorer(reader)?;
        if scorer.skip_next(doc) != SkipResult::Reached {
            return Err( TantivyError::InvalidArgument(format!("Document #({}) does not match", doc)));
        }
        Ok(Explanation::new("CatQuery", 1.0f32))
    }
}


