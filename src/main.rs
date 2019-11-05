use std::fs::File;
use tantivy::schema::Schema;
use tantivy::{Index, Directory, TantivyError, Searcher};
use crate::only_read_directory::OnlyReadDirectory;
use tantivy::directory::MmapDirectory;
use tantivy::query::{TermQuery, Query};
use tantivy::collector::Count;
use tantivy::schema::*;
use tantivy::query::*;
use tantivy::*;
use tantivy::collector::*;
use crate::query::CatQuery;
use std::collections::BinaryHeap;
use tantivy::fastfield::FastFieldReader;
use std::cmp::Ordering;

mod only_read_directory;

mod query_builder;
mod query_parser;
mod query;

fn schema_from_file(schema_path: String) {
    let file_path:&str = &schema_path;
   let schema = std::fs::read_to_string(schema_path.clone()).expect(&format!("read {} file error  from file", file_path));
    serde_json::from_str(&schema).expect(&format!("from {} parse schema expect", file_path))
}
fn query_count(searcher: &Searcher, query: &dyn Query) -> usize {
    searcher.search(query, &Count).expect("search")
}
struct CatCollector {
    limit: usize,
    field: Field,
    left: u64,
    right: u64,
}
impl Collector for CatCollector {
    type Fruit = (Vec<(u64,DocAddress)>, usize);
    type Child = SegmentCatCollector;

    fn for_segment(&self, segment_local_id: u32, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
        Ok(SegmentCatCollector {
            limit: self.limit,
            heap: BinaryHeap::new(),
            left: self.left,
            right: self.right,
            reader: segment.fast_fields().u64(self.field).expect("fast field"),
            segment_id: segment_local_id,
            num: 0,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        if self.limit == 0 {
            return Ok((Vec::new(), 0));
        }
        let mut num = 0;
        let mut top_collector = BinaryHeap::new();
        for child_fruit in children {
            num += child_fruit.1;
            for (feature, doc) in child_fruit.0 {
                if top_collector.len() < self.limit {
                    top_collector.push(ComparableDoc { feature, doc });
                } else if let Some(mut head) = top_collector.peek_mut() {
                    if head.feature < feature {
                        *head = ComparableDoc { feature, doc };
                    }
                }
            }
        }
        Ok((top_collector
            .into_sorted_vec()
            .into_iter()
            .map(|cdoc| (cdoc.feature, cdoc.doc))
            .collect(), num))
    }
}
struct ComparableDoc<T, D> {
    feature: T,
    doc: D
}
impl<T: PartialOrd, D> PartialOrd for ComparableDoc<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: PartialOrd, D> PartialEq for ComparableDoc<T, D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: PartialOrd, D> Eq for ComparableDoc<T, D> {}

impl<T: PartialOrd, D> Ord for ComparableDoc<T, D> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .feature
            .partial_cmp(&self.feature)
            .unwrap_or_else(|| Ordering::Equal)
    }
}
struct SegmentCatCollector {
    limit: usize,
    heap: BinaryHeap<ComparableDoc<u64, u32>>,
    segment_id: u32,
    reader: FastFieldReader<u64>,
    right: u64,
    left: u64,
    num : usize,
}
impl SegmentCollector for SegmentCatCollector {
    type Fruit = (Vec<(u64, DocAddress)>, usize);

    fn collect(&mut self, doc: u32, score: f32) {
        let feature = self.reader.get(doc);
        if self.left > feature || self.right < feature {
            return;
        }
        self.num += 1;
//        println!("feature {:?} ,{:?}, {:?}", feature, self.left, self.right);
        if self.heap.len() >= self.limit {
            if let Some(limit_feature) = self.heap.peek().map(|head| head.feature.clone()) {
                if limit_feature < feature {
                    if let Some(mut head) = self.heap.peek_mut() {
                        head.feature = feature;
                        head.doc = doc;
                    }
                }
            }
        } else {
            self.heap.push(ComparableDoc{feature, doc})
        }
    }

    fn harvest(self) -> Self::Fruit {
        let segment_id = self.segment_id;
        let len = self.heap.len();
        (self.heap.into_sorted_vec().into_iter().map(|comparable_doc| {
            (
                comparable_doc.feature,
                DocAddress(segment_id, comparable_doc.doc),
            )
        }).collect(), self.num)
    }
}
fn query_all(searcher: &Searcher, query: &dyn Query, schema: &Schema, sort_field: &str ) -> (Vec<(u64, String)>,usize) {
    let mut collectors = MultiCollector::new();
//    let top_collector = TopDocs::with_limit(400).order_by_u64_field(schema.get_field(sort_field).expect("????"));
    let top_collector = CatCollector {
        limit: 400,
        field: schema.get_field(sort_field).expect("????"),
        left: 78356886,
        right: 78366880
    };
    let topdocs_handler = collectors.add_collector(top_collector);
//    let count_handler = collectors.add_collector(Count);
    println!("query: {:?}", query);
    let mut multifruits = searcher.search(query, &mut collectors).expect("search");
    let top_docs = topdocs_handler.extract(&mut multifruits);
//    let count = count_handler.extract(&mut multifruits);
    let mut v = vec![];
    //通过文档地址查询文档
    for (feature, doc_address) in top_docs.0 {
        if let Ok(doc) = searcher.doc(doc_address) {
            v.push((feature, schema.to_json(&doc).to_string()));
        }
    }
    (v, top_docs.1)
}
fn read_dir(dir_name : &String) {
    let path = std::path::PathBuf::from(dir_name);
    let dir = OnlyReadDirectory::new(path);
//    let dir = MmapDirectory::open(path).expect("open error");
    let index = Index::open(dir).expect("open dir error");
    let schema = index.schema();
    let reader = index.reader().expect("reader");
    let query =
        std::fs::read_to_string("./query1.json").expect("error parsing config from file");
    let query = query_parser::parse(query, schema.clone(), 10000);
//    let query = r#"{
//	"query": {
//		"bool": {
//			"filter": [{
//				"term": {
//					"status": {
//						"value": "0",
//						"boost": 1
//					}
//				}
//			}, {
//				"term": {
//					"timeUnit": {
//						"value": 20000,
//						"boost": 1
//					}
//				}
//			}],
//			"adjust_pure_negative": true,
//			"boost": 1
//		}
//	}
//}"#;
//    let query = query_parser::parse(query.to_string(), schema.clone());
//    let query = CatQuery::new(query, schema.get_field("time").expect("field time"), 78356886, 78366880, 100000);
    let searcher = reader.searcher();
    let time = std::time::SystemTime::now();
    let result = query_all(&searcher, &query, &schema, "time");
    println!("{:?}, time:{:?}", result.1, std::time::SystemTime::now().duration_since(time).expect("time"));
}

fn  main() {
    read_dir(&"./cattrace-20190830/0".to_string());
}
