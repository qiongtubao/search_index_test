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
mod only_read_directory;

mod query_builder;
mod query_parser;
fn schema_from_file(schema_path: String) {
    let file_path:&str = &schema_path;
   let schema = std::fs::read_to_string(schema_path.clone()).expect(&format!("read {} file error  from file", file_path));
    serde_json::from_str(&schema).expect(&format!("from {} parse schema expect", file_path))
}

fn query_count(searcher: &Searcher, query: &dyn Query) -> usize {
    searcher.search(query, &Count).expect("search")
}
fn query_all(searcher: &Searcher, query: &BooleanQuery, schema: &Schema, sort_field: &str ) -> (Vec<(u64, String)>,usize) {
    let mut collectors = MultiCollector::new();
    let top_collector = TopDocs::with_limit(400).order_by_u64_field(schema.get_field(sort_field).expect("????"));
    let topdocs_handler = collectors.add_collector(top_collector);
    let count_handler = collectors.add_collector(Count);
    println!("query: {:?}", query);
    let mut multifruits = searcher.search(query, &mut collectors).expect("search");
    let top_docs = topdocs_handler.extract(&mut multifruits);
    let count = count_handler.extract(&mut multifruits);
    let mut v = vec![];
    //通过文档地址查询文档
    for (feature, doc_address) in top_docs {
        if let Ok(doc) = searcher.doc(doc_address) {
            v.push((feature, schema.to_json(&doc).to_string()));
        }
    }
    (v, count)
}
fn read_dir(dir_name : &String) {
    let path = std::path::PathBuf::from(dir_name);
    let dir = OnlyReadDirectory::new(path);
//    let dir = MmapDirectory::open(path).expect("open error");
    let index = Index::open(dir).expect("open dir error");
    let schema = index.schema();
    let reader = index.reader().expect("reader");
    let query =
        std::fs::read_to_string("./query.json").expect("error parsing config from file");
    let query = query_parser::parse(query, schema.clone());
    let searcher = reader.searcher();
    let result = query_all(&searcher, &query, &schema, "time");
    println!("{:?}", result.1);
}

fn  main() {
    read_dir(&"./cattrace-20190830/0".to_string());
}
