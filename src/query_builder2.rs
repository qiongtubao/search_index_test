use tantivy::schema::{Schema, IndexRecordOption};
use tantivy::query::{Query, Occur, BooleanQuery, TermQuery, RegexQuery, RangeQuery};
use std::rc::Rc;
use tantivy::Term;
use std::collections::Bound;

pub struct QueryBuilder {
    c: Vec<(Occur, Box<Query>)>,
    p: Option<Box<QueryBuilder>>,
    s: Rc<Schema>,
    o: Occur,
}

impl QueryBuilder {
    pub fn new(schema: Schema, occur: Occur) -> Self {
        QueryBuilder {
            c: vec![],
            p: None,
            s: Rc::new(schema),
            o: occur
        }
    }
    pub fn down(self, occur: Occur) -> QueryBuilder {
        QueryBuilder {
            c: vec![],
            s: Rc::clone(&self.s),
            p: Some(Box::new(self)),
            o: occur
        }
    }
    pub fn also(mut self, occur: Occur) -> QueryBuilder {
        self.o = occur;
        self
    }
    pub fn up(self)-> QueryBuilder {
        match self.p {
            Some(mut p) => {
                p.c.push((p.o, Box::new(BooleanQuery::from(self.c))));
                *p
            },
            None => {
                panic!("exceeding root");
            }
        }
    }
    pub fn add_term_query(mut self, field: &str, value: &str) -> Self {
        let field= match self.s.get_field(field) {
            Some(field) => field,
            None => return self,
        };
        let query = TermQuery::new(Term::from_field_text(field, value), IndexRecordOption::Basic);
        self.c.push((self.o, Box::new(query)));
        self
    }
    pub fn add_prefix_query(mut self, field: &str, value: &str) -> Self {
        let field = match self.s.get_field(field) {
            Some(field) => field,
            None => return self,
        };
        let query = RegexQuery::new(format!(r"{}[u:\x00-u:\xFF]*", value), field);
        self.c.push((self.o, Box::new(query)));
        self
    }
    pub fn build(self) -> BooleanQuery {
        BooleanQuery::from(self.c)
    }
    pub fn add_range_query(mut self, field: &str, left: u64, right: u64,include_left: bool, include_right: bool) -> Self {
        let field = match self.s.get_field(field) {
            Some(field) => field,
            None => return self
        };
        let left = {
            if include_left { Bound::Included(left)} else {
                Bound::Excluded(left)
            }
        };
        let right = {
            if include_right { Bound::Included(right) } else {
                Bound::Excluded(right)
            }
        };
        let query = RangeQuery::new_u64_bounds(field, left, right);
        self.c.push((self.o, Box::new(query)));
        self
    }
    pub fn add_term_query_str(mut self,  field: &str, value: &str) -> Self {
        let field = match self.s.get_field(field) {
            Some(field) => field,
            None => return self,
        };
        let query = TermQuery::new(Term::from_field_text(field, value),
        IndexRecordOption::Basic);
        self.c.push((self.o, Box::new(query)));
        self
    }
    pub fn add_term_query_u64(mut self, field: &str, value: u64) -> Self {
        let field = match self.s.get_field(field) {
            Some(field) => field,
            None => return self,
        };
        let query = TermQuery::new(
            Term::from_field_u64(field, value),
            IndexRecordOption::Basic,
        );
        self.c.push((self.o, Box::new(query)));
        self
    }
}