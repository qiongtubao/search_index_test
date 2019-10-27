use tantivy::schema::{Schema, IndexRecordOption, Field};
use tantivy::query::{Query, Occur, BooleanQuery, TermQuery, RegexQuery, RangeQuery};
use std::rc::Rc;
use tantivy::Term;
use std::collections::Bound;
use crate::query::CatQuery;


struct CatQueryBuilder {
    c: Vec<(Occur, Box<Query>)>,
    left: u64,
    right: u64,
    field: Option<Field>,
    limit : usize
}
impl CatQueryBuilder {
    fn build(mut self) -> Box<dyn Query>{
        if let Some(field) = self.field {
            Box::new(CatQuery::new(BooleanQuery::from(self.c), field, self.left, self.right, self.limit))
        }else {
            if self.c.len() == 1 {
                let (a, b) =  self.c.pop().expect("");
                return b;
            }else{
                Box::new(BooleanQuery::from(self.c))
            }
        }
    }
    fn push(&mut self, c: (Occur, Box<Query>)) {
        self.c.push(c);
    }
    fn add_range_query(&mut self, field: Field, left: u64, right: u64, occur: Occur) {
        if let Some(field) = self.field {
            self.c.push((occur, Box::new(RangeQuery::new_u64(field, left..right))));
        }else {
           self.left = left;
            self.right = right;
            self.field = Some(field);
        }
    }

}
pub struct QueryBuilder {
    c: CatQueryBuilder,
    p: Option<Box<QueryBuilder>>,
    s: Rc<Schema>,
    o: Occur,
}

impl QueryBuilder {
    pub fn new(schema: Schema, occur: Occur, size: usize) -> Self {
        QueryBuilder {
            c: CatQueryBuilder {
                c: vec![],
                left: 0,
                right: 0,
                field: None,
                limit: size
            },
            p: None,
            s: Rc::new(schema),
            o: occur
        }
    }
    pub fn down(self, occur: Occur) -> QueryBuilder {
        QueryBuilder {
            c: CatQueryBuilder {
                c: vec![],
                left: 0,
                right: 0,
                field: None,
                limit : 0
            },
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
                p.c.push((p.o, self.c.build()));
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
    pub fn build(self) -> Box<dyn Query> {
        self.c.build()
    }
    pub fn add_range_query(mut self, field: &str, left: u64, right:u64, include_left:bool, include_right: bool) -> Self {
        let field = match self.s.get_field(field) {
            Some(field) => field,
            None => return self
        };
        let left = {
            if !include_left {
                left + 1
            }else {
                left
            }
        };
        let right = {
            if !include_right {
                right - 1
            }else{
                right
            }
        };
        self.c.add_range_query(field, left, right, self.o);
        self
    }
//    pub fn add_range_query(mut self, field: &str, left: u64, right: u64,include_left: bool, include_right: bool) -> Self {
//        let field = match self.s.get_field(field) {
//            Some(field) => field,
//            None => return self
//        };
//        let left = {
//            if include_left { Bound::Included(left)} else {
//                Bound::Excluded(left)
//            }
//        };
//        let right = {
//            if include_right { Bound::Included(right) } else {
//                Bound::Excluded(right)
//            }
//        };
//        let query = RangeQuery::new_u64_bounds(field, left, right);
//        self.c.push((self.o, Box::new(query)));
//        self
//    }
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