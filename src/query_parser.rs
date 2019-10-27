use crate::query_builder::QueryBuilder;
use serde_json::Value;
use tantivy::query::{Occur, BooleanQuery, Query};
use tantivy::schema::Schema;
use crate::query::CatQuery;

pub fn parse(query: String, schema: Schema, size: usize) -> Box<dyn Query> {
    let builder = QueryBuilder::new(schema.clone(), Occur::Must, size);
    let mut query = {
        if let Some(query) = serde_json::from_str(&query).ok() {
            builder.parse(&query).build()
        } else {
            builder.build()
        }
    };
    match query.downcast_mut::<CatQuery>() {
        Some(c) => {
            c.set_limit(size);
        }
        _ => {}
    }
    query
}
impl QueryBuilder {
    pub fn parse(self, v: &Value) -> Self {
        if let Some(v) = v.get("query") {
            return self.parse(v)
        }

        if let Some(v) = v.get("bool") {
            if let Some(v) = v.get("filter") {
                return self.down(Occur::Must).parse(v).up()
            }
            if let Some(v) = v.get("should") {
                return self.down(Occur::Should).parse(v).up()
            }
        }
        if let Some(v) = v.as_array() {
            return v.iter().fold(self, |t,v| {
                t.parse(v)
            })
        }
        if let Some(v) = v.get("prefix") {
            if let Some(v) = v.as_object() {
                return v.iter().fold(self, |t, (k,v)| {
                    if let Some(v) = v["value"].as_str() {
                        return t.add_prefix_query(k, v)
                    }
                    t
                })
            }
        }
        if let Some(v) = v.get("term") {
            if let Some(v) = v.as_object() {
                return v.iter().fold(self, |t, (k, v)| {
                    if let Some(v) = v["value"].as_u64() {
                        return t.add_term_query_u64(k, v)
                    }
                    if let Some(v) = v["value"].as_str() {
                        return t.add_term_query_str(k, v)
                    }
                    t
                })
            }
        }
        if let Some(v) = v.get("range") {
            if let Some(v) = v.as_object() {
                return v.iter().fold(self, |t,(k,v )| {
                    //需要4个参数都有才行
                    if let Some(left) = v["from"].as_u64() {
                        if let Some(right) = v["to"].as_u64() {
                            if let Some(include_left) = v["include_lower"].as_bool() {
                                if let Some(include_right) = v["include_upper"].as_bool() {
                                    return t.add_range_query(k, left, right, include_left, include_right);
                                }
                            }
                        }
                    }
                    t
                })
            }
        }

        self
    }
}