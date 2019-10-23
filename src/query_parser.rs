use crate::query_builder::QueryBuilder;
use serde_json::Value;
use tantivy::query::{Occur, BooleanQuery};
use tantivy::schema::Schema;

pub fn parse(query: String, schema: Schema) -> BooleanQuery {
    let builder = QueryBuilder::new(schema.clone(), Occur::Must);
    if let Some(query) = serde_json::from_str(&query).ok() {
        return builder.parse(&query).build()
    }
    return builder.build()
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