use serde::{Deserialize, Serialize};

pub mod query;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Data {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum DataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
}

impl PartialOrd for Data {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Int32(l), Self::Int32(r)) => l.partial_cmp(r),
            (Self::Int64(l), Self::Int64(r)) => l.partial_cmp(r),
            (Self::Float32(l), Self::Float32(r)) => l.partial_cmp(r),
            (Self::Float64(l), Self::Float64(r)) => l.partial_cmp(r),
            (Self::String(l), Self::String(r)) => l.partial_cmp(r),
            // Cross-type numeric promotion
            (Self::Float64(l), Self::Int64(r)) => l.partial_cmp(&(*r as f64)),
            (Self::Int64(l), Self::Float64(r)) => (*l as f64).partial_cmp(r),
            (Self::Float64(l), Self::Int32(r)) => l.partial_cmp(&(*r as f64)),
            (Self::Int32(l), Self::Float64(r)) => (*l as f64).partial_cmp(r),
            (Self::Float32(l), Self::Int32(r)) => l.partial_cmp(&(*r as f32)),
            (Self::Int32(l), Self::Float32(r)) => (*l as f32).partial_cmp(r),
            (Self::Float32(l), Self::Int64(r)) => (*l as f64).partial_cmp(&(*r as f64)),
            (Self::Int64(l), Self::Float32(r)) => (*l as f64).partial_cmp(&(*r as f64)),
            (Self::Int64(l), Self::Int32(r)) => l.partial_cmp(&(*r as i64)),
            (Self::Int32(l), Self::Int64(r)) => (*l as i64).partial_cmp(r),
            _ => None,
        }
    }
}

impl PartialEq for Data {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int32(l), Self::Int32(r)) => l == r,
            (Self::Int64(l), Self::Int64(r)) => l == r,
            (Self::Float32(l), Self::Float32(r)) => l == r,
            (Self::Float64(l), Self::Float64(r)) => l == r,
            (Self::String(l), Self::String(r)) => l == r,
            // Cross-type numeric promotion
            (Self::Float64(l), Self::Int64(r)) => *l == *r as f64,
            (Self::Int64(l), Self::Float64(r)) => *l as f64 == *r,
            (Self::Float64(l), Self::Int32(r)) => *l == *r as f64,
            (Self::Int32(l), Self::Float64(r)) => *l as f64 == *r,
            (Self::Int64(l), Self::Int32(r)) => *l == *r as i64,
            (Self::Int32(l), Self::Int64(r)) => *l as i64 == *r,
            _ => false,
        }
    }
}
