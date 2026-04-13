use common::{Data, DataType};

#[derive(Debug, Clone)]
pub struct Row{
    pub values: Vec<Data>,
}

pub fn decode_row(bytes: &[u8], types: &[DataType]) -> (Row, usize){
    let mut values = Vec::new();
    let mut offset = 0;
    for dt in types {
        match dt {
            DataType::Int32 => {
                let val = i32::from_le_bytes(bytes[offset..offset+4].try_into().unwrap());
                values.push(Data::Int32(val));
                offset += 4;
            }
            DataType::Int64 => {
                let val = i64::from_le_bytes(bytes[offset..offset+8].try_into().unwrap());
                values.push(Data::Int64(val));
                offset += 8;
            }
            DataType::Float32 => {
                let val = f32::from_le_bytes(bytes[offset..offset+4].try_into().unwrap());
                values.push(Data::Float32(val));
                offset += 4;
            }
            DataType::Float64 => {
                let val = f64::from_le_bytes(bytes[offset..offset+8].try_into().unwrap());
                values.push(Data::Float64(val));
                offset += 8;
            }
            DataType::String => {
                let end = bytes[offset..].iter().position(|&b| b == 0).unwrap();
                let val = String::from_utf8(bytes[offset..offset+end].to_vec()).unwrap();
                values.push(Data::String(val));
                offset += end + 1;
            }
        }
    }
    (Row { values }, offset)
}


impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for value in &self.values {
            match value {
                Data::Int32(v) => write!(f, "{}|", v)?,
                Data::Int64(v) => write!(f, "{}|", v)?,
                Data::Float32(v) => write!(f, "{}|", format_float(*v as f64))?,
                Data::Float64(v) => write!(f, "{}|", format_float(*v))?,
                Data::String(v) => write!(f, "{}|", v)?,
            }
        }
        Ok(())
    }
}
/// Format a float to match SQLite output.
///
/// SQLite uses the equivalent of C's `%.15g` format for REAL values:
/// up to 15 significant digits, trailing zeros stripped, with a decimal
/// point always present for non-scientific notation.
///
/// Examples:
/// - 50.0       -> "50.0"
/// - 19.40      -> "19.4"
/// - 0.123      -> "0.123"
/// - 1234.5678  -> "1234.5678"
/// - 0.001      -> "0.001"
/// - 3.14159    -> "3.14159"
fn format_float(v: f64) -> String {
    // Use the same precision SQLite uses internally (%.15g)
    let s = format!("{:.15g}", v);
    // Ensure there's always a decimal point (SQLite always shows one for REAL)
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        format!("{}.0", s)
    } else {
        s
    }
}

pub fn decode_block(block_data: &[u8], types: &[DataType]) -> Vec<Row> {
    let row_count = u16::from_le_bytes(block_data[block_data.len()-2..].try_into().unwrap());
    let mut rows = Vec::new();
    let mut offset = 0;
    for _ in 0..row_count {
        let (row, row_len) = decode_row(&block_data[offset..], types);
        rows.push(row);
        offset += row_len;
    }
    rows
}

pub fn encode_row(row: &Row) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in &row.values {
        match value {
            Data::Int32(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            Data::Int64(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            Data::Float32(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            Data::Float64(v) => bytes.extend_from_slice(&v.to_le_bytes()),
            Data::String(v) => {
                bytes.extend_from_slice(v.as_bytes());
                bytes.push(0x00);
            }
        }
    }
    bytes
}
