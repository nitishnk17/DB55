use common::{Data, DataType};
use db_config::table::ColumnSpec;
use anyhow::{Result, Context};

/// A Row represents a single record in a table
/// It contains a list of 'Data' values (Int32, String, etc.) corresponding to the table's schema
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Data>,
}

impl Row {
    /// Decodes a single row from a raw byte slice based on the table's schema
    /// # Arguments:
    /// * `bytes` - The raw data starting at the beginning of this row.
    /// * `schema` - The list of columns (and their types) we expect to find
    /// # Returns:
    /// * `Ok((Row, usize))` - The decoded Row and the total number of bytes consumed to read it
    pub fn decode(bytes: &[u8], schema: &[ColumnSpec]) -> Result<(Self, usize)> {
        let mut values = Vec::new();
        let mut offset = 0; // Tracks our current position (in bytes) inside the row

        for col in schema {
            match col.data_type {
                DataType::Int32 => {
                    // Int32 is 4 bytes. We convert them from Little Endian (LE) to a Rust i32.
                    let slice = &bytes[offset..offset+4];
                    let val = i32::from_le_bytes(slice.try_into().context("Failed to read Int32")?);
                    values.push(Data::Int32(val));
                    offset += 4; // Move the cursor forward by 4 bytes
                }
                DataType::Int64 => {
                    // Int64 is 8 bytes
                    let slice = &bytes[offset..offset+8];
                    let val = i64::from_le_bytes(slice.try_into().context("Failed to read Int64")?);
                    values.push(Data::Int64(val));
                    offset += 8;
                }
                DataType::Float32 => {
                    // Float32 is 4 bytes
                    let slice = &bytes[offset..offset+4];
                    let val = f32::from_le_bytes(slice.try_into().context("Failed to read Float32")?);
                    values.push(Data::Float32(val));
                    offset += 4;
                }
                DataType::Float64 => {
                    // Float64 is 8 bytes
                    let slice = &bytes[offset..offset+8];
                    let val = f64::from_le_bytes(slice.try_into().context("Failed to read Float64")?);
                    values.push(Data::Float64(val));
                    offset += 8;
                }
                DataType::String => {
                    // Strings in this database are "Null-Terminated"
                    // We scan the bytes starting at 'offset' until we find a 0 byte
                    let mut end = offset;
                    while end < bytes.len() && bytes[end] != 0 {
                        end += 1;
                    }
                    
                    // Convert the slice of bytes into a human-readable UTF-8 string
                    let s = std::str::from_utf8(&bytes[offset..end])
                        .context("Invalid UTF-8 string found in data")?;
                    
                    values.push(Data::String(s.to_string()));
                    
                    // Move offset past the string AND the null terminator +1
                    offset = end + 1;
                }
            }
        }

        // Return the final Row object and how many bytes we ate
        Ok((Row { values }, offset))
    }
}
