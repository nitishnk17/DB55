use common::{Data, DataType};
use std::io::Write;

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Data>,
}

pub(crate) fn build_needed_mask(types_len: usize, needed: &[usize]) -> Vec<bool> {
    let mut mask = vec![false; types_len];
    for &idx in needed {
        if idx < types_len {
            mask[idx] = true;
        }
    }
    mask
}

#[inline]
fn count_needed_columns(needed_mask: &[bool]) -> usize {
    needed_mask.iter().filter(|&&needed| needed).count()
}

#[inline]
fn decode_row_with_mask(
    bytes: &[u8],
    types: &[DataType],
    needed_mask: &[bool],
    needed_count: usize,
) -> Option<(Row, usize)> {
    let mut values = Vec::with_capacity(needed_count);
    let mut offset = 0;
    for (i, dt) in types.iter().enumerate() {
        let is_needed = needed_mask[i];
        match dt {
            DataType::Int32 => {
                if offset + 4 > bytes.len() {
                    return None;
                }
                if is_needed {
                    let val = i32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    values.push(Data::Int32(val));
                }
                offset += 4;
            }
            DataType::Int64 => {
                if offset + 8 > bytes.len() {
                    return None;
                }
                if is_needed {
                    let val = i64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                    values.push(Data::Int64(val));
                }
                offset += 8;
            }
            DataType::Float32 => {
                if offset + 4 > bytes.len() {
                    return None;
                }
                if is_needed {
                    let val = f32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
                    values.push(Data::Float32(val));
                }
                offset += 4;
            }
            DataType::Float64 => {
                if offset + 8 > bytes.len() {
                    return None;
                }
                if is_needed {
                    let val = f64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                    values.push(Data::Float64(val));
                }
                offset += 8;
            }
            DataType::String => {
                if offset >= bytes.len() {
                    return None;
                }
                let end = bytes[offset..].iter().position(|&b| b == 0)?;
                if is_needed {
                    let val = String::from_utf8(bytes[offset..offset + end].to_vec()).unwrap();
                    values.push(Data::String(val));
                }
                offset += end + 1;
            }
        }
    }
    Some((Row { values }, offset))
}

#[inline]
fn decode_row_all(bytes: &[u8], types: &[DataType]) -> Option<(Row, usize)> {
    let mut values = Vec::with_capacity(types.len());
    let mut offset = 0;
    for dt in types {
        match dt {
            DataType::Int32 => {
                if offset + 4 > bytes.len() {
                    return None;
                }
                values.push(Data::Int32(i32::from_le_bytes(
                    bytes[offset..offset + 4].try_into().unwrap(),
                )));
                offset += 4;
            }
            DataType::Int64 => {
                if offset + 8 > bytes.len() {
                    return None;
                }
                values.push(Data::Int64(i64::from_le_bytes(
                    bytes[offset..offset + 8].try_into().unwrap(),
                )));
                offset += 8;
            }
            DataType::Float32 => {
                if offset + 4 > bytes.len() {
                    return None;
                }
                values.push(Data::Float32(f32::from_le_bytes(
                    bytes[offset..offset + 4].try_into().unwrap(),
                )));
                offset += 4;
            }
            DataType::Float64 => {
                if offset + 8 > bytes.len() {
                    return None;
                }
                values.push(Data::Float64(f64::from_le_bytes(
                    bytes[offset..offset + 8].try_into().unwrap(),
                )));
                offset += 8;
            }
            DataType::String => {
                if offset >= bytes.len() {
                    return None;
                }
                let end = bytes[offset..].iter().position(|&b| b == 0)?;
                let val = String::from_utf8(bytes[offset..offset + end].to_vec()).unwrap();
                values.push(Data::String(val));
                offset += end + 1;
            }
        }
    }
    Some((Row { values }, offset))
}

pub fn decode_row(bytes: &[u8], types: &[DataType], needed: &[usize]) -> (Row, usize) {
    let needed_mask = build_needed_mask(types.len(), needed);
    let needed_count = count_needed_columns(&needed_mask);
    decode_row_with_mask(bytes, types, &needed_mask, needed_count)
        .unwrap_or((Row { values: Vec::new() }, bytes.len()))
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

impl Row {
    /// Write this row directly to a [`Write`] sink without going through the
    /// [`std::fmt`] machinery — saves the per-float `String` allocation that
    /// `format_float` would otherwise perform on every output row.
    ///
    /// For numeric values we pack `digits + '|'` into a single stack buffer and
    /// issue one `write_all` per column.  Strings still take two writes (the
    /// payload may be arbitrarily long).
    pub fn write_to<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        for value in &self.values {
            match value {
                Data::Int32(v) => {
                    let mut buf = [0u8; 25];
                    let s = i64_to_str_with_pipe(*v as i64, &mut buf);
                    w.write_all(s)?;
                }
                Data::Int64(v) => {
                    let mut buf = [0u8; 25];
                    let s = i64_to_str_with_pipe(*v, &mut buf);
                    w.write_all(s)?;
                }
                Data::Float32(v) => write_float_with_pipe(*v as f64, w)?,
                Data::Float64(v) => write_float_with_pipe(*v, w)?,
                Data::String(v) => {
                    w.write_all(v.as_bytes())?;
                    w.write_all(b"|")?;
                }
            }
        }
        Ok(())
    }
}

/// Format an i64 into the back of a 25-byte buffer with a trailing `|`,
/// returning the populated slice.  No heap allocation; ASCII bytes only.
/// Layout: `[..digits..][|]` ending at index 24.
#[inline]
fn i64_to_str_with_pipe(mut v: i64, buf: &mut [u8; 25]) -> &[u8] {
    buf[24] = b'|';
    if v == 0 {
        buf[23] = b'0';
        return &buf[23..];
    }
    if v < 0 {
        // Avoid overflow on i64::MIN via i128 widening.
        let mut n = (v as i128).unsigned_abs();
        let mut idx = 24usize;
        while n > 0 {
            idx -= 1;
            buf[idx] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        idx -= 1;
        buf[idx] = b'-';
        return &buf[idx..];
    }
    let mut idx = 24usize;
    while v > 0 {
        idx -= 1;
        buf[idx] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    &buf[idx..]
}

/// Write a float in SQLite-compatible `%.15g` form, packing the trailing `|`
/// (and the `".0"` suffix when the formatted value lacks a decimal/exponent)
/// into the same stack buffer so we issue exactly one `write_all` per column.
#[inline]
fn write_float_with_pipe<W: Write>(v: f64, w: &mut W) -> std::io::Result<()> {
    let mut buf = [0u8; 64];
    let n = unsafe {
        libc::snprintf(
            buf.as_mut_ptr() as *mut libc::c_char,
            buf.len(),
            c"%.15g".as_ptr() as *const libc::c_char,
            v,
        )
    };
    if n <= 0 {
        return Ok(());
    }
    let mut len = n as usize;
    let formatted = &buf[..len];
    let needs_dot = !formatted.contains(&b'.')
        && !formatted.contains(&b'e')
        && !formatted.contains(&b'E');
    // %.15g maxes around 24 chars, so len + 3 always fits in a 64-byte buffer.
    if needs_dot {
        buf[len] = b'.';
        buf[len + 1] = b'0';
        buf[len + 2] = b'|';
        len += 3;
    } else {
        buf[len] = b'|';
        len += 1;
    }
    w.write_all(&buf[..len])
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
    let mut buffer = [0u8; 64];
    let s = unsafe {
        let len = libc::snprintf(
            buffer.as_mut_ptr() as *mut libc::c_char,
            buffer.len(),
            c"%.15g".as_ptr() as *const libc::c_char,
            v,
        );
        std::str::from_utf8_unchecked(&buffer[..len as usize]).to_string()
    };

    // Ensure there's always a decimal point (SQLite always shows one for REAL)
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        format!("{}.0", s)
    } else {
        s
    }
}

pub fn decode_block(block_data: &[u8], types: &[DataType], needed: &[usize]) -> Vec<Row> {
    let needed_mask = build_needed_mask(types.len(), needed);
    decode_block_with_mask(block_data, types, &needed_mask)
}

pub fn decode_block_with_mask(
    block_data: &[u8],
    types: &[DataType],
    needed_mask: &[bool],
) -> Vec<Row> {
    if block_data.len() < 2 {
        return Vec::new();
    }
    let row_count = u16::from_le_bytes(block_data[block_data.len() - 2..].try_into().unwrap());
    let mut rows = Vec::with_capacity(row_count as usize);
    decode_block_with_mask_into(block_data, types, needed_mask, &mut rows);
    rows
}

pub fn decode_block_with_mask_into(
    block_data: &[u8],
    types: &[DataType],
    needed_mask: &[bool],
    rows: &mut Vec<Row>,
) {
    if block_data.len() < 2 {
        return;
    }
    let row_count = u16::from_le_bytes(block_data[block_data.len() - 2..].try_into().unwrap());
    rows.reserve(row_count as usize);
    let needed_count = count_needed_columns(needed_mask);
    let all_needed = needed_count == types.len();
    let mut offset = 0;
    for _ in 0..row_count {
        if offset >= block_data.len() - 2 {
            break;
        }
        let payload = &block_data[offset..block_data.len() - 2];
        let decoded = if all_needed {
            decode_row_all(payload, types)
        } else {
            decode_row_with_mask(payload, types, needed_mask, needed_count)
        };
        let (row, row_len) = match decoded {
            Some(v) => v,
            None => break,
        };
        rows.push(row);
        offset += row_len;
    }
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
