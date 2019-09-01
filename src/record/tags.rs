use std::io::{self, Read};
use std::io::ErrorKind::InvalidData;
use std::mem;

use byteorder::{LittleEndian, ReadBytesExt};

use super::{Error, resize};

/// Enum that represents tag type for the cases when a tag contains integer.
///
/// Possible values are `I8` (`c`), `U8` (`C`), `I16` (`s`), `U16` (`S`), `I32` (`i`) and `U32` (`I`).
#[derive(Clone, Copy)]
pub enum IntegerType {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
}

impl IntegerType {
    /// Returns a letter that represents the integer type. For example, `i8` corresponds to `c`.
    pub fn letter(self) -> u8 {
        use IntegerType::*;
        match self {
            I8 => b'c',
            U8 => b'C',
            I16 => b's',
            U16 => b'S',
            I32 => b'i',
            U32 => b'I',
        }
    }

    /// Returns IntegerType from a letter, such as `c`.
    pub fn from_letter(ty: u8) -> Option<Self> {
        use IntegerType::*;
        match ty {
            b'c' => Some(I8),
            b'C' => Some(U8),
            b's' => Some(I16),
            b'S' => Some(U16),
            b'i' => Some(I32),
            b'I' => Some(U32),
            _ => None,
        }
    }

    pub fn size_of(self) -> usize {
        use IntegerType::*;
        match self {
            I8 | U8 => 1,
            I16 | U16 => 2,
            I32 | U32 => 4,
        }
    }

    pub fn parse_raw(self, mut raw_tag: &[u8]) -> i64 {
        use IntegerType::*;
        match self {
            I8 => unsafe { mem::transmute::<u8, i8>(raw_tag[0]) as i64 },
            U8 => raw_tag[0] as i64,
            I16 => raw_tag.read_i16::<LittleEndian>()
                .expect("Failed to extract tag from a raw representation.") as i64,
            U16 => raw_tag.read_u16::<LittleEndian>()
                .expect("Failed to extract tag from a raw representation.") as i64,
            I32 => raw_tag.read_i32::<LittleEndian>()
                .expect("Failed to extract tag from a raw representation.") as i64,
            U32 => raw_tag.read_u32::<LittleEndian>()
                .expect("Failed to extract tag from a raw representation.") as i64,
        }
    }
}

fn parse_float(mut raw_tag: &[u8]) -> f32 {
    raw_tag.read_f32::<LittleEndian>()
        .expect("Failed to extract tag from a raw representation.")
}

/// Enum that represents tag type for `String` and `Hex` types (`Z` and `H`).
#[derive(Clone, Copy)]
pub enum StringType {
    String,
    Hex,
}

impl StringType {
    /// Returns a letter that represents the string type.
    pub fn letter(&self) -> u8 {
        match self {
            StringType::String => b'Z',
            StringType::Hex => b'H',
        }
    }

    fn from_letter(ty: u8) -> Option<Self> {
        match ty {
            b'Z' => Some(StringType::String),
            b'H' => Some(StringType::Hex),
            _ => None,
        }
    }
}

/// Wrapper around raw integer array stored in a tag.
pub struct IntArrayView<'a> {
    raw: &'a [u8],
    int_type: IntegerType,
}

impl<'a> IntArrayView<'a> {
    /// Get the type of the inner array
    pub fn int_type(&self) -> IntegerType {
        self.int_type
    }

    pub fn len(&self) -> usize {
        self.raw.len() / self.int_type.size_of()
    }

    /// Returns an element at the `index`. Returns `i64` to include both `i32` and `u32`.
    pub fn at(&self, index: usize) -> i64 {
        let start = self.int_type.size_of() * index;
        let end = start + self.int_type.size_of();
        if end > self.raw.len() {
            panic!("Index out of bounds: index {}, len {}", index, self.len());
        }
        self.int_type.parse_raw(&self.raw[start..end])
    }

    /// Returns `i64` iterator
    pub fn iter<'b: 'a>(&'b self) -> impl Iterator<Item = i64> + 'b {
        self.raw.chunks(self.int_type.size_of()).map(move |chunk| self.int_type.parse_raw(chunk))
    }
}

/// Wrapper around raw float array stored in a tag.
pub struct FloatArrayView<'a> {
    raw: &'a [u8],
}

impl<'a> FloatArrayView<'a> {
    pub fn len(&self) -> usize {
        self.raw.len() / 4
    }

    /// Returns an element at the `index`.
    pub fn at(&self, index: usize) -> f32 {
        let start = 4 * index;
        let end = start + 4;
        if end > self.raw.len() {
            panic!("Index out of bounds: index {}, len {}", index, self.len());
        }
        parse_float(&self.raw[start..end])
    }

    pub fn iter(&self) -> impl Iterator<Item = f32> + 'a {
        self.raw.chunks(4).map(|chunk| parse_float(chunk))
    }
}

/// Enum with all possible tag values.
///
/// If a tag contains integer value, or array with integer values, this enum will store `i64`,
/// to be able to contain both types `i` (`i32`) and `I` (`u32`).
pub enum TagValue<'a> {
    Char(u8),
    Int(i64, IntegerType),
    Float(f32),
    String(&'a [u8], StringType),
    IntArray(IntArrayView<'a>),
    FloatArray(FloatArrayView<'a>),
}

impl<'a> TagValue<'a> {
    /// Get [TagValue](enum.TagValue.html) from a raw representation.
    /// This function expects the raw tag to have a correct length.
    fn from_raw(ty: u8, mut raw_tag: &'a [u8]) -> TagValue<'a> {
        use TagValue::*;
        use IntegerType::*;

        if let Some(int_type) = IntegerType::from_letter(ty) {
            return Int(int_type.parse_raw(raw_tag), int_type);
        }
        if let Some(str_type) = StringType::from_letter(ty) {
            return String(&raw_tag[..raw_tag.len() - 1], str_type);
        }

        match ty {
            b'A' => Char(raw_tag[0]),
            b'f' => Float(parse_float(raw_tag)),
            b'B' => {
                let arr_ty = raw_tag[0];
                if arr_ty == b'f' {
                    return FloatArray(FloatArrayView {
                        raw: &raw_tag[1..],
                    });
                }
                if let Some(int_type) = IntegerType::from_letter(arr_ty) {
                    return IntArray(IntArrayView {
                        raw: &raw_tag[1..],
                        int_type,
                    });
                }
                panic!("Unexpected tag array type: {}", arr_ty as char);
            }
            _ => panic!("Unexpected tag type: {}", ty as char),
        }
    }
}

/// Wrapper around raw tags
pub struct TagViewer {
    raw: Vec<u8>,
    lengths: Vec<u32>,
}

/// Get a size of type from letter (c -> 1), (i -> 4). Returns Error for non int/float types
fn tag_type_size(ty: u8) -> Result<u32, Error> {
    match ty {
        b'c' | b'C' => Ok(1),
        b's' | b'S' => Ok(2),
        b'i' | b'I' | b'f' => Ok(4),
        _ => Err(Error::Corrupted("Unexpected tag type")),
    }
}

/// Get the length of the first tag (including name) in a raw tags array.
///
/// For example, the function would return 7 for the raw representation of `"AA:i:10    BB:i:20"`.
fn get_length(raw_tags: &[u8]) -> Result<u32, Error> {
    if raw_tags.len() < 4 {
        return Err(Error::Corrupted("Truncated tags"));
    }
    let ty = raw_tags[2];
    match ty {
        b'Z' | b'H' => {
            for i in 3..raw_tags.len() {
                if raw_tags[i] == 0 {
                    if ty == b'H' && i % 2 != 0 {
                        return Err(Error::Corrupted("Hex tag has uneven number of bytes"));
                    }
                    // 3 (tag:ty) + index + 1
                    return Ok(4 + i as u32);
                }
            }
            Err(Error::Corrupted("Truncated tags"))
        },
        b'B' => {
            if raw_tags.len() < 8 {
                return Err(Error::Corrupted("Truncated tags"));
            }
            let arr_len = (&raw_tags[4..8]).read_i32::<LittleEndian>()? as u32;
            Ok(8 + tag_type_size(raw_tags[3])? * arr_len)
        },
        _ => Ok(3 + tag_type_size(raw_tags[2])?),
    }
}

impl TagViewer {
    /// Create a new tag viewer
    pub(crate) fn new() -> Self {
        Self {
            raw: Vec::new(),
            lengths: Vec::new(),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.raw.clear();
        self.lengths.clear();
    }

    fn fill_from<R: Read>(&mut self, stream: &mut R, length: usize) -> Result<(), Error> {
        unsafe {
            resize(&mut self.raw, length);
        }
        stream.read_exact(&mut self.raw)?;
        self.lengths.clear();
        let mut sum_len = 0;
        while sum_len < self.raw.len() {
            let tag_len = get_length(&self.raw[sum_len..])?;
            self.lengths.push(tag_len);
            sum_len += tag_len as usize;
        }
        if sum_len > self.raw.len() {
            Err(Error::Corrupted("Truncated tags"))
        } else {
            Ok(())
        }
    }
}