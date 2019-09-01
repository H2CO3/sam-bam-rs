use std::io::{self, Read, Write};
use std::mem;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

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

    /// Returns raw array
    pub fn raw(&self) -> &[u8] {
        self.raw
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

    /// Write the tag value in a sam format
    pub fn write_sam<W: Write>(&self, f: &mut W) -> io::Result<()> {
        use TagValue::*;
        match self {
            Char(value) => f.write_all(&[b'A', b':', *value]),
            Int(value, _) => write!(f, "i:{}", *value),
            Float(value) => write!(f, "f:{}", *value),
            String(u8_slice, str_type) => {
                f.write_all(&[str_type.letter(), b':'])?;
                f.write_all(u8_slice)
            },
            IntArray(arr_view) => {
                f.write_all(b"B:i")?;
                for value in arr_view.iter() {
                    write!(f, ",{}", value)?;
                }
                Ok(())
            },
            FloatArray(arr_view) => {
                f.write_all(b"B:f")?;
                for value in arr_view.iter() {
                    write!(f, ",{}", value)?;
                }
                Ok(())
            },
        }
    }
}

/// Wrapper around `&u8`, used to specify tag type as _Hex_, not `String` and not `&[u8]`.
/// Should have even number of characters.
pub struct Hex<'a>(pub &'a [u8]);

mod private {
    /// Sealed trait, designed to forbid new implementations of `WriteValue`.
    pub trait Sealed {}
    impl Sealed for char {}
    impl Sealed for i8 {}
    impl Sealed for u8 {}
    impl Sealed for i16 {}
    impl Sealed for u16 {}
    impl Sealed for i32 {}
    impl Sealed for u32 {}
    impl Sealed for f32 {}
    
    impl Sealed for &str {}
    impl<'a> Sealed for super::Hex<'a> {}
    
    impl Sealed for &[i8] {}
    impl Sealed for &[u8] {}
    impl Sealed for &[i16] {}
    impl Sealed for &[u16] {}
    impl Sealed for &[i32] {}
    impl Sealed for &[u32] {}
    impl Sealed for &[f32] {}
}

/// A trait for writing tag values. Implemented for:
/// * `char`, but will return error, if char takes more than one byte,
/// * numeric values `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `f32`,
/// * numeric slices `&[i8]`, `&[u8]`, `&[i16]`, ..., `&[f32]`,
/// * string slice `&str`. You can use `std::str::from_utf8_unchecked` to convert `&[u8]` to `&str`.
/// We use `&str` here to distinguish between string and int array types.
/// * hex wrapper [Hex(&[u8])](struct.Hex.html),
///
/// The trait cannot be implemented for new types.
pub trait WriteValue: private::Sealed {
    /// Returns the number of written bytes.
    fn write<W: Write>(&self, f: &mut W) -> io::Result<usize>;
}

impl WriteValue for char {
    fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
        if self.len_utf8() != 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "Cannot write tag value: char value takes more than one byte"));
        }
        f.write_all(&[b'A', *self as u8])?;
        Ok(2)
    }
}

impl WriteValue for i8 {
    fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
        f.write_all(&[b'c', unsafe { std::mem::transmute::<i8, u8>(*self) }])?;
        Ok(2)
    }
}

impl WriteValue for u8 {
    fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
        f.write_all(&[b'C', *self])?;
        Ok(2)
    }
}

macro_rules! write_value_prim {
    ($name:ty, $letter:expr, $fun:ident) => {
        impl WriteValue for $name {
            fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
                f.write_u8($letter)?;
                f.$fun::<LittleEndian>(*self)?;
                Ok(1 + mem::size_of::<$name>())
            }
        }
    }
}

write_value_prim!(i16, b's', write_i16);
write_value_prim!(u16, b'S', write_u16);
write_value_prim!(i32, b'i', write_i32);
write_value_prim!(u32, b'I', write_u32);
write_value_prim!(f32, b'f', write_f32);

impl WriteValue for &str {
    fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
        f.write_u8(b'Z')?;
        f.write_all(self.as_bytes())?;
        f.write_u8(0)?;
        Ok(2 + self.len())
    }
}

impl<'a> WriteValue for Hex<'a> {
    fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
        if self.0.len() % 2 != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "Cannot write tag value: Hex string contains odd number of bytes"));
        }
        f.write_u8(b'H')?;
        f.write_all(self.0)?;
        f.write_u8(0)?;
        Ok(2 + self.0.len())
    }
}

impl WriteValue for &[i8] {
    fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
        f.write_all(b"Bc")?;
        f.write_i32::<LittleEndian>(self.len() as i32)?;
        unsafe {
            f.write_all(std::slice::from_raw_parts(self.as_ptr() as *const u8, self.len()))?;
        }
        Ok(8 + self.len())
    }
}

impl WriteValue for &[u8] {
    fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
        f.write_all(b"BC")?;
        f.write_i32::<LittleEndian>(self.len() as i32)?;
        f.write_all(self)?;
        Ok(8 + self.len())
    }
}

macro_rules! write_value_array {
    ($name:ty, $letter:expr, $fun:ident) => {
        impl WriteValue for &[$name] {
            fn write<W: Write>(&self, f: &mut W) -> io::Result<usize> {
                f.write_all(&[b'B', $letter])?;
                f.write_i32::<LittleEndian>(self.len() as i32)?;
                for v in self.iter() {
                    f.$fun::<LittleEndian>(*v)?;
                }
                Ok(8 + self.len() * mem::size_of::<$name>())
            }
        }
    }
}

write_value_array!(i16, b's', write_i16);
write_value_array!(u16, b'S', write_u16);
write_value_array!(i32, b'i', write_i32);
write_value_array!(u32, b'I', write_u32);
write_value_array!(f32, b'f', write_f32);


/// Wrapper around raw tags
pub struct TagViewer {
    raw: Vec<u8>,
    lengths: Vec<u32>,
}

/// Get a size of type from letter (c -> 1), (i -> 4). Returns Error for non int/float types
fn tag_type_size(ty: u8) -> Result<u32, Error> {
    match ty {
        b'c' | b'C' | b'A' => Ok(1),
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
                        return Err(Error::Corrupted("Hex tag has odd number of bytes"));
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

/// Alias for a tag name. Equals to `[u8; 2]`.
pub type TagName = [u8; 2];

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

    /// Returns a value of a tag with `name`. Value integer/float types, returns copied value, for
    /// array and string types returns a wrapper over reference. Takes `O(n_tags)`.
    pub fn get<'a>(&'a self, name: &TagName) -> Option<TagValue<'a>> {
        let mut start = 0;
        for &tag_len in self.lengths.iter() {
            let tag_len = tag_len as usize;
            if name == &self.raw[start..start + 2] {
                return Some(TagValue::from_raw(self.raw[start + 3],
                    &self.raw[start + 4..start + tag_len]));
            }
            start += tag_len;
        }
        None
    }

    /// Iterate over tuples `(name, tag_value)`, where `name: [u8; 2]` and `tag_value: TagValue`.
    pub fn iter<'a>(&'a self) -> TagIter<'a> {
        TagIter {
            pos: 0,
            lengths: self.lengths.iter(),
            raw: &self.raw,
        }
    }

    /// Appends a new tag. Trait [WriteValue](trait.WriteValue.html) implemented for possible
    /// values, so you can push tags like this:
    ///
    /// ```rust
    /// tags.push(b"AA", 10)
    /// ```
    ///
    /// The function does not check, if there is a tag with the same name, O(new_tag_len).
    ///
    /// See [WriteValue](trait.WriteValue.html) for more information.
    pub fn push<V: WriteValue>(&mut self, name: &TagName, value: V) -> io::Result<()> {
        self.lengths.push(value.write(&mut self.raw)? as u32);
        Ok(())
    }

    /// Inserts a new tag instead of existing. If there is no tags with the same name, pushes to
    /// the end. Takes O(raw_tags_len + new_tag_len).
    pub fn insert<V: WriteValue>(&mut self, name: &TagName, value: V) -> io::Result<()> {
        let mut start = 0;
        for i in 0..self.lengths.len() {
            let tag_len = self.lengths[i] as usize;
            if name == &self.raw[start..start + 2] {
                let mut new_tag = Vec::new();
                let new_len = value.write(&mut new_tag)? as u32;
                self.raw.splice(start + 4..start + tag_len, new_tag);
                self.lengths[i] = new_len;
                return Ok(());
            }
            start += tag_len;
        }
        self.push(name, value)
    }

    /// Removes a tag, if present. Takes O(raw_tags_len).
    /// Returns `true`, if the element was deleted, and `false` otherwise.
    pub fn remove(&mut self, name: &TagName) -> bool {
        let mut start = 0;
        for i in 0..self.lengths.len() {
            let tag_len = self.lengths[i] as usize;
            if name == &self.raw[start..start + 2] {
                self.raw.drain(start + 4..start + tag_len);
                self.lengths.remove(i);
                return true;
            }
            start += tag_len;
        }
        false
    }
}

/// Iterator over tags.
pub struct TagIter<'a> {
    pos: usize,
    lengths: std::slice::Iter<'a, u32>,
    raw: &'a [u8],
}

impl<'a> Iterator for TagIter<'a> {
    type Item = (TagName, TagValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.lengths.next() {
            Some(tag_len) => {
                let tag_len = *tag_len as usize;
                let start = self.pos;
                self.pos += tag_len;
                let name = [self.raw[start], self.raw[start + 1]];
                Some((name, TagValue::from_raw(self.raw[start + 3],
                        &self.raw[start + 4..start + tag_len])))
            },
            None => None,
        }
    }
}
