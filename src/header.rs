use std::io::{Write, Result};

use byteorder::WriteBytesExt;

pub type TagName = [u8; 2];

/// A single tag in a line.
#[derive(Clone)]
pub struct Tag {
    name: TagName,
    value: String,
}

impl Tag {
    /// Creates a new tag.
    pub fn new(name: &TagName, value: String) -> Tag {
        Tag {
            name: name.clone(),
            value
        }
    }

    pub fn name(&self) -> &TagName {
        &self.name
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    /// Sets a new value and returns an old one.
    pub fn set_value(&mut self, new_value: String) -> String {
        std::mem::replace(&mut self.value, new_value)
    }

    /// Consumes tag and returns value.
    pub fn take_value(self) -> String {
        self.value
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.name)?;
        writer.write_u8(b':')?;
        writer.write_all(self.value.as_bytes())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EntryType {
    HeaderLine,
    RefSequence,
    ReadGroup,
    Program,
}

impl EntryType {
    /// Returns record two-letter name of the type (such as HD or SQ).
    pub fn name(self) -> &'static TagName {
        use EntryType::*;
        match self {
            HeaderLine => b"HD",
            RefSequence => b"SQ",
            ReadGroup => b"RG",
            Program => b"PG",
        }
    }
}

/// A single header line.
///
/// You can create a new entry using [new_header_line](#method.new_header_line),
/// [new_ref_sequence](#method.new_ref_sequence) and so on. After that you can modify line tags
/// using [push](#method.push), [remove](#method.remove) and others.
///
/// However, be careful not to delete the required tag, as well as check the required tag format.
#[derive(Clone)]
pub struct HeaderEntry {
    tags: Vec<Tag>,
    entry_type: EntryType,
}

impl HeaderEntry {
    /// Creates a new @HD header entry.
    pub fn new_header_line(version: String) -> HeaderEntry {
        let mut res = HeaderEntry {
            tags: Vec::new(),
            entry_type: EntryType::HeaderLine,
        };
        res.push(b"VN", version);
        res
    }

    /// Creates a new @SQ header entry.
    pub fn new_ref_sequence(seq_name: String, seq_len: u32) -> HeaderEntry {
        let mut res = HeaderEntry {
            tags: Vec::new(),
            entry_type: EntryType::RefSequence,
        };
        res.push(b"SN", seq_name);
        res.push(b"LN", seq_len.to_string());
        res
    }

    /// Creates a new @RG header entry.
    pub fn new_read_group(ident: String) -> HeaderEntry {
        let mut res = HeaderEntry {
            tags: Vec::new(),
            entry_type: EntryType::ReadGroup,
        };
        res.push(b"ID", ident);
        res
    }

    /// Creates a new @PG header entry.
    pub fn new_program(ident: String) -> HeaderEntry {
        let mut res = HeaderEntry {
            tags: Vec::new(),
            entry_type: EntryType::Program,
        };
        res.push(b"ID", ident);
        res
    }

    pub fn entry_type(&self) -> EntryType {
        self.entry_type
    }

    /// Returns record two-letter name of the line (such as HD or SQ).
    pub fn entry_name(&self) -> &'static TagName {
        self.entry_type.name()
    }

    /// Returns all tags in a line.
    // fn tags(&self) -> &[Tag];
    // /// Returns mutable tags in a line.
    // fn tags_mut(&mut self) -> &mut Vec<Tag>;

    /// Returns a tag with `name` if present. Takes `O(n_tags)`.
    pub fn get(&self, name: &TagName) -> Option<&str> {
        for tag in self.tags.iter() {
            if &tag.name == name {
                return Some(&tag.value);
            }
        }
        None
    }

    /// Returns a mutable tag with `name` if present. Takes `O(n_tags)`.
    pub fn get_mut(&mut self, name: &TagName) -> Option<&mut str> {
        for tag in self.tags.iter_mut() {
            if &tag.name == name {
                return Some(&mut tag.value);
            }
        }
        None
    }

    /// Pushes a tag to the end. Takes `O(1)`.
    pub fn push(&mut self, name: &TagName, value: String) -> &mut Self {
        self.tags.push(Tag::new(name, value));
        self
    }

    /// Replaces tag value if present and returns previous value. If there is no tag
    /// with the same name, pushes the new tag to the end. Takes `O(n_tags)`.
    pub fn insert(&mut self, name: &TagName, value: String) -> Option<String> {
        for tag in self.tags.iter_mut() {
            if &tag.name == name {
                return Some(tag.set_value(value));
            }
        }
        self.tags.push(Tag::new(name, value));
        None
    }

    /// Removes the tag, if present, and returns its value. Takes `O(n_tags)`.
    pub fn remove(&mut self, name: &TagName) -> Option<String> {
        for i in 0..self.tags.len() {
            if &self.tags[i].name == name {
                return Some(self.tags.remove(i).take_value());
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Write the whole record in a line.
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u8(b'@')?;
        writer.write_all(self.entry_name())?;
        for tag in self.tags.iter() {
            writer.write_u8(b'\t')?;
            tag.write(writer)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub enum HeaderLine {
    Entry(HeaderEntry),
    Comment(String),
}

/// A single header line. Can be a [HeaderEntry](struct.HeaderEntry.html) or a string comment.
///
/// You cannot remove lines from the header, but you can create a new header and a subset of
/// lines there.
#[derive(Clone)]
pub struct Header {
    lines: Vec<HeaderLine>,
    ref_names: Vec<String>,
    ref_lengths: Vec<u32>,
}

impl Header {
    /// Creates an empty header.
    pub fn new() -> Header {
        Header {
            lines: Vec::new(),
            ref_names: Vec::new(),
            ref_lengths: Vec::new(),
        }
    }

    /// Iterator over lines.
    pub fn lines(&self) -> std::slice::Iter<HeaderLine> {
        self.lines.iter()
    }

    /// Pushes a new header entry.
    pub fn push_entry(&mut self, entry: HeaderEntry) {
        if entry.entry_type() == EntryType::RefSequence {
            let name = entry.get(b"SN")
                .expect("@SQ header entry does not have a SN tag").to_string();
            let len: u32 = entry.get(b"LN")
                .expect("@SQ header entry does not have a LN tag")
                .parse()
                .expect("@SQ header entry has a non-integer LN tag");
            assert!(len > 1, "Reference length must be positive");
            self.ref_names.push(name);
            self.ref_lengths.push(len);
        }
        self.lines.push(HeaderLine::Entry(entry));
    }

    /// Pushes a new comment.
    pub fn push_comment(&mut self, comment: String) {
        self.lines.push(HeaderLine::Comment(comment));
    }

    pub fn write_text<W: Write>(&self, writer: &mut W) -> Result<()> {
        for line in self.lines.iter() {
            match line {
                HeaderLine::Entry(entry) => entry.write(writer)?,
                HeaderLine::Comment(comment) => {
                    writer.write_all(b"@CO\t")?;
                    writer.write_all(comment.as_bytes())?;
                },
            }
        }
        Ok(())
    }
}
