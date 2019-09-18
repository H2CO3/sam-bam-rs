extern crate rand;
extern crate bam;

use std::fs::File;
use std::process::Command;
use std::time::Instant;
use std::io::Read;

use rand::Rng;
use glob::glob;

use bam::{RecordReader, RecordWriter};

fn test_indexed_reader(path: &str) {
    let mut reader = bam::IndexedReader::from_path(path).unwrap();
    let header = reader.header().clone();

    const ITERATIONS: usize = 10;
    let mut rng = rand::thread_rng();
    let mut record = bam::Record::new();

    let output = format!("{}.test_indexed_reader", path);
    for i in 0..ITERATIONS {
        let ref_id = rng.gen_range(0, header.n_references());
        let length = header.reference_len(ref_id).unwrap();
        let start = rng.gen_range(0, length);
        let end = rng.gen_range(start + 1, length + 1);

        let mut count = 0;
        let ref_name = header.reference_name(ref_id).unwrap();
        println!("    Iteration {}", i);
        println!("    Fetching {}:{}-{}", ref_name, start + 1, end);

        let timer = Instant::now();
        {
            let mut sam_writer = bam::SamWriter::from_path(&output, header.clone()).unwrap();
            let mut viewer = reader.fetch(ref_id as u32, start, end).unwrap();
            loop {
                match viewer.read_into(&mut record) {
                    Ok(()) => {},
                    Err(bam::Error::NoMoreRecords) => break,
                    Err(e) => panic!("{}", e),
                }
                count += 1;
                sam_writer.write(&record).unwrap();
            }
        }
        println!("        bam::IndexedReader: {:?}", timer.elapsed());
        let mut output1 = Vec::new();
        File::open(&output).unwrap().read_to_end(&mut output1).unwrap();

        let timer = Instant::now();
        let samtools_output = Command::new("samtools")
            .args(&["view", "-h"])
            .arg(path)
            .arg(format!("{}:{}-{}", ref_name, start + 1, end))
            .output()
            .expect("failed to execute process");
        println!("        samtools view:      {:?}", timer.elapsed());
        println!("        total {} records", count);
        assert!(samtools_output.status.success());
        let output2 = samtools_output.stdout;

        if output1 != output2 {
            println!("bam::IndexedReader output:");
            println!("{}\n", std::string::String::from_utf8_lossy(&output1));
            println!("Samtools view:");
            println!("{}\n", std::string::String::from_utf8_lossy(&output2));
            panic!("Outputs are different for {}:{}-{}", ref_name, start + 1, end);
        }
    }
}

fn test_bam_reader(path: &str) {
    let mut reader = bam::BamReader::from_path(path).unwrap();

    let mut record = bam::Record::new();
    let mut count = 0;

    let output = format!("{}.test_bam_reader", path);
    let timer = Instant::now();
    {
        let mut sam_writer = bam::SamWriter::from_path(&output, reader.header().clone()).unwrap();
        loop {
            match reader.read_into(&mut record) {
                Ok(()) => {},
                Err(bam::Error::NoMoreRecords) => break,
                Err(e) => panic!("{}", e),
            }
            count += 1;
            sam_writer.write(&record).unwrap();
        }
    }
    println!("        bam::BamReader: {:?}", timer.elapsed());
    let mut output1 = Vec::new();
    File::open(&output).unwrap().read_to_end(&mut output1).unwrap();

    let timer = Instant::now();
    let samtools_output = Command::new("samtools")
        .args(&["view", "-h"])
        .arg(path)
        .output()
        .expect("failed to execute process");
    println!("        samtools view:  {:?}", timer.elapsed());
    println!("        total {} records", count);
    assert!(samtools_output.status.success());
    let output2 = samtools_output.stdout;

    if output1 != output2 {
        println!("bam::BamReader output:");
        println!("{}\n", std::string::String::from_utf8_lossy(&output1));
        println!("Samtools view:");
        println!("{}\n", std::string::String::from_utf8_lossy(&output2));
        panic!("Outputs are different");
    }
}

fn test_bam_writer(path: &str) {
    let mut reader = bam::BamReader::from_path(path).unwrap();

    let mut record = bam::Record::new();
    let mut count = 0;

    let output = format!("{}.test_bam_writer", path);
    let timer = Instant::now();
    {
        let mut bam_writer = bam::BamWriter::from_path(&output, reader.header().clone()).unwrap();
        loop {
            match reader.read_into(&mut record) {
                Ok(()) => {},
                Err(bam::Error::NoMoreRecords) => break,
                Err(e) => panic!("{}", e),
            }
            count += 1;
            bam_writer.write(&record).unwrap();
        }
    }
    println!("        bam::BamReader: {:?}", timer.elapsed());
    let samtools_output1 = Command::new("samtools")
        .args(&["view", "-h"])
        .arg(&output)
        .output()
        .expect("failed to execute process");
    assert!(samtools_output1.status.success());
    let output1 = samtools_output1.stdout;

    let timer = Instant::now();
    let samtools_output2 = Command::new("samtools")
        .args(&["view", "-h"])
        .arg(path)
        .output()
        .expect("failed to execute process");
    println!("        samtools view:  {:?}", timer.elapsed());
    println!("        total {} records", count);
    assert!(samtools_output2.status.success());
    let output2 = samtools_output2.stdout;

    if output1 != output2 {
        println!("bam::BamWriter output:");
        println!("{}\n", std::string::String::from_utf8_lossy(&output1));
        println!("Samtools view:");
        println!("{}\n", std::string::String::from_utf8_lossy(&output2));
        panic!("Outputs are different");
    }
}

fn test_sam_to_bam(path: &str) {
    let mut reader = bam::SamReader::from_path(path).unwrap();
    let mut record = bam::Record::new();

    let output = format!("{}.test_sam_to_bam", path);
    let timer = Instant::now();
    {
        let mut bam_writer = bam::BamWriter::from_path(&output, reader.header().clone()).unwrap();
        loop {
            match reader.read_into(&mut record) {
                Ok(()) => {},
                Err(bam::Error::NoMoreRecords) => break,
                Err(e) => panic!("{}", e),
            }
            bam_writer.write(&record).unwrap();
        }
    }
    println!("        bam::BamReader: {:?}", timer.elapsed());
    let samtools_output1 = Command::new("samtools")
        .args(&["view", "-h"])
        .arg(&output)
        .output()
        .expect("failed to execute process");
    assert!(samtools_output1.status.success());
    let output1 = samtools_output1.stdout;

    let mut output2 = Vec::new();
    File::open(&path).unwrap().read_to_end(&mut output2).unwrap();

    if output1 != output2 {
        println!("bam::BamWriter output:");
        println!("{}\n", std::string::String::from_utf8_lossy(&output1));
        println!("Input file:");
        println!("{}\n", std::string::String::from_utf8_lossy(&output2));
        panic!("Outputs are different");
    }
}

#[test]
fn indexed_reader() {
    for entry in glob("tests/data/iread*.bam").unwrap() {
        let entry = entry.unwrap();
        println!("Analyzing {}", entry.display());
        let entry_str = entry.as_os_str().to_str().unwrap();
        test_indexed_reader(entry_str);
    }
}

#[test]
fn bam_reader() {
    for entry in glob("tests/data/read*.bam").unwrap() {
        let entry = entry.unwrap();
        println!("Analyzing {}", entry.display());
        let entry_str = entry.as_os_str().to_str().unwrap();
        test_bam_reader(entry_str);
    }
}

#[test]
fn bam_writer() {
    for entry in glob("tests/data/read*.bam").unwrap() {
        let entry = entry.unwrap();
        println!("Analyzing {}", entry.display());
        let entry_str = entry.as_os_str().to_str().unwrap();
        test_bam_writer(entry_str);
    }
}

#[test]
fn sam_to_bam() {
    for entry in glob("tests/data/read*.sam").unwrap() {
        let entry = entry.unwrap();
        println!("Analyzing {}", entry.display());
        let entry_str = entry.as_os_str().to_str().unwrap();
        test_sam_to_bam(entry_str);
    }
}