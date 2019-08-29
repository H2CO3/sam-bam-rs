extern crate rand;
extern crate bam;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::cmp::min;
use std::time::Instant;

use rand::Rng;
use bam::BamReader;

fn print_records(records1: &Vec<&str>, records2: &Vec<&str>) {
    println!("Records 1:");
    for record in records1.iter() {
        println!("    {}", &record[..min(record.len(), 20)]);
    }

    println!("Records 2:");
    for record in records2.iter() {
        println!("    {}", &record[..min(record.len(), 20)]);
    }
}

fn compare_bam(path: &str) {
    let mut reader = bam::IndexedReader::from_path(path).unwrap();
    let header = reader.header().clone();

    const ITERATIONS: usize = 1000;

    let mut record = bam::Record::new();

    let mut rng = rand::thread_rng();
    for i in 0..ITERATIONS {
        let ref_id = rng.gen_range(0, header.n_references());
        let length = header.reference_len(ref_id).unwrap();
        let start = rng.gen_range(0, length);
        let end = rng.gen_range(start + 1, length + 1);

        let mut output1 = Vec::new();
        let mut count = 0;
        println!("Iteration {}", i);
        let ref_name = header.reference_name(ref_id).unwrap();
        println!("Fetching {}:{}-{}", ref_name, start + 1, end);

        let timer = Instant::now();
        let mut viewer = reader.fetch(ref_id as i32, start, end);
        loop {
            match viewer.read_into(&mut record) {
                Ok(()) => {},
                Err(bam::Error::NoMoreRecords) => break,
                Err(e) => panic!(e),
            }
            count += 1;
            record.write_sam(&mut output1, &header).unwrap();
        }
        println!("    bam::IndexedReader: {:?}", timer.elapsed());
        output1.pop();
        let output1 = std::str::from_utf8(&output1).unwrap();

        let timer = Instant::now();
        let samtools_output = Command::new("samtools")
            .arg("view")
            .arg(path)
            .arg(format!("{}:{}-{}", ref_name, start + 1, end))
            .output()
            .expect("failed to execute process");
        println!("    samtools view:      {:?}", timer.elapsed());
        println!("    total {} records", count);
        let mut output2 = samtools_output.stdout;
        output2.pop();
        let output2 = std::str::from_utf8(&output2).unwrap();

        let mut records1: Vec<_> = output1.split('\n').collect();
        let mut records2: Vec<_> = output2.split('\n').collect();
        records1.sort();
        records2.sort();
        if records1.len() != records2.len() {
            print_records(&records1, &records2);
            assert_eq!(records1.len(), records2.len());
        }
        for i in 0..records1.len() {
            if records1[i] != records2[i] {
                print_records(&records1, &records2);
                assert_eq!(records1[i], records2[i]);
            }
        }
    }
}

#[test]
fn compare_with_samtools() {
    let data = Path::new("tests/data");
    assert!(data.exists() && data.is_dir(), "Cannot load test data");
    for entry in fs::read_dir(data).unwrap() {
        let entry = entry.unwrap();
        let bam_path = entry.path();
        let bai_path = PathBuf::from(format!("{}.bai", bam_path.display()));
        let bam_str = bam_path.to_str().unwrap();
        if bam_path.is_file() && bam_str.ends_with(".bam") && bai_path.exists() {
            println!("Analyzing {}", bam_str);
            compare_bam(bam_str);
        }
    }
}