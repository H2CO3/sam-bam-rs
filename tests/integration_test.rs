extern crate rand;
extern crate bam;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use rand::Rng;
use bam::RecordReader;

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
        let mut viewer = reader.fetch(ref_id as u32, start, end).unwrap();
        loop {
            match viewer.read_into(&mut record) {
                Ok(()) => {},
                Err(bam::Error::NoMoreRecords) => break,
                Err(e) => panic!("{}", e),
            }
            count += 1;
            record.write_sam(&mut output1, &header).unwrap();
        }
        println!("    bam::IndexedReader: {:?}", timer.elapsed());

        let timer = Instant::now();
        let samtools_output = Command::new("samtools")
            .arg("view")
            .arg(path)
            .arg(format!("{}:{}-{}", ref_name, start + 1, end))
            .output()
            .expect("failed to execute process");
        println!("    samtools view:      {:?}", timer.elapsed());
        println!("    total {} records", count);
        let output2 = samtools_output.stdout;

        if output1 != output2 {
            println!("Crate output:");
            println!("{}\n", std::string::String::from_utf8_lossy(&output1));
            println!("Samtools view:");
            println!("{}\n", std::string::String::from_utf8_lossy(&output2));
            panic!("Outputs are different for {}:{}-{}", ref_name, start + 1, end);
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