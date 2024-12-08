use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Write},
    path::Path,
};

use clap::Parser;
use csv::Writer;
use glam::{ivec3, IVec3, Vec3};
use half::f16;
use log::{error, info, LevelFilter};
use vdb_rs::VdbReader;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    source_directory: String,
    #[arg(short, long)]
    output_directory: String,
    #[arg(short, long)]
    recursive: bool,
    #[arg(short, long)]
    multithreading: bool,
}

fn parse_vdb_file(source_path: &Path, output_path: &Path) {
    // Make sure the output dir exists
    match output_path.parent() {
        Some(source_path) => match std::fs::create_dir_all(source_path) {
            Ok(_) => (),
            Err(err) => {
                error!("{}", err);
                return;
            }
        },
        None => {
            error!("{:?} does not have a parent directory", output_path);
            return;
        }
    }

    // Find grid names
    let grid_names = {
        let vdb_file = File::open(source_path).unwrap();
        let vdb_reader = VdbReader::new(BufReader::new(&vdb_file)).unwrap();
        vdb_reader.available_grids()
    };

    // Setup structure to collect voxel values into
    type VoxelT = f16;
    for (i, name) in grid_names.iter().enumerate() {
        // open reader
        let vdb_file = File::open(source_path).unwrap();
        let reader = BufReader::new(&vdb_file);
        let mut vdb_reader = VdbReader::new(reader).unwrap();

        // open writer
        let csv_filename = output_path.with_extension(name.clone() + ".csv");
        info!("{:?} => {:?}", source_path, csv_filename);
        let mut wtr = match Writer::from_path(&csv_filename) {
            Ok(writer) => writer,
            Err(err) => {
                error!("{}", err);
                let alternative_csv_filename = output_path.with_extension(i.to_string() + ".csv");
                error!(
                    "Grid with name: {} could not create output file with name {:?}. Resorting to file name: {:?}",
                    name, csv_filename, alternative_csv_filename
                );
                Writer::from_path(&alternative_csv_filename).unwrap()
            }
        };
        wtr.write_record(vec!["x", "y", "z", "span", "value"])
            .unwrap();

        // Read specific grid
        let grid = vdb_reader.read_grid::<VoxelT>(name).unwrap();
        for (position, voxel, level) in grid.iter() {
            let position = ivec3(position.x as i32, position.y as i32, position.z as i32);
            wtr.write_record(vec![
                position.x.to_string(),
                position.y.to_string(),
                position.z.to_string(),
                (level.scale() as i32).to_string(),
                voxel.to_string(),
            ])
            .unwrap();
        }
    }
}

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .filter_module("vdb_rs::reader", LevelFilter::Error)
        .init();
    let args = Args::parse();

    info!("Reading from: {}", args.source_directory);
    info!("Exporting to: {}", args.output_directory);

    let source_dir = Path::new(&args.source_directory);
    let output_dir = Path::new(&args.output_directory);

    let walker = if !args.recursive {
        WalkDir::new(source_dir).max_depth(1)
    } else {
        WalkDir::new(source_dir)
    };

    rayon::scope(|scope| {
        for entry in walker {
            match entry {
                Ok(entry) => {
                    if let Some(file_with_extension) = entry.path().extension() {
                        if file_with_extension == "vdb" {
                            if args.multithreading {
                                scope.spawn(move |_| {
                                    let entry = entry.clone();

                                    // get source dir local path
                                    let local_pos = match entry.path().strip_prefix(&source_dir) {
                                        Ok(local) => local,
                                        Err(err) => {
                                            error!("{}", err);
                                            return;
                                        }
                                    };

                                    // get output dir global path
                                    let source_path = entry.path();
                                    let output_path = output_dir.join(local_pos);

                                    parse_vdb_file(source_path, &output_path);
                                });
                            } else {
                                let entry = entry.clone();

                                // get source dir local path
                                let local_pos = match entry.path().strip_prefix(&source_dir) {
                                    Ok(local) => local,
                                    Err(err) => {
                                        error!("{}", err);
                                        return;
                                    }
                                };

                                // get output dir global path
                                let source_path = entry.path();
                                let output_path = output_dir.join(local_pos);

                                parse_vdb_file(source_path, &output_path);
                            }
                        }
                    }
                }
                Err(err) => error!("{}", err),
            }
        }
    });
}
