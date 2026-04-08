use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use flate2::read::GzDecoder;
use log::info;
use polars::io::parquet::write::BatchedWriter;
use polars::prelude::*;
use tar::Archive;

use super::channel_data::ChannelData;
use super::channel_map::{Board, ChannelMap};
use super::compass_file::CompassFile;
use super::error::EVBError;
use super::event_builder::EventBuilder;
use super::kinematics::{calculate_weights, KineParameters};
use super::nuclear_data::MassMap;
use super::scaler_list::{ScalerEntryUI, ScalerList};
use super::shapira_fp::FocalPlaneTilt;
use super::shift_map::{ShiftMap, ShiftMapEntry};
use super::used_size::UsedSize;

// Default in-memory buffer size before flushing a parquet row group.
pub const DEFAULT_FLUSH_BUFFER_SIZE_MB: u64 = 8_000;

fn flush_buffer_size_bytes(flush_buffer_size_mb: u64) -> usize {
    flush_buffer_size_mb
        .max(1)
        .saturating_mul(1_000_000)
        .try_into()
        .unwrap_or(usize::MAX)
}

fn check_cancel(cancel_requested: &AtomicBool) -> Result<(), EVBError> {
    if cancel_requested.load(Ordering::Relaxed) {
        Err(EVBError::Cancelled)
    } else {
        Ok(())
    }
}

#[derive(Debug)]
struct RunParams<'a> {
    pub run_archive_path: PathBuf,
    pub unpack_dir_path: PathBuf,
    pub output_file_path: PathBuf,
    pub scalerlist: Vec<ScalerEntryUI>,
    pub scalerout_file_path: PathBuf,
    pub nuc_map: &'a MassMap,
    pub focal_plane_tilt: &'a FocalPlaneTilt, // JCE 2026
    pub channel_map: &'a ChannelMap,
    pub shift_map: &'a Option<ShiftMap>,
    pub coincidence_window: f64,
    pub flush_buffer_size_mb: u64,
    pub run_number: i32,
}

fn clean_up_unpack_dir(unpack_dir: &Path) -> Result<(), EVBError> {
    for entry in unpack_dir.read_dir()?.flatten() {
        if entry.metadata()?.is_file() {
            std::fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn channel_data_to_dataframe(data: ChannelData) -> Result<DataFrame, PolarsError> {
    DataFrame::new(data.convert_to_columns())
}

struct RunParquetWriter {
    path: PathBuf,
    temp_path: PathBuf,
    writer: Option<BatchedWriter<File>>,
}

impl RunParquetWriter {
    fn new(path: PathBuf) -> Self {
        let temp_path = temporary_parquet_path(&path);
        Self {
            path,
            temp_path,
            writer: None,
        }
    }

    fn ensure_writer(&mut self, schema: &Schema) -> Result<(), EVBError> {
        if self.writer.is_none() {
            info!("Writing dataframe to disk at {}", self.path.display());
            let output_file = File::create(&self.temp_path)?;
            let writer = ParquetWriter::new(output_file).batched(schema)?;
            self.writer = Some(writer);
        }

        Ok(())
    }

    fn write_batch(&mut self, data: ChannelData) -> Result<(), EVBError> {
        let df = channel_data_to_dataframe(data)?;
        self.ensure_writer(df.schema())?;

        if df.height() > 0 {
            self.writer.as_mut().unwrap().write_batch(&df)?;
        }

        Ok(())
    }

    fn finish(&mut self, data: ChannelData) -> Result<(), EVBError> {
        let df = channel_data_to_dataframe(data)?;
        self.ensure_writer(df.schema())?;

        if df.height() > 0 {
            self.writer.as_mut().unwrap().write_batch(&df)?;
        }

        if let Some(writer) = self.writer.as_ref() {
            writer.finish()?;
        }
        self.writer = None;

        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        std::fs::rename(&self.temp_path, &self.path)?;
        Ok(())
    }

    fn abort(&mut self) -> Result<(), EVBError> {
        self.writer = None;
        if self.temp_path.exists() {
            std::fs::remove_file(&self.temp_path)?;
        }
        Ok(())
    }
}

fn temporary_parquet_path(path: &Path) -> PathBuf {
    let mut file_name = path
        .file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("output.parquet"));
    file_name.push(".partial");
    path.with_file_name(file_name)
}

fn process_run(
    params: RunParams<'_>,
    k_params: &KineParameters,
    progress: Arc<Mutex<f32>>,
    cancel_requested: Arc<AtomicBool>,
) -> Result<(), EVBError> {
    let mut parquet_writer = RunParquetWriter::new(params.output_file_path.clone());
    let mut files: Vec<CompassFile<'_>> = vec![];

    let result = (|| {
        check_cancel(cancel_requested.as_ref())?;

        // Protective, ensure no loose files
        clean_up_unpack_dir(&params.unpack_dir_path)?;

        check_cancel(cancel_requested.as_ref())?;
        let archive_file = File::open(&params.run_archive_path)?;
        let mut decompressed_archive = Archive::new(GzDecoder::new(archive_file));
        decompressed_archive.unpack(&params.unpack_dir_path)?;

        check_cancel(cancel_requested.as_ref())?;
        let mut scaler_list = Some(ScalerList::new(params.scalerlist));

        // Collect all files from unpack, separate scalers from normal files
        let mut total_count: u64 = 0;
        for item in params.unpack_dir_path.read_dir()? {
            check_cancel(cancel_requested.as_ref())?;

            let filepath = &item?.path();
            if let Some(list) = &mut scaler_list {
                if list.read_scaler(filepath) {
                    continue;
                }
            };

            files.push(CompassFile::new(filepath, params.shift_map)?);
            files.last_mut().unwrap().set_hit_used();
            files.last_mut().unwrap().get_top_hit()?;
            total_count += files.last().unwrap().get_number_of_hits();
        }

        let mut evb = EventBuilder::new(&params.coincidence_window);
        let mut analyzed_data = ChannelData::new(params.channel_map);
        // calculating kinematic weights for Xavg and Xshap
        let x_weights = calculate_weights(k_params, params.nuc_map);
        let xshap_params = &params.focal_plane_tilt; // JCE 12/2025

        let mut earliest_file_index: Option<usize>;

        let mut count: u64 = 0;
        let mut flush_count: u64 = 0;
        let flush_percent = 0.01;
        let flush_val: u64 = ((total_count as f64) * flush_percent) as u64;
        let flush_buffer_size_bytes = flush_buffer_size_bytes(params.flush_buffer_size_mb);

        loop {
            check_cancel(cancel_requested.as_ref())?;

            // Bulk of the work ... look for the earliest hit in the file collection
            earliest_file_index = Option::None;
            for i in 0..files.len() {
                if !files[i].is_eof() {
                    let hit = files[i].get_top_hit()?;
                    if hit.is_default() {
                        continue;
                    }

                    earliest_file_index = match earliest_file_index {
                        None => Some(i),
                        Some(index) => {
                            if hit.timestamp < files[index].get_top_hit()?.timestamp {
                                Some(i)
                            } else {
                                Some(index)
                            }
                        }
                    };
                }
            }

            match earliest_file_index {
                None => break, // This is how we exit, no more hits to be found
                Some(i) => {
                    // else we pop the earliest hit off to the event builder
                    let hit = files[i].get_top_hit()?;
                    evb.push_hit(hit);
                    files[i].set_hit_used();
                }
            }

            if evb.is_event_ready() {
                analyzed_data.append_event(
                    evb.get_ready_event(),
                    params.channel_map,
                    x_weights,
                    xshap_params,
                ); // JCE 12/2025

                // Flush buffered rows into a single parquet file before memory spikes.
                if analyzed_data.get_used_size() > flush_buffer_size_bytes {
                    parquet_writer.write_batch(analyzed_data)?;
                    analyzed_data = ChannelData::new(params.channel_map);
                }
            }

            // Progress report
            count += 1;
            if count == flush_val.max(1) {
                flush_count += 1;
                count = 0;

                match progress.lock() {
                    Ok(mut prog) => *prog = (flush_count as f64 * flush_percent) as f32,
                    Err(_) => return Err(EVBError::Sync),
                };
            }
        }

        check_cancel(cancel_requested.as_ref())?;
        parquet_writer.finish(analyzed_data)?;
        println!("\tWriting run {}", params.run_number);

        if let Some(list) = scaler_list {
            list.write_scalers(&params.scalerout_file_path)?
        }

        Ok(())
    })();

    // To be safe, manually drop all files in unpack dir before deleting all the files
    drop(files);
    let cleanup_result = clean_up_unpack_dir(&params.unpack_dir_path);

    match result {
        Ok(()) => {
            cleanup_result?;
            Ok(())
        }
        Err(err) => {
            parquet_writer.abort()?;
            let _ = cleanup_result;
            Err(err)
        }
    }
}

pub struct ProcessParams {
    pub archive_dir: PathBuf,
    pub unpack_dir: PathBuf,
    pub output_dir: PathBuf,
    pub focal_plane_tilt: FocalPlaneTilt, // JCE 2026
    pub channel_map: Vec<Board>,
    pub scaler_list: Vec<ScalerEntryUI>,
    pub shift_map: Vec<ShiftMapEntry>,
    pub coincidence_window: f64,
    pub flush_buffer_size_mb: u64,
    pub run_min: i32,
    pub run_max: i32,
}

//Function which handles processing multiple runs, this is what the UI actually calls
pub fn process_runs(
    params: ProcessParams,
    k_params: KineParameters,
    progress: Arc<Mutex<f32>>,
    current_run: Arc<Mutex<Option<i32>>>,
    cancel_requested: Arc<AtomicBool>,
) -> Result<(), EVBError> {
    let channel_map = ChannelMap::new(&params.channel_map);
    let mass_map = MassMap::new()?;
    let shift_map = ShiftMap::new(params.shift_map);

    let result = (|| {
        println!(
            "Processing runs {} to {}",
            params.run_min,
            params.run_max - 1
        );
        for run in params.run_min..params.run_max {
            check_cancel(cancel_requested.as_ref())?;

            match current_run.lock() {
                Ok(mut current) => *current = Some(run),
                Err(_) => return Err(EVBError::Sync),
            };

            let local_params = RunParams {
                run_archive_path: params.archive_dir.join(format!("run_{}.tar.gz", run)),
                unpack_dir_path: params.unpack_dir.clone(),
                output_file_path: params.output_dir.join(format!("run_{}.parquet", run)),
                scalerlist: params.scaler_list.clone(),
                // scalerout_file_path: params.output_dir.join(format!("run_{}_scalers.txt", run)),
                scalerout_file_path: params
                    .output_dir
                    .parent() // Navigate one level up from output_dir
                    .unwrap() // Safely unwrap, assuming output_dir has a parent
                    .join("scalers") // Append the "scalers" directory
                    .join(format!("run_{}_scalers.txt", run)),
                nuc_map: &mass_map,
                focal_plane_tilt: &params.focal_plane_tilt, // JCE 2026
                channel_map: &channel_map,
                shift_map: &Some(shift_map.clone()),
                coincidence_window: params.coincidence_window,
                flush_buffer_size_mb: params.flush_buffer_size_mb,
                run_number: run,
            };

            match progress.lock() {
                Ok(mut prog) => *prog = 0.0,
                Err(_) => return Err(EVBError::Sync),
            };

            //Skip over run if it doesnt exist
            if local_params.run_archive_path.exists() {
                process_run(
                    local_params,
                    &k_params,
                    progress.clone(),
                    cancel_requested.clone(),
                )?;
            }
        }

        println!("Processing complete\n");
        Ok(())
    })();

    match current_run.lock() {
        Ok(mut current) => *current = None,
        Err(_) => return Err(EVBError::Sync),
    };

    result
}
