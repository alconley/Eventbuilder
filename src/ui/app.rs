use log::{error, info};
use serde::{Deserialize, Serialize};

use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use eframe::egui::{self, Color32, RichText};
use eframe::App;

use super::ws::{Workspace, WorkspaceError};
use crate::evb::archivist::Archivist;
use crate::evb::channel_map::Board;
use crate::evb::compass_run::{process_runs, ProcessParams};
use crate::evb::error::EVBError;
use crate::evb::kinematics::KineParameters;
use crate::evb::nuclear_data::MassMap;
use crate::evb::scaler_list::ScalerEntryUI;
use crate::evb::shift_map::ShiftMapEntry;

#[derive(Debug, Serialize, Deserialize)]
struct EvbAppParams {
    pub workspace: Option<Workspace>,
    pub kinematics: KineParameters,
    pub coincidence_window: f64,
    pub run_min: i32,
    pub run_max: i32,
    pub channel_map_entries: Vec<Board>,
    pub shift_map_entries: Vec<ShiftMapEntry>,
    pub scaler_list_entries: Vec<ScalerEntryUI>,
    pub multiple_runs: bool,
}

impl Default for EvbAppParams {
    fn default() -> Self {
        EvbAppParams {
            workspace: None,
            kinematics: KineParameters::default(),
            coincidence_window: 3.0e3,
            run_min: 0,
            run_max: 0,
            channel_map_entries: Vec::new(),
            shift_map_entries: Vec::new(),
            scaler_list_entries: Vec::new(),
            multiple_runs: false,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum ActiveTab {
    Archivist,
    MainTab,
    Kinematics,
    ChannelMap,
    ShiftMap,
    ScalerList,
}

impl Default for ActiveTab {
    fn default() -> Self {
        Self::MainTab
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Default)]
pub struct EVBApp {
    #[serde(skip)]
    progress: Arc<Mutex<f32>>,

    parameters: EvbAppParams,
    archivist: Archivist,
    rxn_eqn: String,
    active_tab: ActiveTab,

    mass_map: MassMap,

    #[serde(skip)]
    thread_handle: Option<JoinHandle<Result<(), EVBError>>>,

    window: bool,
}

impl EVBApp {
    pub fn new(cc: &eframe::CreationContext<'_>, window: bool) -> Self {
        if let Some(storage) = cc.storage {
            return eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default();
        }

        #[cfg(not(target_arch = "wasm32"))]
        EVBApp {
            progress: Arc::new(Mutex::new(0.0)),
            parameters: EvbAppParams::default(),
            archivist: Archivist::default(),
            active_tab: ActiveTab::MainTab,
            rxn_eqn: String::from("None"),
            mass_map: MassMap::new().expect("Could not open amdc data, shutting down!"),
            thread_handle: None,
            window,
        }
    }

    fn check_and_startup_processing_thread(&mut self) -> Result<(), WorkspaceError> {
        if !self.parameters.multiple_runs {
            self.parameters.run_max = self.parameters.run_min;
        }

        if self.thread_handle.is_none()
            && self.parameters.workspace.is_some()
            && !self.parameters.channel_map_entries.is_empty()
        {
            let prog = self.progress.clone();
            let r_params = ProcessParams {
                archive_dir: self
                    .parameters
                    .workspace
                    .as_ref()
                    .unwrap()
                    .get_archive_dir()?,
                unpack_dir: self
                    .parameters
                    .workspace
                    .as_ref()
                    .unwrap()
                    .get_unpack_dir()?,
                output_dir: self
                    .parameters
                    .workspace
                    .as_ref()
                    .unwrap()
                    .get_output_dir()?,
                channel_map: self.parameters.channel_map_entries.clone(),
                scaler_list: self.parameters.scaler_list_entries.clone(),
                shift_map: self.parameters.shift_map_entries.clone(),
                coincidence_window: self.parameters.coincidence_window,
                run_min: self.parameters.run_min,
                run_max: self.parameters.run_max + 1, //Make it [run_min, run_max]
            };

            match self.progress.lock() {
                Ok(mut x) => *x = 0.0,
                Err(_) => error!("Could not aquire lock at starting processor..."),
            };
            let k_params = self.parameters.kinematics.clone();
            self.thread_handle = Some(std::thread::spawn(|| {
                process_runs(r_params, k_params, prog)
            }));
        } else {
            error!("Cannot run event builder without all filepaths specified");
        }
        Ok(())
    }

    fn check_and_shutdown_processing_thread(&mut self) {
        if self.thread_handle.is_some() && self.thread_handle.as_ref().unwrap().is_finished() {
            match self.thread_handle.take().unwrap().join() {
                Ok(result) => {
                    match result {
                        Ok(_) => info!("Finished processing the run"),
                        Err(x) => {
                            error!("An error occured while processing the run: {x}. Job stopped.")
                        }
                    };
                }
                Err(_) => error!("An error occured in joining the processing thread!"),
            };
        }
    }

    fn write_params_to_file(&self, path: &Path) {
        if let Ok(mut config) = File::create(path) {
            match serde_yaml::to_string(&self.parameters) {
                Ok(yaml_str) => match config.write(yaml_str.as_bytes()) {
                    Ok(_) => (),
                    Err(x) => error!("Error writing config to file{}: {}", path.display(), x),
                },
                Err(x) => error!(
                    "Unable to write configuration to file, serializer error: {}",
                    x
                ),
            };
        } else {
            error!("Could not open file {} for config write", path.display());
        }
    }

    fn read_params_from_file(&mut self, path: &Path) {
        let yaml_str = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(x) => {
                error!(
                    "Unable to open and read config file {} with error {}",
                    path.display(),
                    x
                );
                return;
            }
        };

        match serde_yaml::from_str::<EvbAppParams>(&yaml_str) {
            Ok(params) => self.parameters = params,
            Err(x) => error!(
                "Unable to write configuration to file, serializer error: {}",
                x
            ),
        };
    }

    fn main_tab_ui(&mut self, ui: &mut egui::Ui) {
        //Files/Workspace
        ui.horizontal(|ui| {
            ui.label(
                RichText::new("Run Information")
                    .color(Color32::LIGHT_BLUE)
                    .size(18.0),
            );

            if ui.button("Open").clicked() {
                let result = rfd::FileDialog::new()
                    .set_directory(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
                    .pick_folder();

                if let Some(real_path) = result {
                    self.parameters.workspace = match Workspace::new(&real_path) {
                        Ok(ws) => Some(ws),
                        Err(e) => {
                            eprintln!("Error creating workspace: {}", e);
                            None
                        }
                    }
                }
            }
        });

        ui.horizontal_wrapped(|ui| {
            ui.label("Current Folder: ");
            ui.label(match &self.parameters.workspace {
                Some(ws) => ws.get_parent_str(),
                None => "None",
            });
        });

        egui::Grid::new("RunGrid").show(ui, |ui| {
            ui.label("Coincidence Window");
            ui.add(
                egui::widgets::DragValue::new(&mut self.parameters.coincidence_window)
                    .speed(100)
                    .custom_formatter(|n, _| format!("{:e}", n))
                    .suffix(" ns"),
            );
            ui.end_row();

            ui.checkbox(&mut self.parameters.multiple_runs, "Multiple Runs");
            ui.label("Run Range:");
            ui.end_row();

            ui.label("");
            ui.add(egui::widgets::DragValue::new(&mut self.parameters.run_min).speed(1));
            ui.add_enabled(
                self.parameters.multiple_runs,
                egui::widgets::DragValue::new(&mut self.parameters.run_max).speed(1),
            );
            ui.end_row();

            if self.parameters.run_max < self.parameters.run_min {
                self.parameters.run_max = self.parameters.run_min;
            } else if self.parameters.run_min > self.parameters.run_max {
                self.parameters.run_min = self.parameters.run_max;
            } else if !self.parameters.multiple_runs {
                self.parameters.run_max = self.parameters.run_min;
            }
        });
    }

    fn kinematics_ui(&mut self, ui: &mut egui::Ui) {
        ui.label(
            RichText::new("Kinematics")
                .color(Color32::LIGHT_BLUE)
                .size(18.0),
        );

        self.parameters.kinematics.ui(ui);

        ui.separator();

        ui.horizontal(|ui| {
            ui.label("Reaction:");
            ui.label(&self.rxn_eqn);
            if ui.button("View").clicked() {
                self.rxn_eqn = self.parameters.kinematics.generate_rxn_eqn(&self.mass_map);
            }
        });

        // if mass map is empty, load it
        // need this for persistence to work with the mass map for the set kinematics button
        if self.mass_map.is_empty() {
            match MassMap::new() {
                Ok(mass_map) => self.mass_map = mass_map,
                Err(e) => error!("Error loading mass map: {}", e),
            }
        }
    }

    fn channel_map_ui(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(
                RichText::new("Channel Map")
                    .color(Color32::LIGHT_BLUE)
                    .size(18.0),
            );

            if ui.button("+").clicked() {
                let id = self.parameters.channel_map_entries.len() as u32;
                self.parameters.channel_map_entries.push(Board {
                    id,
                    ..Default::default()
                }); // This line seems correct, assuming boards is a Vec<Board>
            }

            ui.separator();

            ui.label("Default Boards:");

            if ui.button("SE-SPS").clicked() {
                let id = self.parameters.channel_map_entries.len() as u32;
                let board = Board::sps(id);
                self.parameters.channel_map_entries.push(board);
            }

            if ui.button("CeBrA").clicked() {
                let id = self.parameters.channel_map_entries.len() as u32;
                let board = Board::cebra(id);
                self.parameters.channel_map_entries.push(board);
            }
        });

        ui.add_space(1.0);

        // egui::ScrollArea::both().show(ui, |ui| {
        ui.horizontal(|ui| {
            let mut remove_indices = vec![];
            for (board_idx, board) in self.parameters.channel_map_entries.iter_mut().enumerate() {
                board.ui(ui, board_idx, || {
                    remove_indices.push(board_idx);
                });
                ui.separator();
                ui.add_space(1.0);
            }

            for &idx in remove_indices.iter().rev() {
                self.parameters.channel_map_entries.remove(idx);
            }
        });
        // });
    }

    fn shift_map_ui(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(
                RichText::new("Time Shift Map")
                    .color(Color32::LIGHT_BLUE)
                    .size(18.0),
            );

            // Assuming `self.shift_map_entries` is a Vec<ShiftMapEntry>
            if ui.button("+").clicked() {
                // Add a new entry with default values
                self.parameters.shift_map_entries.push(ShiftMapEntry {
                    board_number: 0,
                    channel_number: 0,
                    time_shift: 0.0,
                });
            }
        });

        let mut remove_indices = vec![];
        for (index, entry) in self.parameters.shift_map_entries.iter_mut().enumerate() {
            entry.ui(ui, || {
                remove_indices.push(index);
            });
        }

        // Remove entries marked for removal
        // Iterate in reverse to ensure indices remain valid after removals
        for &index in remove_indices.iter().rev() {
            self.parameters.shift_map_entries.remove(index);
        }
    }

    fn scaler_list_ui(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(
                RichText::new("Scalar List")
                    .color(Color32::LIGHT_BLUE)
                    .size(18.0),
            );

            if ui.button("+").clicked() {
                // Add a new entry with default values
                self.parameters.scaler_list_entries.push(ScalerEntryUI {
                    file_pattern: "".to_string(),
                    scaler_name: "".to_string(),
                });
            }
        });

        // Use a `ScrollArea` to ensure the UI can handle many entries
        // egui::ScrollArea::horizontal().show(ui, |ui| {
        let mut to_remove = Vec::new(); // Indices of entries to remove
        for (index, entry) in self.parameters.scaler_list_entries.iter_mut().enumerate() {
            entry.ui(ui, || {
                to_remove.push(index);
            });
        }

        // Remove entries marked for removal, in reverse order to maintain correct indices
        for &index in to_remove.iter().rev() {
            self.parameters.scaler_list_entries.remove(index);
        }
        // });
    }

    fn ui_tabs(&mut self, ui: &mut egui::Ui) {
        egui::TopBottomPanel::top("cebra_sps_top_panel").show_inside(ui, |ui| {
            ui.horizontal(|ui| {
                if ui
                    .selectable_label(matches!(self.active_tab, ActiveTab::Archivist), "Archivist")
                    .clicked()
                {
                    self.active_tab = ActiveTab::Archivist;
                }
                if ui
                    .selectable_label(
                        matches!(self.active_tab, ActiveTab::MainTab),
                        "Eventbuilder",
                    )
                    .clicked()
                {
                    self.active_tab = ActiveTab::MainTab;
                }
                if ui
                    .selectable_label(
                        matches!(self.active_tab, ActiveTab::Kinematics),
                        "Kinematics",
                    )
                    .clicked()
                {
                    self.active_tab = ActiveTab::Kinematics;
                }
                if ui
                    .selectable_label(
                        matches!(self.active_tab, ActiveTab::ChannelMap),
                        "Channel Map",
                    )
                    .clicked()
                {
                    self.active_tab = ActiveTab::ChannelMap;
                }
                if ui
                    .selectable_label(matches!(self.active_tab, ActiveTab::ShiftMap), "Shift Map")
                    .clicked()
                {
                    self.active_tab = ActiveTab::ShiftMap;
                }
                if ui
                    .selectable_label(
                        matches!(self.active_tab, ActiveTab::ScalerList),
                        "Scaler List",
                    )
                    .clicked()
                {
                    self.active_tab = ActiveTab::ScalerList;
                }
            });
        });

        egui::ScrollArea::both().show(ui, |ui| match self.active_tab {
            ActiveTab::Archivist => self.archivist.ui(ui),
            ActiveTab::MainTab => self.main_tab_ui(ui),
            ActiveTab::Kinematics => self.kinematics_ui(ui),
            ActiveTab::ChannelMap => self.channel_map_ui(ui),
            ActiveTab::ShiftMap => self.shift_map_ui(ui),
            ActiveTab::ScalerList => self.scaler_list_ui(ui),
        });
    }

    fn progress_ui(&mut self, ui: &mut egui::Ui) {
        if self.active_tab == ActiveTab::Archivist {
            return;
        }
        egui::TopBottomPanel::bottom("cebra_sps_bottom_panel").show_inside(ui, |ui| {
            ui.add(
                egui::widgets::ProgressBar::new(match self.progress.lock() {
                    Ok(x) => *x,
                    Err(_) => 0.0,
                })
                .show_percentage(),
            );

            // Check if the thread handle exists to determine if the process is running
            let is_running = self.thread_handle.is_some();
            if is_running {
                ui.add(egui::Spinner::new());
            }

            if ui
                .add_enabled(
                    self.thread_handle.is_none(),
                    egui::widgets::Button::new("Run"),
                )
                .clicked()
            {
                info!("Starting processor...");
                match self.check_and_startup_processing_thread() {
                    Ok(_) => (),
                    Err(e) => error!(
                        "Could not start processor, recieved the following error: {}",
                        e
                    ),
                };
            } else {
                self.check_and_shutdown_processing_thread();
            }
        });
    }

    fn ui(&mut self, ui: &mut egui::Ui) {
        self.progress_ui(ui);

        ui.menu_button("File", |ui| {
            if ui.button("Open Config...").clicked() {
                let result = rfd::FileDialog::new()
                    .set_directory(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
                    .add_filter("YAML file", &["yaml"])
                    .pick_file();

                if let Some(real_path) = result {
                    self.read_params_from_file(&real_path)
                }
            }
            if ui.button("Save Config...").clicked() {
                let result = rfd::FileDialog::new()
                    .set_directory(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
                    .add_filter("YAML file", &["yaml"])
                    .save_file();

                if let Some(real_path) = result {
                    self.write_params_to_file(&real_path)
                }
            }
        });

        ui.separator();

        self.ui_tabs(ui);
    }
}

impl App for EVBApp {
    /// Called by the frame work to save state before shutdown.
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, eframe::APP_KEY, self);
    }

    fn update(&mut self, ctx: &eframe::egui::Context, _frame: &mut eframe::Frame) {
        if self.window {
            egui::Window::new("CeBrA - SE-SPS Event Builder")
                .min_width(200.0)
                .max_width(600.0)
                .show(ctx, |ui| {
                    self.ui(ui);
                });
        } else {
            egui::CentralPanel::default().show(ctx, |ui| {
                self.ui(ui);
            });
        }
    }
}
