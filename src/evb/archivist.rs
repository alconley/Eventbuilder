use std::path::PathBuf;
use std::process::Command;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct SSH {
    pub enabled: bool,
    pub user: String,
    pub host: String,
    pub password: String,
}

impl Default for SSH {
    fn default() -> Self {
        Self {
            enabled: false,
            user: "spieker-group".to_string(),
            host: "spiekerlab.physics.fsu.edu".to_string(),
            password: "$PIEKER#_group".to_string(),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Default)]
pub struct Archivist {
    pub compass_path: String,
    pub output_path: String,
    pub min_run: u32,
    pub max_run: u32,
    pub multiple_runs: bool,
    pub ssh: SSH,
}

impl Archivist {
    pub fn run_archive(&self, run_no: u32) {
        let output_path = PathBuf::from(&self.output_path);
        if !output_path.exists() {
            log::error!("Output path {:?} does not exist", output_path);
            return;
        }

        let archive_path = format!("{}/run_{}.tar.gz", self.output_path, run_no);
        let archive_path = PathBuf::from(&archive_path);

        let binary_dir = format!("{}/DAQ/run_{}/UNFILTERED", self.compass_path, run_no);
        let binary_dir = PathBuf::from(&binary_dir);
        if !binary_dir.exists() {
            log::error!("Binary data directory {:?} does not exist", binary_dir);
            return;
        }

        println!(
            "Running archivist for binary data in {:?} to archive {:?}...",
            binary_dir, archive_path
        );

        let output = Command::new("bash")
            .arg("-c")
            .arg(format!(
                "cd {:?} && tar -cvzf {:?} ./*.BIN && cd -",
                binary_dir.display(),
                archive_path.display()
            ))
            .output();

        if let Err(e) = output {
            log::error!("Failed to run command: {}", e);
            return;
        }

        let output = output.unwrap();
        if !output.status.success() {
            log::error!(
                "Error during archiving: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            // print the stdout
            println!("{}", String::from_utf8_lossy(&output.stdout));
        }
    }

    // pub fn run_archive_ssh(&self, run_no: u32) {
    //     let password = self.ssh.password.clone();
    //     let user = self.ssh.user.clone();
    //     let host = self.ssh.host.clone();
    // }

    pub fn ui(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new("Archivist")
                    .color(egui::Color32::LIGHT_BLUE)
                    .size(18.0),
            );
            ui.checkbox(&mut self.ssh.enabled, "SSH enabled");
        });

        ui.horizontal(|ui| {
            if self.ssh.enabled {
                ui.horizontal(|ui| {
                    ui.label("SSH:");
                    ui.add(egui::TextEdit::singleline(&mut self.ssh.user).hint_text("User"));
                });
                ui.label("@");
                ui.add(egui::TextEdit::singleline(&mut self.ssh.host).hint_text("Host"));
                ui.label(":");
            }
        });

        if self.ssh.enabled {
            ui.add(
                egui::TextEdit::singleline(&mut self.ssh.password)
                    .hint_text("SSH password")
                    .password(true),
            );
        }

        ui.horizontal(|ui| {
            ui.label("Compass DAQ Folder");
            ui.add(egui::TextEdit::singleline(&mut self.compass_path).clip_text(false));
            ui.label("/DAQ/run_#/UNFLITERED/*.BIN");
        });

        ui.horizontal(|ui| {
            ui.label("Local Output Folder");
            ui.add(egui::TextEdit::singleline(&mut self.output_path).clip_text(false));
        });

        ui.horizontal(|ui| {
            ui.add(egui::DragValue::new(&mut self.min_run).prefix("Min run: "));
            ui.add_enabled(
                self.multiple_runs,
                egui::DragValue::new(&mut self.max_run).prefix("Max run: "),
            );
            ui.checkbox(&mut self.multiple_runs, "Multiple runs");
        });

        if ui.button("Archive").clicked() {
            if self.multiple_runs {
                for run in self.min_run..=self.max_run {
                    self.run_archive(run);
                }
            } else {
                self.run_archive(self.min_run);
            }
            // if self.ssh.enabled {
            //     if self.multiple_runs {
            //         for run_no in self.min_run..=self.max_run {
            //             if let Err(e) = self.run_archive_ssh(run_no) {
            //                 eprintln!("Error archiving run {}: {}", run_no, e);
            //             }
            //         }
            //     } else {
            //         if let Err(e) = self.run_archive_ssh(self.min_run) {
            //             eprintln!("Error archiving run {}: {}", self.min_run, e);
            //         }
            //     }
            // } else {
            //     if self.multiple_runs {
            //         for run_no in self.min_run..=self.max_run {
            //             if let Err(e) = self.run_archive(run_no) {
            //                 eprintln!("Error archiving run {}: {}", run_no, e);
            //             }
            //         }
            //     } else {
            //         if let Err(e) = self.run_archive(self.min_run) {
            //             eprintln!("Error archiving run {}: {}", self.min_run, e);
            //         }
            //     }
            // }
        }
    }
}
