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
            password: String::new(),
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
    pub fn run_archive(&self, run_no: u32) -> Result<(), String> {
        let binary_dir = format!("{}/run_{}/UNFILTERED", self.compass_path, run_no);
        let archive_path = format!("{}/run_{}.tar.gz", self.output_path, run_no);
    
        // Convert paths to PathBuf for proper handling
        let binary_dir = PathBuf::from(&binary_dir);
        let archive_path = PathBuf::from(&archive_path);
    
        // Check if binary_dir exists
        if !binary_dir.exists() {
            return Err(format!("Binary data directory {:?} does not exist", binary_dir));
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
    
        match output {
            Ok(output) => {
                if output.status.success() {
                    println!("Archive complete.");
                    Ok(())
                } else {
                    Err(format!(
                        "Error during archiving: {}",
                        String::from_utf8_lossy(&output.stderr)
                    ))
                }
            }
            Err(e) => Err(format!("Failed to run command: {}", e)),
        }
    }

    pub fn run_archive_ssh(&self, run_no: u32) -> Result<(), String> {
        if !self.ssh.enabled {
            return Err("SSH is not enabled.".to_string());
        }

        let remote_binary_dir = format!("{}/run_{}/UNFILTERED", self.compass_path, run_no);
        let local_archive_path = format!("{}/run_{}.tar.gz", self.output_path, run_no);

        // Ensure the local archive path is valid
        let local_archive_path = PathBuf::from(&local_archive_path);

        println!(
            "Archiving files from remote: {}@{}:{} to local: {:?}",
            self.ssh.user, self.ssh.host, remote_binary_dir, local_archive_path
        );

        // SSH command to tar files on the remote server
        let tar_command = format!(
            "sshpass -p {} {}@{} cd {}; tar -czf - ./*.BIN'",
            self.ssh.password, self.ssh.user, self.ssh.host, remote_binary_dir
        );

        // SCP command to send the tarball locally
        let scp_command = format!(
            "{} > {:?}",
            tar_command,
            local_archive_path.display()
        );

        // Execute the tar and transfer command
        let output = Command::new("bash")
            .arg("-c")
            .arg(&scp_command)
            .output();

        match output {
            Ok(output) => {
                if output.status.success() {
                    println!("Archive successfully transferred to local machine.");
                    Ok(())
                } else {
                    Err(format!(
                        "Error during SSH tar/scp operation: {}",
                        String::from_utf8_lossy(&output.stderr)
                    ))
                }
            }
            Err(e) => Err(format!("Failed to execute SSH tar/scp command: {}", e)),
        }
    }

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
            ui.add(egui::TextEdit::singleline(&mut self.ssh.password).hint_text("SSH password").password(true));
        }

        ui.horizontal(|ui| {
            ui.label("Compass DAQ Folder");
            ui.add(
                egui::TextEdit::singleline(&mut self.compass_path)
                    .clip_text(false),
            );
        });


        ui.horizontal(|ui| {
            ui.label("Local Output Folder");
            ui.add(
                egui::TextEdit::singleline(&mut self.output_path)
                    .clip_text(false),
            );    
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
            if self.ssh.enabled {
                if self.multiple_runs {
                    for run_no in self.min_run..=self.max_run {
                        if let Err(e) = self.run_archive_ssh(run_no) {
                            eprintln!("Error archiving run {}: {}", run_no, e);
                        }
                    }
                } else {
                    if let Err(e) = self.run_archive_ssh(self.min_run) {
                        eprintln!("Error archiving run {}: {}", self.min_run, e);
                    }
                }
            } else {
                if self.multiple_runs {
                    for run_no in self.min_run..=self.max_run {
                        if let Err(e) = self.run_archive(run_no) {
                            eprintln!("Error archiving run {}: {}", run_no, e);
                        }
                    }
                } else {
                    if let Err(e) = self.run_archive(self.min_run) {
                        eprintln!("Error archiving run {}: {}", self.min_run, e);
                    }
                }
            }
        }
    }
}
