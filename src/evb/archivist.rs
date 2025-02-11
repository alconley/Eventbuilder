use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(serde::Deserialize, serde::Serialize, Debug, Clone)]
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
            password: "".to_string(),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug, Default, Clone)]
pub struct Archivist {
    pub compass_path: String,
    pub output_path: String,
    pub min_run: u32,
    pub max_run: u32,
    pub multiple_runs: bool,
    pub ssh: SSH,
    #[serde(skip)]
    pub is_archiving: Arc<AtomicBool>,
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

    pub fn run_archive_ssh(&self, run_no: u32) {
        if !self.ssh.enabled {
            log::error!("SSH is not enabled in the configuration.");
            return;
        }

        let archive_path = format!("{}/run_{}.tar.gz", self.output_path, run_no);
        let binary_dir = format!("{}/DAQ/run_{}/UNFILTERED", self.compass_path, run_no);

        println!(
            "Running archivist over SSH for binary data in {:?} to create archive {:?}...",
            binary_dir, archive_path
        );

        // Step 1: Tar the files on the remote server
        let remote_archive_path = format!("/tmp/run_{}.tar.gz", run_no);
        let ssh_tar_command = format!(
            "sshpass -p '{}' ssh {}@{} \"cd {} && tar -cvzf {} ./*.BIN\"",
            self.ssh.password, self.ssh.user, self.ssh.host, binary_dir, remote_archive_path
        );

        let tar_output = Command::new("bash")
            .arg("-c")
            .arg(&ssh_tar_command)
            .output();

        if let Err(e) = tar_output {
            log::error!("Failed to run SSH tar command: {}", e);
            return;
        }

        let tar_output = tar_output.unwrap();
        if !tar_output.status.success() {
            log::error!(
                "Error during remote tar creation: {}",
                String::from_utf8_lossy(&tar_output.stderr)
            );
            return;
        }

        println!(
            "\tRemote tarball created successfully at {:?}.",
            remote_archive_path
        );

        // Step 2: Copy the tarball from the remote server to the local machine
        let scp_command = format!(
            "sshpass -p '{}' scp {}@{}:{} {}",
            self.ssh.password, self.ssh.user, self.ssh.host, remote_archive_path, archive_path
        );

        let scp_output = Command::new("bash").arg("-c").arg(&scp_command).output();

        if let Err(e) = scp_output {
            log::error!("Failed to run SCP command: {}", e);
            return;
        }

        let scp_output = scp_output.unwrap();
        if !scp_output.status.success() {
            log::error!(
                "Error during SCP: {}",
                String::from_utf8_lossy(&scp_output.stderr)
            );
            return;
        }

        println!(
            "\tTarball successfully copied to local path {:?}.",
            archive_path
        );

        // Step 3: Optionally clean up the remote tarball
        let ssh_cleanup_command = format!(
            "sshpass -p '{}' ssh {}@{} \"rm -f {}\"",
            self.ssh.password, self.ssh.user, self.ssh.host, remote_archive_path
        );

        let cleanup_output = Command::new("bash")
            .arg("-c")
            .arg(&ssh_cleanup_command)
            .output();

        if let Err(e) = cleanup_output {
            log::warn!("Failed to clean up remote tarball: {}", e);
        } else {
            let cleanup_output = cleanup_output.unwrap();
            if !cleanup_output.status.success() {
                log::warn!(
                    "Error during remote cleanup: {}",
                    String::from_utf8_lossy(&cleanup_output.stderr)
                );
            } else {
                println!("\tRemote tarball cleaned up successfully.");
            }
        }
    }

    fn archive(&self) {
        if self.ssh.enabled {
            if self.multiple_runs {
                for run_no in self.min_run..=self.max_run {
                    self.run_archive_ssh(run_no);
                }
            } else {
                self.run_archive_ssh(self.min_run);
            }
        } else if self.multiple_runs {
            for run_no in self.min_run..=self.max_run {
                self.run_archive(run_no);
            }
        } else {
            self.run_archive(self.min_run);
        }

        println!("\nArchiving completed.\n");
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

        // if ui.button("Archive").clicked() {
        //     self.archive();
        // }

        ui.horizontal(|ui| {
            if ui
                .add_enabled(
                    !self.is_archiving.load(Ordering::Relaxed),
                    egui::Button::new("Archive"),
                )
                .clicked()
            {
                let archivist_clone = self.clone();
                let is_archiving = Arc::clone(&self.is_archiving);

                is_archiving.store(true, Ordering::Relaxed);

                std::thread::spawn(move || {
                    archivist_clone.archive();
                    is_archiving.store(false, Ordering::Relaxed);
                });
            }

            if self.is_archiving.load(Ordering::Relaxed) {
                ui.label("Archiving...");
                ui.add(egui::Spinner::new());
            }
        });
    }
}
