#![warn(clippy::all, rust_2018_idioms)]
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use eventbuilder::ui::app::EVBApp;

fn main() -> eframe::Result<()> {
    env_logger::init(); // Log to stderr (if you run with `RUST_LOG=debug`).

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([425.0, 250.0])
            .with_min_inner_size([425.0, 250.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Eventbuilder",
        native_options,
        Box::new(|cc| Ok(Box::new(EVBApp::new(cc, false)))),
    )
}
