#[cfg(not(target_arch = "wasm32"))]
pub mod app;
#[cfg(not(target_arch = "wasm32"))]
pub mod ws;

#[cfg(target_arch = "wasm32")]
pub mod app_web;
