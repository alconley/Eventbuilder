// shapira_fp.rs
// Tilted focal-plane reconstruction according Shapira et al. (1985).  
// https://doi.org/10.1016/0029-554X(75)90121-4
// JCE 12/2025

use serde::{Deserialize, Serialize};
use crate::evb::kinematics::SPS_DETECTOR_WIRE_DIST;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FocalPlaneTilt {
    pub alpha_deg: f64, // degrees
    pub h: f64,        // unitless until H_mm
    pub s: f64,         // mm
    
}

impl Default for FocalPlaneTilt {
    fn default() -> Self {
        Self {
            
            // dummy Shapira parameters
            alpha_deg: 0.0001,
            h: 1.0000,
            s: SPS_DETECTOR_WIRE_DIST * 10.0, // cm → mm

        }
    }
}

pub fn calculate_xshap(
    x1: f64,
    x2: f64,
    params: &FocalPlaneTilt,
) -> Option<f64> {
    let dx = x2 - x1;

    let alpha = params.alpha_deg.to_radians();
    let tan_a = alpha.tan();
    if tan_a.abs() < 1e-12 {
        // α ≈ 0 → untilted focal plane limit
        let h_mm = params.h * params.s;
        return Some((x2 * params.s - dx * h_mm) / params.s);
    }
    let cot_a = 1.0 / tan_a;

    let norm_tan = (1.0 + tan_a * tan_a).sqrt();
    let norm_cot = (1.0 + cot_a * cot_a).sqrt();

    // define h_mm, numerator, denominator
    let h_mm = params.h * params.s;
    let numerator =
        x2 * (params.s / norm_tan) - (dx * h_mm);

    let denominator =
        (params.s / norm_tan) - (dx / norm_cot);

    if denominator.abs() < 1e-12 {
        None
    } else {
        Some(numerator / denominator)
    }
}
