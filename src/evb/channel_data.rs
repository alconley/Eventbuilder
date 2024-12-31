use super::channel_map::{ChannelMap, ChannelType};
#[allow(unused_imports)]
use super::compass_data::{decompose_uuid_to_board_channel, CompassData};
use super::used_size::UsedSize;
use std::hash::Hash;
use std::{collections::BTreeMap, vec};

use strum::IntoEnumIterator;
use strum_macros::{AsRefStr, EnumCount, EnumIter};

use polars::prelude::*;

const INVALID_VALUE: f64 = -1.0e6;

#[derive(Debug, Clone, Hash, Eq, PartialOrd, Ord, PartialEq, EnumIter, EnumCount, AsRefStr)]
pub enum ChannelDataField {
    AnodeFrontEnergy,
    AnodeFrontShort,
    AnodeFrontTime,
    AnodeBackEnergy,
    AnodeBackShort,
    AnodeBackTime,
    ScintLeftEnergy,
    ScintLeftShort,
    ScintLeftTime,
    ScintRightEnergy,
    ScintRightShort,
    ScintRightTime,
    CathodeEnergy,
    CathodeShort,
    CathodeTime,
    DelayFrontLeftEnergy,
    DelayFrontLeftShort,
    DelayFrontLeftTime,
    DelayFrontRightEnergy,
    DelayFrontRightShort,
    DelayFrontRightTime,
    DelayBackLeftEnergy,
    DelayBackLeftShort,
    DelayBackLeftTime,
    DelayBackRightEnergy,
    DelayBackRightShort,
    DelayBackRightTime,
    MonitorEnergy,
    MonitorShort,
    MonitorTime,
    X1,
    X2,
    Xavg,
    Theta,
    X,
    Z,

    Cebra0Energy,
    Cebra1Energy,
    Cebra2Energy,
    Cebra3Energy,
    Cebra4Energy,
    Cebra5Energy,
    Cebra6Energy,
    Cebra7Energy,
    Cebra8Energy,

    Cebra0Short,
    Cebra1Short,
    Cebra2Short,
    Cebra3Short,
    Cebra4Short,
    Cebra5Short,
    Cebra6Short,
    Cebra7Short,
    Cebra8Short,

    Cebra0Time,
    Cebra1Time,
    Cebra2Time,
    Cebra3Time,
    Cebra4Time,
    Cebra5Time,
    Cebra6Time,
    Cebra7Time,
    Cebra8Time,

    Cebra0RelTime,
    Cebra1RelTime,
    Cebra2RelTime,
    Cebra3RelTime,
    Cebra4RelTime,
    Cebra5RelTime,
    Cebra6RelTime,
    Cebra7RelTime,
    Cebra8RelTime,

    PIPS1000Energy,
    PIPS500Energy,
    PIPS300Energy,
    PIPS100Energy,

    PIPS1000Short,
    PIPS500Short,
    PIPS300Short,
    PIPS100Short,

    PIPS1000Time,
    PIPS500Time,
    PIPS300Time,
    PIPS100Time,

    CATRINA0Energy,
    CATRINA1Energy,
    CATRINA2Energy,

    CATRINA0Short,
    CATRINA1Short,
    CATRINA2Short,

    CATRINA0Time,
    CATRINA1Time,
    CATRINA2Time,

    CATRINA0PSD,
    CATRINA1PSD,
    CATRINA2PSD,
}

impl ChannelDataField {
    //Returns a list of fields for iterating over
    pub fn get_field_vec() -> Vec<ChannelDataField> {
        ChannelDataField::iter().collect()
    }

    pub fn get_filtered_field_vec(channel_map: &ChannelMap) -> Vec<ChannelDataField> {
        let all_delay_lines_present = channel_map
            .contains_channel_type(ChannelType::DelayFrontLeft)
            && channel_map.contains_channel_type(ChannelType::DelayFrontRight)
            && channel_map.contains_channel_type(ChannelType::DelayBackLeft)
            && channel_map.contains_channel_type(ChannelType::DelayBackRight);
        ChannelDataField::iter()
            .filter(|field| {
                match field {
                    // Include additional fields only if all delay line channels are present
                    ChannelDataField::X1
                    | ChannelDataField::X2
                    | ChannelDataField::Xavg
                    | ChannelDataField::X
                    | ChannelDataField::Z
                    | ChannelDataField::Theta => all_delay_lines_present,
                    // Filter other fields based on the channel map
                    ChannelDataField::AnodeFrontEnergy
                    | ChannelDataField::AnodeFrontShort
                    | ChannelDataField::AnodeFrontTime => {
                        channel_map.contains_channel_type(ChannelType::AnodeFront)
                    }
                    ChannelDataField::AnodeBackEnergy
                    | ChannelDataField::AnodeBackShort
                    | ChannelDataField::AnodeBackTime => {
                        channel_map.contains_channel_type(ChannelType::AnodeBack)
                    }
                    ChannelDataField::ScintLeftEnergy
                    | ChannelDataField::ScintLeftShort
                    | ChannelDataField::ScintLeftTime => {
                        channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }
                    ChannelDataField::ScintRightEnergy
                    | ChannelDataField::ScintRightShort
                    | ChannelDataField::ScintRightTime => {
                        channel_map.contains_channel_type(ChannelType::ScintRight)
                    }
                    ChannelDataField::CathodeEnergy
                    | ChannelDataField::CathodeShort
                    | ChannelDataField::CathodeTime => {
                        channel_map.contains_channel_type(ChannelType::Cathode)
                    }
                    ChannelDataField::DelayFrontLeftEnergy
                    | ChannelDataField::DelayFrontLeftShort
                    | ChannelDataField::DelayFrontLeftTime => {
                        channel_map.contains_channel_type(ChannelType::DelayFrontLeft)
                    }
                    ChannelDataField::DelayFrontRightEnergy
                    | ChannelDataField::DelayFrontRightShort
                    | ChannelDataField::DelayFrontRightTime => {
                        channel_map.contains_channel_type(ChannelType::DelayFrontRight)
                    }
                    ChannelDataField::DelayBackLeftEnergy
                    | ChannelDataField::DelayBackLeftShort
                    | ChannelDataField::DelayBackLeftTime => {
                        channel_map.contains_channel_type(ChannelType::DelayBackLeft)
                    }
                    ChannelDataField::DelayBackRightEnergy
                    | ChannelDataField::DelayBackRightShort
                    | ChannelDataField::DelayBackRightTime => {
                        channel_map.contains_channel_type(ChannelType::DelayBackRight)
                    }
                    ChannelDataField::MonitorEnergy
                    | ChannelDataField::MonitorShort
                    | ChannelDataField::MonitorTime => {
                        channel_map.contains_channel_type(ChannelType::Monitor)
                    }

                    ChannelDataField::Cebra0Energy
                    | ChannelDataField::Cebra0Short
                    | ChannelDataField::Cebra0Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra0)
                    }
                    ChannelDataField::Cebra0RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra0)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }

                    ChannelDataField::Cebra1Energy
                    | ChannelDataField::Cebra1Short
                    | ChannelDataField::Cebra1Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra1)
                    }
                    ChannelDataField::Cebra1RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra1)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }

                    ChannelDataField::Cebra2Energy
                    | ChannelDataField::Cebra2Short
                    | ChannelDataField::Cebra2Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra2)
                    }
                    ChannelDataField::Cebra2RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra2)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }

                    ChannelDataField::Cebra3Energy
                    | ChannelDataField::Cebra3Short
                    | ChannelDataField::Cebra3Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra3)
                    }
                    ChannelDataField::Cebra3RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra3)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }
                    ChannelDataField::Cebra4Energy
                    | ChannelDataField::Cebra4Short
                    | ChannelDataField::Cebra4Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra4)
                    }
                    ChannelDataField::Cebra4RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra4)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }
                    ChannelDataField::Cebra5Energy
                    | ChannelDataField::Cebra5Short
                    | ChannelDataField::Cebra5Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra5)
                    }
                    ChannelDataField::Cebra5RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra5)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }
                    ChannelDataField::Cebra6Energy
                    | ChannelDataField::Cebra6Short
                    | ChannelDataField::Cebra6Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra6)
                    }
                    ChannelDataField::Cebra6RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra6)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }
                    ChannelDataField::Cebra7Energy
                    | ChannelDataField::Cebra7Short
                    | ChannelDataField::Cebra7Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra7)
                    }
                    ChannelDataField::Cebra7RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra7)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }
                    ChannelDataField::Cebra8Energy
                    | ChannelDataField::Cebra8Short
                    | ChannelDataField::Cebra8Time => {
                        channel_map.contains_channel_type(ChannelType::Cebra8)
                    }
                    ChannelDataField::Cebra8RelTime => {
                        channel_map.contains_channel_type(ChannelType::Cebra8)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }

                    ChannelDataField::PIPS1000Energy
                    | ChannelDataField::PIPS1000Short
                    | ChannelDataField::PIPS1000Time => {
                        channel_map.contains_channel_type(ChannelType::PIPS1000)
                    }

                    ChannelDataField::PIPS500Energy
                    | ChannelDataField::PIPS500Short
                    | ChannelDataField::PIPS500Time => {
                        channel_map.contains_channel_type(ChannelType::PIPS500)
                    }

                    ChannelDataField::PIPS300Energy
                    | ChannelDataField::PIPS300Short
                    | ChannelDataField::PIPS300Time => {
                        channel_map.contains_channel_type(ChannelType::PIPS300)
                    }

                    ChannelDataField::PIPS100Energy
                    | ChannelDataField::PIPS100Short
                    | ChannelDataField::PIPS100Time => {
                        channel_map.contains_channel_type(ChannelType::PIPS100)
                    }

                    ChannelDataField::CATRINA0Energy
                    | ChannelDataField::CATRINA0Short
                    | ChannelDataField::CATRINA0Time
                    | ChannelDataField::CATRINA0PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA0)
                    }

                    ChannelDataField::CATRINA1Energy
                    | ChannelDataField::CATRINA1Short
                    | ChannelDataField::CATRINA1Time
                    | ChannelDataField::CATRINA1PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA1)
                    }

                    ChannelDataField::CATRINA2Energy
                    | ChannelDataField::CATRINA2Short
                    | ChannelDataField::CATRINA2Time
                    | ChannelDataField::CATRINA2PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA2)
                    }
                }
            })
            .collect()
    }
}

impl UsedSize for ChannelDataField {
    fn get_used_size(&self) -> usize {
        std::mem::size_of::<ChannelDataField>()
    }
}

#[derive(Debug, Clone)]
pub struct ChannelData {
    //Columns must always come in same order, so use sorted map
    pub fields: BTreeMap<ChannelDataField, Vec<f64>>,
    pub nested_fields: BTreeMap<ChannelDataField, Vec<Vec<f64>>>,
    pub rows: usize,
}

impl Default for ChannelData {
    fn default() -> Self {
        let fields = ChannelDataField::get_field_vec();
        let mut data = ChannelData {
            fields: BTreeMap::new(),
            nested_fields: BTreeMap::new(),
            rows: 0,
        };
        fields.into_iter().for_each(|f| {
            data.fields.insert(f, vec![]);
        });
        data
    }
}

impl UsedSize for ChannelData {
    fn get_used_size(&self) -> usize {
        self.fields.get_used_size() + self.nested_fields.get_used_size()
    }
}

impl ChannelData {
    // Constructor accepting a channel map to initialize only valid fields
    pub fn new(channel_map: &ChannelMap) -> Self {
        let fields = ChannelDataField::get_filtered_field_vec(channel_map);
        let mut data = ChannelData {
            fields: BTreeMap::new(),
            nested_fields: BTreeMap::new(),
            rows: 0,
        };
        fields.into_iter().for_each(|f| {
            if f == ChannelDataField::X || f == ChannelDataField::Z {
                data.nested_fields.insert(f, vec![vec![]]);
            } else {
                data.fields.insert(f, vec![]);
            }
        });
        data
    }

    //To keep columns all same length, push invalid values as necessary
    fn push_defaults(&mut self) {
        for field in self.fields.iter_mut() {
            if field.1.len() < self.rows {
                field.1.push(INVALID_VALUE)
            }
        }

        // Pad nested fields
        for field in self.nested_fields.iter_mut() {
            // Pad outer vector to match rows
            if field.1.len() < self.rows {
                field.1.push(vec![INVALID_VALUE]); // Push an empty vector if missing
            }
        }
    }

    //Update the last element to the given value
    fn set_value(&mut self, field: &ChannelDataField, value: f64) {
        if let Some(list) = self.fields.get_mut(field) {
            if let Some(back) = list.last_mut() {
                *back = value;
            }
        }
    }

    fn set_nested_values(&mut self, field: &ChannelDataField, values: Vec<f64>) {
        if let Some(nested) = self.nested_fields.get_mut(field) {
            nested.push(values);
        }
    }

    pub fn append_event(
        &mut self,
        event: Vec<CompassData>,
        map: &ChannelMap,
        weights: Option<(f64, f64)>,
    ) {
        self.rows += 1;
        self.push_defaults();

        let mut dfl_time = INVALID_VALUE;
        let mut dfr_time = INVALID_VALUE;
        let mut dbl_time = INVALID_VALUE;
        let mut dbr_time = INVALID_VALUE;

        // for cebra relative time
        let mut scint_left_time = INVALID_VALUE;
        let mut anode_back_time = INVALID_VALUE;
        let mut cebra0_time = INVALID_VALUE;
        let mut cebra1_time = INVALID_VALUE;
        let mut cebra2_time = INVALID_VALUE;
        let mut cebra3_time = INVALID_VALUE;
        let mut cebra4_time = INVALID_VALUE;
        let mut cebra5_time = INVALID_VALUE;
        let mut cebra6_time = INVALID_VALUE;
        let mut cebra7_time = INVALID_VALUE;
        let mut cebra8_time = INVALID_VALUE;

        for hit in event.iter() {
            //Fill out detector fields using channel map
            let channel_data = match map.get_channel_data(&hit.uuid) {
                Some(data) => data,
                None => continue,
            };
            match channel_data.channel_type {
                ChannelType::ScintLeft => {
                    self.set_value(&ChannelDataField::ScintLeftEnergy, hit.energy);
                    self.set_value(&ChannelDataField::ScintLeftShort, hit.energy_short);
                    self.set_value(&ChannelDataField::ScintLeftTime, hit.timestamp);
                    scint_left_time = hit.timestamp;
                }

                ChannelType::ScintRight => {
                    self.set_value(&ChannelDataField::ScintRightEnergy, hit.energy);
                    self.set_value(&ChannelDataField::ScintRightShort, hit.energy_short);
                    self.set_value(&ChannelDataField::ScintRightTime, hit.timestamp);
                }

                ChannelType::Cathode => {
                    self.set_value(&ChannelDataField::CathodeEnergy, hit.energy);
                    self.set_value(&ChannelDataField::CathodeShort, hit.energy_short);
                    self.set_value(&ChannelDataField::CathodeTime, hit.timestamp);
                }

                ChannelType::DelayFrontRight => {
                    self.set_value(&ChannelDataField::DelayFrontRightEnergy, hit.energy);
                    self.set_value(&ChannelDataField::DelayFrontRightShort, hit.energy_short);
                    self.set_value(&ChannelDataField::DelayFrontRightTime, hit.timestamp);
                    dfr_time = hit.timestamp;
                }

                ChannelType::DelayFrontLeft => {
                    self.set_value(&ChannelDataField::DelayFrontLeftEnergy, hit.energy);
                    self.set_value(&ChannelDataField::DelayFrontLeftShort, hit.energy_short);
                    self.set_value(&ChannelDataField::DelayFrontLeftTime, hit.timestamp);
                    dfl_time = hit.timestamp;
                }

                ChannelType::DelayBackRight => {
                    self.set_value(&ChannelDataField::DelayBackRightEnergy, hit.energy);
                    self.set_value(&ChannelDataField::DelayBackRightShort, hit.energy_short);
                    self.set_value(&ChannelDataField::DelayBackRightTime, hit.timestamp);
                    dbr_time = hit.timestamp;
                }

                ChannelType::DelayBackLeft => {
                    self.set_value(&ChannelDataField::DelayBackLeftEnergy, hit.energy);
                    self.set_value(&ChannelDataField::DelayBackLeftShort, hit.energy_short);
                    self.set_value(&ChannelDataField::DelayBackLeftTime, hit.timestamp);
                    dbl_time = hit.timestamp;
                }

                ChannelType::AnodeFront => {
                    self.set_value(&ChannelDataField::AnodeFrontEnergy, hit.energy);
                    self.set_value(&ChannelDataField::AnodeFrontShort, hit.energy_short);
                    self.set_value(&ChannelDataField::AnodeFrontTime, hit.timestamp);
                }

                ChannelType::AnodeBack => {
                    self.set_value(&ChannelDataField::AnodeBackEnergy, hit.energy);
                    self.set_value(&ChannelDataField::AnodeBackShort, hit.energy_short);
                    self.set_value(&ChannelDataField::AnodeBackTime, hit.timestamp);
                    anode_back_time = hit.timestamp;
                }

                ChannelType::Cebra0 => {
                    self.set_value(&ChannelDataField::Cebra0Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra0Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra0Time, hit.timestamp);
                    cebra0_time = hit.timestamp;
                }

                ChannelType::Cebra1 => {
                    self.set_value(&ChannelDataField::Cebra1Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra1Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra1Time, hit.timestamp);
                    cebra1_time = hit.timestamp;
                }

                ChannelType::Cebra2 => {
                    self.set_value(&ChannelDataField::Cebra2Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra2Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra2Time, hit.timestamp);
                    cebra2_time = hit.timestamp;
                }

                ChannelType::Cebra3 => {
                    self.set_value(&ChannelDataField::Cebra3Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra3Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra3Time, hit.timestamp);
                    cebra3_time = hit.timestamp;
                }

                ChannelType::Cebra4 => {
                    self.set_value(&ChannelDataField::Cebra4Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra4Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra4Time, hit.timestamp);
                    cebra4_time = hit.timestamp;
                }

                ChannelType::Cebra5 => {
                    self.set_value(&ChannelDataField::Cebra5Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra5Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra5Time, hit.timestamp);
                    cebra5_time = hit.timestamp;
                }

                ChannelType::Cebra6 => {
                    self.set_value(&ChannelDataField::Cebra6Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra6Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra6Time, hit.timestamp);
                    cebra6_time = hit.timestamp;
                }

                ChannelType::Cebra7 => {
                    self.set_value(&ChannelDataField::Cebra7Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra7Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra7Time, hit.timestamp);
                    cebra7_time = hit.timestamp;
                }

                ChannelType::Cebra8 => {
                    self.set_value(&ChannelDataField::Cebra8Energy, hit.energy);
                    self.set_value(&ChannelDataField::Cebra8Short, hit.energy_short);
                    self.set_value(&ChannelDataField::Cebra8Time, hit.timestamp);
                    cebra8_time = hit.timestamp;
                }

                ChannelType::PIPS1000 => {
                    self.set_value(&ChannelDataField::PIPS1000Energy, hit.energy);
                    self.set_value(&ChannelDataField::PIPS1000Short, hit.energy_short);
                    self.set_value(&ChannelDataField::PIPS1000Time, hit.timestamp);
                }

                ChannelType::PIPS500 => {
                    self.set_value(&ChannelDataField::PIPS500Energy, hit.energy);
                    self.set_value(&ChannelDataField::PIPS500Short, hit.energy_short);
                    self.set_value(&ChannelDataField::PIPS500Time, hit.timestamp);
                }

                ChannelType::PIPS300 => {
                    self.set_value(&ChannelDataField::PIPS300Energy, hit.energy);
                    self.set_value(&ChannelDataField::PIPS300Short, hit.energy_short);
                    self.set_value(&ChannelDataField::PIPS300Time, hit.timestamp);
                }

                ChannelType::PIPS100 => {
                    self.set_value(&ChannelDataField::PIPS100Energy, hit.energy);
                    self.set_value(&ChannelDataField::PIPS100Short, hit.energy_short);
                    self.set_value(&ChannelDataField::PIPS100Time, hit.timestamp);
                }

                ChannelType::CATRINA0 => {
                    self.set_value(&ChannelDataField::CATRINA0Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA0Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA0Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA0PSD, psd);
                }

                ChannelType::CATRINA1 => {
                    self.set_value(&ChannelDataField::CATRINA1Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA1Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA1Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA1PSD, psd);
                }

                ChannelType::CATRINA2 => {
                    self.set_value(&ChannelDataField::CATRINA2Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA2Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA2Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA2PSD, psd);
                }
                _ => continue,
            }
        }

        //Physics
        let mut x1 = INVALID_VALUE;
        let mut x2 = INVALID_VALUE;
        if dfr_time != INVALID_VALUE && dfl_time != INVALID_VALUE {
            x1 = (dfl_time - dfr_time) * 0.5 * 1.0 / 2.1;
            self.set_value(&ChannelDataField::X1, x1);
        }
        if dbr_time != INVALID_VALUE && dbl_time != INVALID_VALUE {
            x2 = (dbl_time - dbr_time) * 0.5 * 1.0 / 1.98;
            self.set_value(&ChannelDataField::X2, x2);
        }
        if x1 != INVALID_VALUE && x2 != INVALID_VALUE {
            let diff = x2 - x1;
            if diff > 0.0 {
                self.set_value(&ChannelDataField::Theta, (diff / 36.0).atan());
            } else if diff < 0.0 {
                self.set_value(
                    &ChannelDataField::Theta,
                    std::f64::consts::PI + (diff / 36.0).atan(),
                );
            } else {
                self.set_value(&ChannelDataField::Theta, std::f64::consts::PI * 0.5);
            }

            match weights {
                Some(w) => self.set_value(&ChannelDataField::Xavg, w.0 * x1 + w.1 * x2),
                None => self.set_value(&ChannelDataField::Xavg, INVALID_VALUE),
            };

            let z_values: Vec<f64> = (0..400)
                .map(|i| -50.0 + (100.0 / 400.0) * i as f64)
                .collect();

            let x_values: Vec<f64> = z_values
                .iter()
                .map(|&z| (z / 42.8625 + 0.5) * (x2 - x1) + x1)
                .collect();

            self.set_nested_values(&ChannelDataField::X, x_values);
            self.set_nested_values(&ChannelDataField::Z, z_values);
        }

        if scint_left_time != INVALID_VALUE && anode_back_time != INVALID_VALUE {
            if cebra0_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra0RelTime,
                    cebra0_time - scint_left_time,
                );
            }

            if cebra1_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra1RelTime,
                    cebra1_time - scint_left_time,
                );
            }

            if cebra2_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra2RelTime,
                    cebra2_time - scint_left_time,
                );
            }

            if cebra3_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra3RelTime,
                    cebra3_time - scint_left_time,
                );
            }

            if cebra4_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra4RelTime,
                    cebra4_time - scint_left_time,
                );
            }

            if cebra5_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra5RelTime,
                    cebra5_time - scint_left_time,
                );
            }

            if cebra6_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra6RelTime,
                    cebra6_time - scint_left_time,
                );
            }

            if cebra7_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra7RelTime,
                    cebra7_time - scint_left_time,
                );
            }

            if cebra8_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::Cebra8RelTime,
                    cebra8_time - scint_left_time,
                );
            }
        }
    }

    pub fn convert_to_columns(self) -> Vec<Column> {
        let mut columns = vec![];

        let normal_columns: Vec<Column> = self
            .fields
            .into_iter()
            .map(|(field, values)| {
                let name = field.as_ref().into();
                // Convert each field into a Series and then into a Column
                let series = Series::new(name, values);
                Column::Series(series.into())
            })
            .collect();

        columns.extend(normal_columns);

        let nested_columns: Vec<Column> = self
            .nested_fields
            .into_iter()
            .map(|(field, nested_values)| {
                let name = field.as_ref().into();

                // Convert Vec<Vec<f64>> into a ListChunked
                let list_chunked = ListChunked::from_iter(
                    nested_values
                        .into_iter()
                        .map(|inner_vec| Some(Series::new("".into(), inner_vec))),
                )
                .with_name(name);

                Column::Series(list_chunked.into_series().into())
            })
            .collect();

        columns.extend(nested_columns);

        columns
    }
}
