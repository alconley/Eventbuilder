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
    // X,
    // Z,
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

    PIPS1000Time,
    PIPS500Time,
    PIPS300Time,
    PIPS100Time,

    PIPS1000RelTime,
    PIPS500RelTime,
    PIPS300RelTime,
    PIPS100RelTime,

    PIPS1000RelTimeToPIPS500,
    PIPS1000RelTimeToPIPS300,

    CATRINA0Energy,
    CATRINA1Energy,
    CATRINA2Energy,
    CATRINA3Energy,
    CATRINA4Energy,
    CATRINA5Energy,
    CATRINA6Energy,
    CATRINA7Energy,
    CATRINA8Energy,
    CATRINA9Energy,
    CATRINA10Energy,
    CATRINA11Energy,
    CATRINA12Energy,
    CATRINA13Energy,
    CATRINA14Energy,
    CATRINA15Energy,

    CATRINA0Short,
    CATRINA1Short,
    CATRINA2Short,
    CATRINA3Short,
    CATRINA4Short,
    CATRINA5Short,
    CATRINA6Short,
    CATRINA7Short,
    CATRINA8Short,
    CATRINA9Short,
    CATRINA10Short,
    CATRINA11Short,
    CATRINA12Short,
    CATRINA13Short,
    CATRINA14Short,
    CATRINA15Short,

    CATRINA0Time,
    CATRINA1Time,
    CATRINA2Time,
    CATRINA3Time,
    CATRINA4Time,
    CATRINA5Time,
    CATRINA6Time,
    CATRINA7Time,
    CATRINA8Time,
    CATRINA9Time,
    CATRINA10Time,
    CATRINA11Time,
    CATRINA12Time,
    CATRINA13Time,
    CATRINA14Time,
    CATRINA15Time,

    CATRINA0PSD,
    CATRINA1PSD,
    CATRINA2PSD,
    CATRINA3PSD,
    CATRINA4PSD,
    CATRINA5PSD,
    CATRINA6PSD,
    CATRINA7PSD,
    CATRINA8PSD,
    CATRINA9PSD,
    CATRINA10PSD,
    CATRINA11PSD,
    CATRINA12PSD,
    CATRINA13PSD,
    CATRINA14PSD,
    CATRINA15PSD,

    LeftStrip0Energy,
    LeftStrip1Energy,
    LeftStrip2Energy,
    LeftStrip3Energy,
    LeftStrip4Energy,
    LeftStrip5Energy,
    LeftStrip6Energy,
    LeftStrip7Energy,
    LeftStrip8Energy,
    LeftStrip9Energy,
    LeftStrip10Energy,
    LeftStrip11Energy,
    LeftStrip12Energy,
    LeftStrip13Energy,
    LeftStrip14Energy,
    LeftStrip15Energy,

    LeftStrip0Time,
    LeftStrip1Time,
    LeftStrip2Time,
    LeftStrip3Time,
    LeftStrip4Time,
    LeftStrip5Time,
    LeftStrip6Time,
    LeftStrip7Time,
    LeftStrip8Time,
    LeftStrip9Time,
    LeftStrip10Time,
    LeftStrip11Time,
    LeftStrip12Time,
    LeftStrip13Time,
    LeftStrip14Time,
    LeftStrip15Time,

    RightStrip0Energy,
    RightStrip1Energy,
    RightStrip2Energy,
    RightStrip3Energy,
    RightStrip4Energy,
    RightStrip5Energy,
    RightStrip6Energy,
    RightStrip7Energy,
    RightStrip8Energy,
    RightStrip9Energy,
    RightStrip10Energy,
    RightStrip11Energy,
    RightStrip12Energy,
    RightStrip13Energy,
    RightStrip14Energy,
    RightStrip15Energy,

    RightStrip0Time,
    RightStrip1Time,
    RightStrip2Time,
    RightStrip3Time,
    RightStrip4Time,
    RightStrip5Time,
    RightStrip6Time,
    RightStrip7Time,
    RightStrip8Time,
    RightStrip9Time,
    RightStrip10Time,
    RightStrip11Time,
    RightStrip12Time,
    RightStrip13Time,
    RightStrip14Time,
    RightStrip15Time,

    Strip0Energy,
    Strip17Energy,

    Strip0Time,
    Strip17Time,

    RF,
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
                    // | ChannelDataField::X
                    // | ChannelDataField::Z
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

                    ChannelDataField::PIPS1000Energy | ChannelDataField::PIPS1000Time => {
                        channel_map.contains_channel_type(ChannelType::PIPS1000)
                    }
                    ChannelDataField::PIPS1000RelTime => {
                        channel_map.contains_channel_type(ChannelType::PIPS1000)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }
                    ChannelDataField::PIPS1000RelTimeToPIPS500 => {
                        channel_map.contains_channel_type(ChannelType::PIPS1000)
                            && channel_map.contains_channel_type(ChannelType::PIPS500)
                    }
                    ChannelDataField::PIPS1000RelTimeToPIPS300 => {
                        channel_map.contains_channel_type(ChannelType::PIPS1000)
                            && channel_map.contains_channel_type(ChannelType::PIPS300)
                    }
                    ChannelDataField::PIPS500Energy | ChannelDataField::PIPS500Time => {
                        channel_map.contains_channel_type(ChannelType::PIPS500)
                    }
                    ChannelDataField::PIPS500RelTime => {
                        channel_map.contains_channel_type(ChannelType::PIPS500)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }

                    ChannelDataField::PIPS300Energy | ChannelDataField::PIPS300Time => {
                        channel_map.contains_channel_type(ChannelType::PIPS300)
                    }
                    ChannelDataField::PIPS300RelTime => {
                        channel_map.contains_channel_type(ChannelType::PIPS300)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
                    }

                    ChannelDataField::PIPS100Energy | ChannelDataField::PIPS100Time => {
                        channel_map.contains_channel_type(ChannelType::PIPS100)
                    }
                    ChannelDataField::PIPS100RelTime => {
                        channel_map.contains_channel_type(ChannelType::PIPS100)
                            && channel_map.contains_channel_type(ChannelType::ScintLeft)
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

                    ChannelDataField::CATRINA3Energy
                    | ChannelDataField::CATRINA3Short
                    | ChannelDataField::CATRINA3Time
                    | ChannelDataField::CATRINA3PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA3)
                    }

                    ChannelDataField::CATRINA4Energy
                    | ChannelDataField::CATRINA4Short
                    | ChannelDataField::CATRINA4Time
                    | ChannelDataField::CATRINA4PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA4)
                    }

                    ChannelDataField::CATRINA5Energy
                    | ChannelDataField::CATRINA5Short
                    | ChannelDataField::CATRINA5Time
                    | ChannelDataField::CATRINA5PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA5)
                    }

                    ChannelDataField::CATRINA6Energy
                    | ChannelDataField::CATRINA6Short
                    | ChannelDataField::CATRINA6Time
                    | ChannelDataField::CATRINA6PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA6)
                    }

                    ChannelDataField::CATRINA7Energy
                    | ChannelDataField::CATRINA7Short
                    | ChannelDataField::CATRINA7Time
                    | ChannelDataField::CATRINA7PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA7)
                    }

                    ChannelDataField::CATRINA8Energy
                    | ChannelDataField::CATRINA8Short
                    | ChannelDataField::CATRINA8Time
                    | ChannelDataField::CATRINA8PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA8)
                    }

                    ChannelDataField::CATRINA9Energy
                    | ChannelDataField::CATRINA9Short
                    | ChannelDataField::CATRINA9Time
                    | ChannelDataField::CATRINA9PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA9)
                    }

                    ChannelDataField::CATRINA10Energy
                    | ChannelDataField::CATRINA10Short
                    | ChannelDataField::CATRINA10Time
                    | ChannelDataField::CATRINA10PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA10)
                    }

                    ChannelDataField::CATRINA11Energy
                    | ChannelDataField::CATRINA11Short
                    | ChannelDataField::CATRINA11Time
                    | ChannelDataField::CATRINA11PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA11)
                    }

                    ChannelDataField::CATRINA12Energy
                    | ChannelDataField::CATRINA12Short
                    | ChannelDataField::CATRINA12Time
                    | ChannelDataField::CATRINA12PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA12)
                    }

                    ChannelDataField::CATRINA13Energy
                    | ChannelDataField::CATRINA13Short
                    | ChannelDataField::CATRINA13Time
                    | ChannelDataField::CATRINA13PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA13)
                    }

                    ChannelDataField::CATRINA14Energy
                    | ChannelDataField::CATRINA14Short
                    | ChannelDataField::CATRINA14Time
                    | ChannelDataField::CATRINA14PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA14)
                    }

                    ChannelDataField::CATRINA15Energy
                    | ChannelDataField::CATRINA15Short
                    | ChannelDataField::CATRINA15Time
                    | ChannelDataField::CATRINA15PSD => {
                        channel_map.contains_channel_type(ChannelType::CATRINA15)
                    }

                    ChannelDataField::RF => {
                        channel_map.contains_channel_type(ChannelType::RF)
                    }

                    ChannelDataField::LeftStrip0Energy
                    | ChannelDataField::LeftStrip0Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip0)
                    }

                    ChannelDataField::LeftStrip1Energy
                    | ChannelDataField::LeftStrip1Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip1)
                    }

                    ChannelDataField::LeftStrip2Energy
                    | ChannelDataField::LeftStrip2Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip2)
                    }

                    ChannelDataField::LeftStrip3Energy
                    | ChannelDataField::LeftStrip3Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip3)
                    }

                    ChannelDataField::LeftStrip4Energy
                    | ChannelDataField::LeftStrip4Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip4)
                    }

                    ChannelDataField::LeftStrip5Energy
                    | ChannelDataField::LeftStrip5Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip5)
                    }

                    ChannelDataField::LeftStrip6Energy
                    | ChannelDataField::LeftStrip6Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip6)
                    }

                    ChannelDataField::LeftStrip7Energy
                    | ChannelDataField::LeftStrip7Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip7)
                    }

                    ChannelDataField::LeftStrip8Energy
                    | ChannelDataField::LeftStrip8Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip8)
                    }

                    ChannelDataField::LeftStrip9Energy
                    | ChannelDataField::LeftStrip9Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip9)
                    }

                    ChannelDataField::LeftStrip10Energy
                    | ChannelDataField::LeftStrip10Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip10)
                    }

                    ChannelDataField::LeftStrip11Energy
                    | ChannelDataField::LeftStrip11Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip11)
                    }

                    ChannelDataField::LeftStrip12Energy
                    | ChannelDataField::LeftStrip12Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip12)
                    }

                    ChannelDataField::LeftStrip13Energy
                    | ChannelDataField::LeftStrip13Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip13)
                    }

                    ChannelDataField::LeftStrip14Energy
                    | ChannelDataField::LeftStrip14Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip14)
                    }

                    ChannelDataField::LeftStrip15Energy
                    | ChannelDataField::LeftStrip15Time => {
                        channel_map.contains_channel_type(ChannelType::LeftStrip15)
                    }

                    ChannelDataField::RightStrip0Energy
                    | ChannelDataField::RightStrip0Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip0)
                    }

                    ChannelDataField::RightStrip1Energy
                    | ChannelDataField::RightStrip1Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip1)
                    }

                    ChannelDataField::RightStrip2Energy
                    | ChannelDataField::RightStrip2Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip2)
                    }

                    ChannelDataField::RightStrip3Energy
                    | ChannelDataField::RightStrip3Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip3)
                    }

                    ChannelDataField::RightStrip4Energy
                    | ChannelDataField::RightStrip4Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip4)
                    }

                    ChannelDataField::RightStrip5Energy
                    | ChannelDataField::RightStrip5Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip5)
                    }

                    ChannelDataField::RightStrip6Energy
                    | ChannelDataField::RightStrip6Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip6)
                    }

                    ChannelDataField::RightStrip7Energy
                    | ChannelDataField::RightStrip7Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip7)
                    }

                    ChannelDataField::RightStrip8Energy
                    | ChannelDataField::RightStrip8Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip8)
                    }

                    ChannelDataField::RightStrip9Energy
                    | ChannelDataField::RightStrip9Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip9)
                    }

                    ChannelDataField::RightStrip10Energy
                    | ChannelDataField::RightStrip10Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip10)
                    }

                    ChannelDataField::RightStrip11Energy
                    | ChannelDataField::RightStrip11Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip11)
                    }

                    ChannelDataField::RightStrip12Energy
                    | ChannelDataField::RightStrip12Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip12)
                    }

                    ChannelDataField::RightStrip13Energy
                    | ChannelDataField::RightStrip13Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip13)
                    }

                    ChannelDataField::RightStrip14Energy
                    | ChannelDataField::RightStrip14Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip14)
                    }

                    ChannelDataField::RightStrip15Energy
                    | ChannelDataField::RightStrip15Time => {
                        channel_map.contains_channel_type(ChannelType::RightStrip15)
                    }

                    ChannelDataField::Strip0Energy | ChannelDataField::Strip0Time => {
                        channel_map.contains_channel_type(ChannelType::Strip0)
                    }

                    ChannelDataField::Strip17Energy | ChannelDataField::Strip17Time => {
                        channel_map.contains_channel_type(ChannelType::Strip17)
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
            // if f == ChannelDataField::X || f == ChannelDataField::Z {
            //     data.nested_fields.insert(f, vec![vec![]]);
            // } else {
            data.fields.insert(f, vec![]);
            // }
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

    fn _set_nested_values(&mut self, field: &ChannelDataField, values: Vec<f64>) {
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

        let mut scint_left_time = INVALID_VALUE;
        let mut anode_back_time = INVALID_VALUE;

        // for cebra relative time
        let mut cebra0_time = INVALID_VALUE;
        let mut cebra1_time = INVALID_VALUE;
        let mut cebra2_time = INVALID_VALUE;
        let mut cebra3_time = INVALID_VALUE;
        let mut cebra4_time = INVALID_VALUE;
        let mut cebra5_time = INVALID_VALUE;
        let mut cebra6_time = INVALID_VALUE;
        let mut cebra7_time = INVALID_VALUE;
        let mut cebra8_time = INVALID_VALUE;

        // for pips relative time
        let mut pips1000_time = INVALID_VALUE;
        let mut pips500_time = INVALID_VALUE;
        let mut pips300_time = INVALID_VALUE;
        let mut pips100_time = INVALID_VALUE;

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
                    self.set_value(&ChannelDataField::PIPS1000Time, hit.timestamp);
                    pips1000_time = hit.timestamp;
                }

                ChannelType::PIPS500 => {
                    self.set_value(&ChannelDataField::PIPS500Energy, hit.energy);
                    self.set_value(&ChannelDataField::PIPS500Time, hit.timestamp);
                    pips500_time = hit.timestamp;
                }

                ChannelType::PIPS300 => {
                    self.set_value(&ChannelDataField::PIPS300Energy, hit.energy);
                    self.set_value(&ChannelDataField::PIPS300Time, hit.timestamp);
                    pips300_time = hit.timestamp;
                }

                ChannelType::PIPS100 => {
                    self.set_value(&ChannelDataField::PIPS100Energy, hit.energy);
                    self.set_value(&ChannelDataField::PIPS100Time, hit.timestamp);
                    pips100_time = hit.timestamp;
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

                ChannelType::CATRINA3 => {
                    self.set_value(&ChannelDataField::CATRINA3Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA3Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA3Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA3PSD, psd);
                }

                ChannelType::CATRINA4 => {
                    self.set_value(&ChannelDataField::CATRINA4Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA4Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA4Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA4PSD, psd);
                }

                ChannelType::CATRINA5 => {
                    self.set_value(&ChannelDataField::CATRINA5Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA5Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA5Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA5PSD, psd);
                }

                ChannelType::CATRINA6 => {
                    self.set_value(&ChannelDataField::CATRINA6Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA6Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA6Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA6PSD, psd);
                }

                ChannelType::CATRINA7 => {
                    self.set_value(&ChannelDataField::CATRINA7Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA7Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA7Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA7PSD, psd);
                }

                ChannelType::CATRINA8 => {
                    self.set_value(&ChannelDataField::CATRINA8Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA8Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA8Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA8PSD, psd);
                }

                ChannelType::CATRINA9 => {
                    self.set_value(&ChannelDataField::CATRINA9Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA9Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA9Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA9PSD, psd);
                }

                ChannelType::CATRINA10 => {
                    self.set_value(&ChannelDataField::CATRINA10Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA10Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA10Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA10PSD, psd);
                }

                ChannelType::CATRINA11 => {
                    self.set_value(&ChannelDataField::CATRINA11Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA11Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA11Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA11PSD, psd);
                }

                ChannelType::CATRINA12 => {
                    self.set_value(&ChannelDataField::CATRINA12Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA12Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA12Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA12PSD, psd);
                }

                ChannelType::CATRINA13 => {
                    self.set_value(&ChannelDataField::CATRINA13Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA13Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA13Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA13PSD, psd);
                }

                ChannelType::CATRINA14 => {
                    self.set_value(&ChannelDataField::CATRINA14Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA14Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA14Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA14PSD, psd);
                }

                ChannelType::CATRINA15 => {
                    self.set_value(&ChannelDataField::CATRINA15Energy, hit.energy);
                    self.set_value(&ChannelDataField::CATRINA15Short, hit.energy_short);
                    self.set_value(&ChannelDataField::CATRINA15Time, hit.timestamp);
                    let long = hit.energy;
                    let short = hit.energy_short;
                    let psd = (long - short) / long;
                    self.set_value(&ChannelDataField::CATRINA15PSD, psd);
                }

                ChannelType::LeftStrip0 => {
                    self.set_value(&ChannelDataField::LeftStrip0Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip0Time, hit.timestamp);
                }

                ChannelType::LeftStrip1 => {
                    self.set_value(&ChannelDataField::LeftStrip1Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip1Time, hit.timestamp);
                }

                ChannelType::LeftStrip2 => {
                    self.set_value(&ChannelDataField::LeftStrip2Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip2Time, hit.timestamp);
                }

                ChannelType::LeftStrip3 => {
                    self.set_value(&ChannelDataField::LeftStrip3Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip3Time, hit.timestamp);
                }

                ChannelType::LeftStrip4 => {
                    self.set_value(&ChannelDataField::LeftStrip4Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip4Time, hit.timestamp);
                }

                ChannelType::LeftStrip5 => {
                    self.set_value(&ChannelDataField::LeftStrip5Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip5Time, hit.timestamp);
                }

                ChannelType::LeftStrip6 => {
                    self.set_value(&ChannelDataField::LeftStrip6Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip6Time, hit.timestamp);
                }

                ChannelType::LeftStrip7 => {
                    self.set_value(&ChannelDataField::LeftStrip7Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip7Time, hit.timestamp);
                }

                ChannelType::LeftStrip8 => {
                    self.set_value(&ChannelDataField::LeftStrip8Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip8Time, hit.timestamp);
                }

                ChannelType::LeftStrip9 => {
                    self.set_value(&ChannelDataField::LeftStrip9Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip9Time, hit.timestamp);
                }

                ChannelType::LeftStrip10 => {
                    self.set_value(&ChannelDataField::LeftStrip10Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip10Time, hit.timestamp);
                }

                ChannelType::LeftStrip11 => {
                    self.set_value(&ChannelDataField::LeftStrip11Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip11Time, hit.timestamp);
                }

                ChannelType::LeftStrip12 => {
                    self.set_value(&ChannelDataField::LeftStrip12Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip12Time, hit.timestamp);
                }

                ChannelType::LeftStrip13 => {
                    self.set_value(&ChannelDataField::LeftStrip13Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip13Time, hit.timestamp);
                }

                ChannelType::LeftStrip14 => {
                    self.set_value(&ChannelDataField::LeftStrip14Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip14Time, hit.timestamp);
                }

                ChannelType::LeftStrip15 => {
                    self.set_value(&ChannelDataField::LeftStrip15Energy, hit.energy);
                    self.set_value(&ChannelDataField::LeftStrip15Time, hit.timestamp);
                }

                ChannelType::RightStrip0 => {
                    self.set_value(&ChannelDataField::RightStrip0Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip0Time, hit.timestamp);
                }

                ChannelType::RightStrip1 => {
                    self.set_value(&ChannelDataField::RightStrip1Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip1Time, hit.timestamp);
                }

                ChannelType::RightStrip2 => {
                    self.set_value(&ChannelDataField::RightStrip2Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip2Time, hit.timestamp);
                }

                ChannelType::RightStrip3 => {
                    self.set_value(&ChannelDataField::RightStrip3Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip3Time, hit.timestamp);
                }

                ChannelType::RightStrip4 => {
                    self.set_value(&ChannelDataField::RightStrip4Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip4Time, hit.timestamp);
                }

                ChannelType::RightStrip5 => {
                    self.set_value(&ChannelDataField::RightStrip5Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip5Time, hit.timestamp);
                }

                ChannelType::RightStrip6 => {
                    self.set_value(&ChannelDataField::RightStrip6Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip6Time, hit.timestamp);
                }

                ChannelType::RightStrip7 => {
                    self.set_value(&ChannelDataField::RightStrip7Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip7Time, hit.timestamp);
                }

                ChannelType::RightStrip8 => {
                    self.set_value(&ChannelDataField::RightStrip8Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip8Time, hit.timestamp);
                }

                ChannelType::RightStrip9 => {
                    self.set_value(&ChannelDataField::RightStrip9Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip9Time, hit.timestamp);
                }

                ChannelType::RightStrip10 => {
                    self.set_value(&ChannelDataField::RightStrip10Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip10Time, hit.timestamp);
                }

                ChannelType::RightStrip11 => {
                    self.set_value(&ChannelDataField::RightStrip11Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip11Time, hit.timestamp);
                }

                ChannelType::RightStrip12 => {
                    self.set_value(&ChannelDataField::RightStrip12Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip12Time, hit.timestamp);
                }

                ChannelType::RightStrip13 => {
                    self.set_value(&ChannelDataField::RightStrip13Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip13Time, hit.timestamp);
                }

                ChannelType::RightStrip14 => {
                    self.set_value(&ChannelDataField::RightStrip14Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip14Time, hit.timestamp);
                }

                ChannelType::RightStrip15 => {
                    self.set_value(&ChannelDataField::RightStrip15Energy, hit.energy);
                    self.set_value(&ChannelDataField::RightStrip15Time, hit.timestamp);
                }

                ChannelType::Strip0 => {
                    self.set_value(&ChannelDataField::Strip0Energy, hit.energy);
                    self.set_value(&ChannelDataField::Strip0Time, hit.timestamp);
                }

                ChannelType::Strip17 => {
                    self.set_value(&ChannelDataField::Strip17Energy, hit.energy);
                    self.set_value(&ChannelDataField::Strip17Time, hit.timestamp);
                }

                ChannelType::RF => {
                    self.set_value(&ChannelDataField::RF, hit.timestamp);
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

            // let z_values: Vec<f64> = (0..400)
            //     .map(|i| -50.0 + (100.0 / 400.0) * i as f64)
            //     .collect();

            // let x_values: Vec<f64> = z_values
            //     .iter()
            //     .map(|&z| (z / 42.8625 + 0.5) * (x2 - x1) + x1)
            //     .collect();

            // self.set_nested_values(&ChannelDataField::X, x_values);
            // self.set_nested_values(&ChannelDataField::Z, z_values);
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

        if scint_left_time != INVALID_VALUE {
            if pips1000_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::PIPS1000RelTime,
                    pips1000_time - scint_left_time,
                );
            }

            if pips500_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::PIPS500RelTime,
                    pips500_time - scint_left_time,
                );
            }

            if pips300_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::PIPS300RelTime,
                    pips300_time - scint_left_time,
                );
            }

            if pips100_time != INVALID_VALUE {
                self.set_value(
                    &ChannelDataField::PIPS100RelTime,
                    pips100_time - scint_left_time,
                );
            }
        }

        if pips1000_time != INVALID_VALUE && pips500_time != INVALID_VALUE {
            self.set_value(
                &ChannelDataField::PIPS1000RelTimeToPIPS500,
                pips1000_time - pips500_time,
            );
        }

        if pips1000_time != INVALID_VALUE && pips300_time != INVALID_VALUE {
            self.set_value(
                &ChannelDataField::PIPS1000RelTimeToPIPS300,
                pips1000_time - pips300_time,
            );
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
