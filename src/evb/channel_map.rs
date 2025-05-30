use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::ParseIntError;
use strum_macros::{AsRefStr, EnumIter};

use super::compass_data::generate_board_channel_uuid;

//Channels to be mapped in the ChannelMap, each variant is the verbatim keyword in the channel map
#[derive(Debug, Clone, Copy, PartialEq, AsRefStr, EnumIter, Serialize, Deserialize)]
pub enum ChannelType {
    //Detector fields -> can be channel mapped
    AnodeFront,
    AnodeBack,
    ScintLeft,
    ScintRight,
    Cathode,
    DelayFrontLeft,
    DelayFrontRight,
    DelayBackLeft,
    DelayBackRight,
    Monitor,

    Cebra0,
    Cebra1,
    Cebra2,
    Cebra3,
    Cebra4,
    Cebra5,
    Cebra6,
    Cebra7,
    Cebra8,

    PIPS1000,
    PIPS500,
    PIPS300,
    PIPS100,

    CATRINA0,
    CATRINA1,
    CATRINA2,
    CATRINA3,
    CATRINA4,
    CATRINA5,
    CATRINA6,
    CATRINA7,
    CATRINA8,
    CATRINA9,
    CATRINA10,
    CATRINA11,
    CATRINA12,
    CATRINA13,
    CATRINA14,
    CATRINA15,

    RF,

    LeftStrip0,
    LeftStrip1,
    LeftStrip2,
    LeftStrip3,
    LeftStrip4,
    LeftStrip5,
    LeftStrip6,
    LeftStrip7,
    LeftStrip8,
    LeftStrip9,
    LeftStrip10,
    LeftStrip11,
    LeftStrip12,
    LeftStrip13,
    LeftStrip14,
    LeftStrip15,

    RightStrip0,
    RightStrip1,
    RightStrip2,
    RightStrip3,
    RightStrip4,
    RightStrip5,
    RightStrip6,
    RightStrip7,
    RightStrip8,
    RightStrip9,
    RightStrip10,
    RightStrip11,
    RightStrip12,
    RightStrip13,
    RightStrip14,
    RightStrip15,

    Strip0,
    Strip17,

    // make sure to update app.rs so the channel map combo box are updated

    //Invalid channel
    None,
}

impl ChannelType {
    fn default() -> Self {
        ChannelType::None // Default type
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Board {
    pub id: u32,                     // Board ID
    pub channels: [ChannelType; 16], // Each board has 16 channels
}

impl Default for Board {
    fn default() -> Self {
        Board {
            id: 0,                                  // Default board ID
            channels: [ChannelType::default(); 16], // Initialize all channels with the default type
        }
    }
}

impl Board {
    pub fn sps(id: u32) -> Board {
        let channels = [
            ChannelType::ScintRight,
            ChannelType::ScintLeft,
            ChannelType::None,
            ChannelType::None,
            ChannelType::None,
            ChannelType::None,
            ChannelType::None,
            ChannelType::Cathode,
            ChannelType::DelayFrontLeft,
            ChannelType::DelayFrontRight,
            ChannelType::DelayBackLeft,
            ChannelType::DelayBackRight,
            ChannelType::None,
            ChannelType::AnodeFront,
            ChannelType::None,
            ChannelType::AnodeBack,
        ];
        Board { id, channels }
    }

    pub fn cebra(id: u32) -> Board {
        let channels = [
            ChannelType::Cebra0,
            ChannelType::Cebra1,
            ChannelType::Cebra2,
            ChannelType::Cebra3,
            ChannelType::Cebra4,
            ChannelType::Cebra5,
            ChannelType::Cebra6,
            ChannelType::Cebra7,
            ChannelType::Cebra8,
            ChannelType::None,
            ChannelType::None,
            ChannelType::None,
            ChannelType::None,
            ChannelType::None,
            ChannelType::None,
            ChannelType::None,
        ];
        Board { id, channels }
    }

    pub fn catrina(id: u32) -> Board {
        let channels = [
            ChannelType::CATRINA0,
            ChannelType::CATRINA1,
            ChannelType::CATRINA2,
            ChannelType::CATRINA3,
            ChannelType::CATRINA4,
            ChannelType::CATRINA5,
            ChannelType::CATRINA6,
            ChannelType::CATRINA7,
            ChannelType::CATRINA8,
            ChannelType::CATRINA9,
            ChannelType::CATRINA10,
            ChannelType::CATRINA11,
            ChannelType::CATRINA12,
            ChannelType::CATRINA13,
            ChannelType::CATRINA14,
            ChannelType::CATRINA15,
        ];
        Board { id, channels }
    }

    pub fn left_strip(id: u32) -> Board {
        let channels = [
            ChannelType::LeftStrip0,
            ChannelType::LeftStrip1,
            ChannelType::LeftStrip2,
            ChannelType::LeftStrip3,
            ChannelType::LeftStrip4,
            ChannelType::LeftStrip5,
            ChannelType::LeftStrip6,
            ChannelType::LeftStrip7,
            ChannelType::LeftStrip8,
            ChannelType::LeftStrip9,
            ChannelType::LeftStrip10,
            ChannelType::LeftStrip11,
            ChannelType::LeftStrip12,
            ChannelType::LeftStrip13,
            ChannelType::LeftStrip14,
            ChannelType::LeftStrip15,
        ];
        Board { id, channels }
    }
    pub fn right_strip(id: u32) -> Board {
        let channels = [
            ChannelType::RightStrip0,
            ChannelType::RightStrip1,
            ChannelType::RightStrip2,
            ChannelType::RightStrip3,
            ChannelType::RightStrip4,
            ChannelType::RightStrip5,
            ChannelType::RightStrip6,
            ChannelType::RightStrip7,
            ChannelType::RightStrip8,
            ChannelType::RightStrip9,
            ChannelType::RightStrip10,
            ChannelType::RightStrip11,
            ChannelType::RightStrip12,
            ChannelType::RightStrip13,
            ChannelType::RightStrip14,
            ChannelType::RightStrip15,
        ];
        Board { id, channels }
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, board_idx: usize, on_remove: impl FnOnce()) {
        ui.vertical(|ui| {
            egui::Grid::new(format!("board_{}", board_idx))
                .num_columns(2)
                .spacing([20.0, 4.0])
                .show(ui, |ui| {
                    ui.add(egui::DragValue::new(&mut self.id).prefix("Board ID: "))
                        .on_hover_text("If data is from CoMPASS, the board id will start at 0 and increment by 1 for each board.\nIf the data is converted from FSUDAQ to CAEN format, the board id is the id on the digitizer.");

                        ui.horizontal(|ui| {
                            if ui.small_button("Clear").clicked() {
                                for channel in self.channels.iter_mut() {
                                    *channel = ChannelType::None;
                                }
                            }

                            ui.separator();

                            if ui.button("❌").clicked() {
                                on_remove();
                            }
                        });

                    ui.end_row();
                    ui.label("#");
                    ui.label("Type");
                    ui.end_row();

                    for (channel_idx, channel_type) in self.channels.iter_mut().enumerate() {
                        ui.label(format!("{}", channel_idx));
                        egui::ComboBox::from_id_salt(format!(
                            "channel_type_{}_{}",
                            board_idx, channel_idx
                        ))
                        .selected_text(format!("{:?}", channel_type))
                        .show_ui(ui, |ui| {
                            for variant in [
                                ChannelType::AnodeFront,
                                ChannelType::AnodeBack,
                                ChannelType::ScintLeft,
                                ChannelType::ScintRight,
                                ChannelType::Cathode,
                                ChannelType::DelayFrontLeft,
                                ChannelType::DelayFrontRight,
                                ChannelType::DelayBackLeft,
                                ChannelType::DelayBackRight,
                                ChannelType::Monitor,
                                ChannelType::Cebra0,
                                ChannelType::Cebra1,
                                ChannelType::Cebra2,
                                ChannelType::Cebra3,
                                ChannelType::Cebra4,
                                ChannelType::Cebra5,
                                ChannelType::Cebra6,
                                ChannelType::Cebra7,
                                ChannelType::Cebra8,
                                ChannelType::PIPS1000,
                                ChannelType::PIPS500,
                                ChannelType::PIPS300,
                                ChannelType::PIPS100,
                                ChannelType::CATRINA0,
                                ChannelType::CATRINA1,
                                ChannelType::CATRINA2,
                                ChannelType::CATRINA3,
                                ChannelType::CATRINA4,
                                ChannelType::CATRINA5,
                                ChannelType::CATRINA6,
                                ChannelType::CATRINA7,
                                ChannelType::CATRINA8,
                                ChannelType::CATRINA9,
                                ChannelType::CATRINA10,
                                ChannelType::CATRINA11,
                                ChannelType::CATRINA12,
                                ChannelType::CATRINA13,
                                ChannelType::CATRINA14,
                                ChannelType::CATRINA15,
                                ChannelType::LeftStrip0,
                                ChannelType::LeftStrip1,
                                ChannelType::LeftStrip2,
                                ChannelType::LeftStrip3,
                                ChannelType::LeftStrip4,
                                ChannelType::LeftStrip5,
                                ChannelType::LeftStrip6,
                                ChannelType::LeftStrip7,
                                ChannelType::LeftStrip8,
                                ChannelType::LeftStrip9,
                                ChannelType::LeftStrip10,
                                ChannelType::LeftStrip11,
                                ChannelType::LeftStrip12,
                                ChannelType::LeftStrip13,
                                ChannelType::LeftStrip14,
                                ChannelType::LeftStrip15,
                                ChannelType::RightStrip0,
                                ChannelType::RightStrip1,
                                ChannelType::RightStrip2,
                                ChannelType::RightStrip3,
                                ChannelType::RightStrip4,
                                ChannelType::RightStrip5,
                                ChannelType::RightStrip6,
                                ChannelType::RightStrip7,
                                ChannelType::RightStrip8,
                                ChannelType::RightStrip9,
                                ChannelType::RightStrip10,
                                ChannelType::RightStrip11,
                                ChannelType::RightStrip12,
                                ChannelType::RightStrip13,
                                ChannelType::RightStrip14,
                                ChannelType::RightStrip15,
                                ChannelType::RF,
                                ChannelType::Strip0,
                                ChannelType::Strip17,
                                ChannelType::None,
                            ] {
                                ui.selectable_value(channel_type, variant, variant.as_ref());
                            }
                        });
                        ui.end_row();
                    }
                });
                ui.add_space(1.0);
        });
    }
}

#[derive(Debug)]
pub enum ChannelMapError {
    IOError(std::io::Error),
    ParseError(ParseIntError),
}

impl From<std::io::Error> for ChannelMapError {
    fn from(e: std::io::Error) -> Self {
        ChannelMapError::IOError(e)
    }
}

impl From<ParseIntError> for ChannelMapError {
    fn from(e: ParseIntError) -> Self {
        ChannelMapError::ParseError(e)
    }
}

impl std::fmt::Display for ChannelMapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelMapError::IOError(x) => {
                write!(f, "Channel map had an error with the input file: {}", x)
            }
            ChannelMapError::ParseError(x) => write!(
                f,
                "Channel map had an error parsing the channel map file: {}",
                x
            ),
        }
    }
}

impl std::error::Error for ChannelMapError {}

#[derive(Debug, Clone)]
pub struct ChannelData {
    pub channel_type: ChannelType,
}

impl Default for ChannelData {
    fn default() -> Self {
        ChannelData {
            channel_type: ChannelType::None,
        }
    }
}

#[derive(Debug)]
pub struct ChannelMap {
    map: HashMap<u32, ChannelData>,
}

impl ChannelMap {
    pub fn new(boards: &[Board]) -> ChannelMap {
        let mut cmap = ChannelMap {
            map: HashMap::new(),
        };
        for board in boards.iter() {
            for (channel_index, channel) in board.channels.iter().enumerate() {
                let data = ChannelData {
                    channel_type: *channel,
                };

                cmap.map.insert(
                    generate_board_channel_uuid(&(board.id), &(channel_index as u32)),
                    data,
                );
            }
        }
        cmap
    }

    pub fn get_channel_data(&self, uuid: &u32) -> Option<&ChannelData> {
        self.map.get(uuid)
    }

    // Check if a channel type is present in the channel map
    pub fn contains_channel_type(&self, channel_type: ChannelType) -> bool {
        self.map
            .values()
            .any(|data| data.channel_type == channel_type)
    }
}
