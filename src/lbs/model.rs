use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::constants::RadioType;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Cell {
    pub lte: Option<Vec<Lte>>,
    pub wcdma: Option<Vec<Wcdma>>,
    pub gsm: Option<Vec<Gsm>>,
    pub nr: Option<Vec<Nr>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CellMeasurement {
    pub radio_type: String,
    pub mcc: u16,
    pub mnc: u16,
    pub lac: u64,
    pub cid: u64,
    pub signal_strength: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Gsm {
    pub mcc: u16,
    pub mnc: u16,
    pub lac: u64,
    pub ci: u64,
    pub rxlev: f64,
    pub age: Option<i32>,
    pub bsic: Option<u32>,
    pub arfcn: Option<u16>,
    pub ta: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Wcdma {
    pub mcc: u16,
    pub mnc: u16,
    pub lac: u64,
    pub ci: u64,
    pub rscp: f64,
    pub age: Option<i32>,
    pub psc: Option<u32>,
    pub uarfcn: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Lte {
    pub mcc: u16,
    pub mnc: u16,
    pub tac: u64,
    pub eci: u64,
    pub rsrp: f64,
    pub age: Option<i32>,
    pub rsrq: Option<f64>,
    pub pci: Option<u16>,
    pub earfcn: Option<u16>,
    pub ta: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Nr {
    pub mcc: u16,
    pub mnc: u16,
    pub tac: u64,
    pub nci: u64,
    pub ss_rsrp: f64,
    pub age: Option<i32>,
    pub rsrq: Option<f64>,
    pub pci: Option<u16>,
    pub arcfn: Option<u32>,
    pub ssbi: Option<u8>,
}

pub fn create_cell_measurement(cell: &Cell) -> Vec<CellMeasurement> {
    let mut cms = Vec::new();
    if let Some(gsm) = &cell.gsm {
        for g in gsm {
            let cm = CellMeasurement {
                radio_type: RadioType::Gsm.to_string(),
                mcc: g.mcc,
                mnc: g.mnc,
                lac: g.lac,
                cid: g.ci,
                signal_strength: g.rxlev,
            };
            cms.push(cm);
        }
    }
    if let Some(wcdma) = &cell.wcdma {
        for w in wcdma {
            let cm = CellMeasurement {
                radio_type: RadioType::Wcdma.to_string(),
                mcc: w.mcc,
                mnc: w.mnc,
                lac: w.lac,
                cid: w.ci,
                signal_strength: w.rscp,
            };
            cms.push(cm);
        }
    }
    if let Some(lte) = &cell.lte {
        for l in lte {
            let cm = CellMeasurement {
                radio_type: RadioType::Lte.to_string(),
                mcc: l.mcc,
                mnc: l.mnc,
                lac: l.tac,
                cid: l.eci,
                signal_strength: l.rsrp,
            };
            cms.push(cm);
        }
    }
    if let Some(nr) = &cell.nr {
        for n in nr {
            let cm = CellMeasurement {
                radio_type: RadioType::Nr.to_string(),
                mcc: n.mcc,
                mnc: n.mnc,
                lac: n.tac,
                cid: n.nci,
                signal_strength: n.ss_rsrp,
            };
            cms.push(cm);
        }
    }
    cms
}
