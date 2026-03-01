use strum_macros::{AsRefStr, Display, EnumString, IntoStaticStr};

// 2 for outside, 3-5 inside
// based on https://codeberg.org/Locator/Locator/issues/31#issuecomment-3098830
pub const SIGNAL_DROP_COEFFICIENT: f64 = 3.0;

// RSSI at 1m from AP, used to estimate accuracy
pub const BASE_RSSI: f64 = -30.0;
// default RSSI
pub const DEFAULT_RSSI: f64 = -90.0;

pub const WIFI_SSID_IGNORED: &[&str] = &["carcam"];
pub const MAX_SCOOTER_SPEED: f64 = 25.0; // 25 km/h = 7 m/s
pub const MAX_DISTANCE: f64 = 60_000.0; // meters

pub const MAX_DISTANCE_REPORT_LBS: f64 = 70.0; // meters

pub const GPS_VALID_DISTANCE_BY_WIFI: f64 = 50.0; // meters
pub const GPS_VALID_DISTANCE_BY_CELL: f64 = 500.0; // meters

pub const FALLBACK_EPSILON_CLUSTER: f64 = 150.0; // meters
pub const FALLBACK_LOCATE_DISTANCE: f64 = 2500.0; // meters

pub const HOUR: u64 = 3600; // seconds

pub const PRESAVED_PARTITIONS_COUNT: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr)]
pub enum HC {
    #[strum(serialize = "surf")]
    Surf,
    #[strum(serialize = "reqwest")]
    Reqwest,
}

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr)]
pub enum Collection {
    #[strum(serialize = "cell")]
    Cell,
    #[strum(serialize = "wifi")]
    Wifi,
    #[strum(serialize = "bluetooth")]
    Bluetooth,

    // Access points from Yandex Locator
    #[strum(serialize = "lbs:yandex:wifi")]
    LbsYandexWifi,
    // Access points from Yandex Locator
    #[strum(serialize = "lbs:yandex:cell")]
    LbsYandexCell,

    // Access points from Yandex Locator
    #[strum(serialize = "lbs:yandex:wifi:missing")]
    LbsYandexWifiMissing,
    // Access points from Yandex Locator
    #[strum(serialize = "lbs:yandex:cell:missing")]
    LbsYandexCellMissing,

    // Whoosh scooters
    #[strum(serialize = "device:whoosh")]
    DeviceWhoosh,

    // Blobasaur namespaces
    #[strum(serialize = "whoosh_wifi")]
    BaWifi,
    #[strum(serialize = "lbs_yandex_wifi")]
    BaLbsYandexWifi,
    #[strum(serialize = "lbs_yandex_cell")]
    BaLbsYandexCell,
}

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr)]
pub enum RadioType {
    #[strum(serialize = "gsm")]
    Gsm,
    #[strum(serialize = "wcdma")]
    Wcdma,
    #[strum(serialize = "lte")]
    Lte,
    #[strum(serialize = "nr")]
    Nr,
}

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr)]
pub enum T38RoleName {
    #[strum(serialize = "master")]
    Master,
    #[strum(serialize = "slave")]
    Slave,
}

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr)]
pub enum T38StateName {
    #[strum(serialize = "active")]
    Active,
    #[strum(serialize = "inactive")]
    Inactive,
}
