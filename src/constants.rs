use strum_macros::{AsRefStr, Display, EnumString, IntoStaticStr};

// 2 for outside, 3-5 inside
// based on https://codeberg.org/Locator/Locator/issues/31#issuecomment-3098830
pub const SIGNAL_DROP_COEFFICIENT: f64 = 3.0;

// RSSI at 1m from AP, used to estimate accuracy
pub const BASE_RSSI: f64 = -30.0;
pub const WIFI_SSID_IGNORED: &'static [&'static str] = &["carcam"];
pub const MAX_SCOOTER_SPEED: f64 = 25.0; // 25 km/h = 7 m/s
pub const MAX_DISTANCE: f64 = 60_000.0; // meters
pub const HOUR: u64 = 3600; // seconds

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
    // Whoosh scooters
    #[strum(serialize = "device:whoosh")]
    DeviceWhoosh,
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
