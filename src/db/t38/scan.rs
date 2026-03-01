#![allow(unused)]

use std::io;
use std::time::Duration;

use clusters::Proximity;
use log::error;
use redis::RedisError;
use serde::{Deserialize, Serialize};

use super::{COUNT_ATTEMPTS_RUN_CMD, ERROR_ID_NOT_FOUND, ERROR_KEY_NOT_FOUND, TIMEOUT};
use crate::{
    constants::{Collection, MAX_DISTANCE_REPORT_LBS},
    db::{
        blobasaur::{set_ba_lbs_yandex_cell_one, set_ba_lbs_yandex_wifi_one, set_ba_wifi_one},
        pg::transmitter::TransmitterLocation,
        t38::{del_wifi_one, del_yandex_lbs_wifi_one, get_wifi_one, get_yandex_lbs_wifi_one},
    },
    lbs::yandex::wifi::YandexLbsResponse,
    services::locate::dbscan::Point,
    tasks::{
        blobasaur::BAConnectionManageMessage,
        t38::{T38ConnectionManageMessage, get_connection},
    },
};

pub async fn scan_yandex_wifi(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    collection: &str,
    limit: u64,
) -> Result<(), RedisError> {
    let cmd_arg = redis::cmd("SCAN")
        .arg(collection)
        .arg("LIMIT")
        .arg(limit)
        .to_owned();

    let mut connection = get_connection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    loop {
        match cmd_arg
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
        {
            Err(e) => {
                if e.to_string().contains(ERROR_ID_NOT_FOUND)
                    || e.to_string().contains(ERROR_KEY_NOT_FOUND)
                {
                    // id was not found in the tile38 database.
                    return Err(e);
                }

                if e.category() == "busy loading" {
                    error!("Tile38 is unavailable because it is loading the dataset into memory");
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "Unable to connect to the Tile38 store",
                    )
                    .into());
                }

                if i > COUNT_ATTEMPTS_RUN_CMD {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        format!("Unable to connect to the Redis store: {}", e.to_string()),
                    )
                    .into());
                }

                tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

                match get_connection(tx_t38_conn.clone(), Some(e.to_string())).await {
                    Err(e) => {
                        error!("get_connecton: {}", e);
                        continue;
                    }
                    Ok(c) => {
                        connection = c;
                    }
                }
                i += 1;
            }
            Ok(values) => {
                let mut i2 = 0;
                let mut all_count = 0;
                for v in values {
                    match v {
                        redis::Value::Array(array) => {
                            for v2 in array {
                                match v2 {
                                    redis::Value::Array(array2) => {
                                        for v3 in array2 {
                                            if let redis::Value::Array(v4) = v3 {
                                                if v4.len() < 4 {
                                                    continue;
                                                }
                                                if let redis::Value::BulkString(s) = &v4[3] {
                                                    let mac =
                                                        String::from_utf8(s.to_vec()).unwrap();
                                                    if let redis::Value::BulkString(s1) = &v4[1] {
                                                        let ylr_str =
                                                            String::from_utf8(s1.to_vec()).unwrap();
                                                        let ylr: YandexLbsResponse =
                                                            serde_json::from_str(&ylr_str).unwrap();

                                                        if let Err(e) = set_ba_lbs_yandex_wifi_one(
                                                            tx_ba_conn.clone(),
                                                            Collection::BaLbsYandexWifi.as_ref(),
                                                            ylr,
                                                            &mac,
                                                        )
                                                        .await
                                                        {
                                                            println!(
                                                                "ERROR save YandexLbsResponse to blobasaur"
                                                            );
                                                        }
                                                        i2 += 1;

                                                        // if ylr.location.accuracy == 150.0 {
                                                        //     println!("mac: {}", mac);
                                                        //     println!("ylr: {:?}", ylr);
                                                        //     del_yandex_lbs_wifi_one(
                                                        //         tx_t38_conn.clone(),
                                                        //         "lbs:yandex:wifi",
                                                        //         &mac,
                                                        //     )
                                                        //     .await;
                                                        // }

                                                        // if true {
                                                        //     all_count += 1;
                                                        //     let ps = format!(
                                                        //         "{}",
                                                        //         ylr.location.point.lat
                                                        //     );
                                                        //     let ps2 = format!(
                                                        //         "{}",
                                                        //         ylr.location.point.lon
                                                        //     );
                                                        //     if ps.len() <= 9 || ps2.len() <= 9 {
                                                        //         // DEBUG
                                                        //         // TODO: remove after tests by Whoosh
                                                        //         println!("mac: {}", mac);
                                                        //         println!(
                                                        //             "ylr: {:?}",
                                                        //             ylr.location.point
                                                        //         );
                                                        //         del_wifi_one(
                                                        //             tx_t38_conn.clone(),
                                                        //             "wifi",
                                                        //             &mac,
                                                        //         )
                                                        //         .await;
                                                        //         del_yandex_lbs_wifi_one(
                                                        //             tx_t38_conn.clone(),
                                                        //             "lbs:yandex:wifi",
                                                        //             &mac,
                                                        //         )
                                                        //         .await;
                                                        //         i2 += 1;
                                                        //     }
                                                        // }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
                // DEBUG
                // TODO: remove after tests by Whoosh
                // println!("all count lbs wifi: {}", all_count);
                // println!("count del lbs wifi: {}", i2);

                println!("count upload lbs wifi: {}", i2);
                return Ok(());
            }
        }
    }
}

pub async fn scan_wifi(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    collection: &str,
    limit: u64,
) -> Result<(), RedisError> {
    let cmd_arg = redis::cmd("SCAN")
        .arg(collection)
        .arg("LIMIT")
        .arg(limit)
        .to_owned();

    let mut connection = get_connection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    loop {
        match cmd_arg
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
        {
            Err(e) => {
                if e.to_string().contains(ERROR_ID_NOT_FOUND)
                    || e.to_string().contains(ERROR_KEY_NOT_FOUND)
                {
                    // id was not found in the tile38 database.
                    return Err(e);
                }

                if e.category() == "busy loading" {
                    error!("Tile38 is unavailable because it is loading the dataset into memory");
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "Unable to connect to the Tile38 store",
                    )
                    .into());
                }

                if i > COUNT_ATTEMPTS_RUN_CMD {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        format!("Unable to connect to the Redis store: {}", e.to_string()),
                    )
                    .into());
                }

                tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

                match get_connection(tx_t38_conn.clone(), Some(e.to_string())).await {
                    Err(e) => {
                        error!("get_connecton: {}", e);
                        continue;
                    }
                    Ok(c) => {
                        connection = c;
                    }
                }
                i += 1;
            }
            Ok(values) => {
                let mut i = 0;
                for v in values {
                    match v {
                        redis::Value::Array(array) => {
                            for v2 in array {
                                match v2 {
                                    redis::Value::Array(array2) => {
                                        for v3 in array2 {
                                            if let redis::Value::Array(v4) = v3 {
                                                if v4.len() < 4 {
                                                    continue;
                                                }
                                                if let redis::Value::BulkString(s) = &v4[3] {
                                                    let mac =
                                                        String::from_utf8(s.to_vec()).unwrap();
                                                    if let redis::Value::BulkString(s1) = &v4[1] {
                                                        let tl_str =
                                                            String::from_utf8(s1.to_vec()).unwrap();
                                                        let tl: TransmitterLocation =
                                                            serde_json::from_str(&tl_str).unwrap();

                                                        if let Err(e) = set_ba_wifi_one(
                                                            tx_ba_conn.clone(),
                                                            Collection::BaWifi.as_ref(),
                                                            &tl,
                                                        )
                                                        .await
                                                        {
                                                            println!(
                                                                "ERROR save TransmitterLocation to blobasaur"
                                                            );
                                                        }
                                                        i += 1;

                                                        // if tl.lat >= 43.7
                                                        //     && tl.lat <= 44.999
                                                        //     && tl.lon >= 36.999
                                                        //     && tl.lon <= 38.111
                                                        // {
                                                        //     // DEBUG
                                                        //     // TODO: remove after tests by Whoosh
                                                        //     println!("mac: {}", mac);
                                                        //     println!("tl: {:?}", tl);
                                                        //     del_wifi_one(
                                                        //         tx_t38_conn.clone(),
                                                        //         "wifi",
                                                        //         &mac,
                                                        //     )
                                                        //     .await;
                                                        //     del_yandex_lbs_wifi_one(
                                                        //         tx_t38_conn.clone(),
                                                        //         "lbs:yandex:wifi",
                                                        //         &mac,
                                                        //     )
                                                        //     .await;
                                                        //     i += 1;
                                                        // }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
                // DEBUG
                // TODO: remove after tests by Whoosh
                println!("count upload Whoosh wifi: {}", i);
                return Ok(());
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DataCell {
    pub data: String,
}

pub async fn scan_yandex_cell(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    tx_ba_conn: flume::Sender<BAConnectionManageMessage>,
    collection: &str,
    limit: u64,
) -> Result<(), RedisError> {
    let cmd_arg = redis::cmd("SCAN")
        .arg(collection)
        .arg("LIMIT")
        .arg(limit)
        .to_owned();

    let mut connection = get_connection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    loop {
        match cmd_arg
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
        {
            Err(e) => {
                if e.to_string().contains(ERROR_ID_NOT_FOUND)
                    || e.to_string().contains(ERROR_KEY_NOT_FOUND)
                {
                    // id was not found in the tile38 database.
                    return Err(e);
                }

                if e.category() == "busy loading" {
                    error!("Tile38 is unavailable because it is loading the dataset into memory");
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "Unable to connect to the Tile38 store",
                    )
                    .into());
                }

                if i > COUNT_ATTEMPTS_RUN_CMD {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        format!("Unable to connect to the Redis store: {}", e.to_string()),
                    )
                    .into());
                }

                tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

                match get_connection(tx_t38_conn.clone(), Some(e.to_string())).await {
                    Err(e) => {
                        error!("get_connecton: {}", e);
                        continue;
                    }
                    Ok(c) => {
                        connection = c;
                    }
                }
                i += 1;
            }
            Ok(values) => {
                let mut i2 = 0;
                let mut all_count = 0;
                for v in values {
                    match v {
                        redis::Value::Array(array) => {
                            for v2 in array {
                                match v2 {
                                    redis::Value::Array(array2) => {
                                        if let redis::Value::BulkString(s) = &array2[0] {
                                            let cell_code = String::from_utf8(s.to_vec()).unwrap();
                                            if let redis::Value::BulkString(s1) = &array2[1] {
                                                let ylr_str =
                                                    String::from_utf8(s1.to_vec()).unwrap();

                                                let dc: DataCell =
                                                    serde_json::from_str(&ylr_str).unwrap();
                                                let ylr: YandexLbsResponse =
                                                    serde_json::from_str(&dc.data).unwrap();

                                                if let Err(e) = set_ba_lbs_yandex_cell_one(
                                                    tx_ba_conn.clone(),
                                                    Collection::BaLbsYandexCell.as_ref(),
                                                    ylr,
                                                    &cell_code,
                                                )
                                                .await
                                                {
                                                    println!(
                                                        "ERROR save YandexLbsResponse to blobasaur"
                                                    );
                                                }
                                                i2 += 1;
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }

                println!("count upload lbs cell: {}", i2);
                return Ok(());
            }
        }
    }
}

pub async fn scan_clear_wifi(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    limit: u64,
) -> Result<(), RedisError> {
    let cmd_arg = redis::cmd("SCAN")
        .arg(collection)
        .arg("LIMIT")
        .arg(limit)
        .to_owned();

    let mut connection = get_connection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    loop {
        match cmd_arg
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
        {
            Err(e) => {
                if e.to_string().contains(ERROR_ID_NOT_FOUND)
                    || e.to_string().contains(ERROR_KEY_NOT_FOUND)
                {
                    // id was not found in the tile38 database.
                    return Err(e);
                }

                if e.category() == "busy loading" {
                    error!("Tile38 is unavailable because it is loading the dataset into memory");
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "Unable to connect to the Tile38 store",
                    )
                    .into());
                }

                if i > COUNT_ATTEMPTS_RUN_CMD {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        format!("Unable to connect to the Redis store: {}", e.to_string()),
                    )
                    .into());
                }

                tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

                match get_connection(tx_t38_conn.clone(), Some(e.to_string())).await {
                    Err(e) => {
                        error!("get_connecton: {}", e);
                        continue;
                    }
                    Ok(c) => {
                        connection = c;
                    }
                }
                i += 1;
            }
            Ok(values) => {
                let mut i = 0;
                for v in values {
                    match v {
                        redis::Value::Array(array) => {
                            for v2 in array {
                                match v2 {
                                    redis::Value::Array(array2) => {
                                        for v3 in array2 {
                                            if let redis::Value::Array(v4) = v3 {
                                                if v4.len() < 4 {
                                                    continue;
                                                }
                                                if let redis::Value::BulkString(s) = &v4[3] {
                                                    let mac =
                                                        String::from_utf8(s.to_vec()).unwrap();
                                                    if let redis::Value::BulkString(s1) = &v4[1] {
                                                        let tl_str =
                                                            String::from_utf8(s1.to_vec()).unwrap();
                                                        let tl: TransmitterLocation =
                                                            serde_json::from_str(&tl_str).unwrap();

                                                        match get_yandex_lbs_wifi_one(
                                                            tx_t38_conn.clone(),
                                                            "lbs:yandex:wifi",
                                                            &mac,
                                                        )
                                                        .await
                                                        {
                                                            Err(_e) => {}
                                                            Ok(w) => {
                                                                if let Some(ylr) = w {
                                                                    let p_yandex = Point {
                                                                        id: 0,
                                                                        lat: ylr.location.point.lat,
                                                                        lon: ylr.location.point.lon,
                                                                    };
                                                                    let p_tl = Point {
                                                                        id: 1,
                                                                        lat: tl.lat,
                                                                        lon: tl.lon,
                                                                    };
                                                                    let d =
                                                                        p_yandex.distance(&p_tl);
                                                                    if d > MAX_DISTANCE_REPORT_LBS {
                                                                        // DEBUG
                                                                        // TODO: remove after tests by Whoosh
                                                                        println!("d mac: {}", mac);
                                                                        println!("d tl: {:?}", tl);
                                                                        del_wifi_one(
                                                                            tx_t38_conn.clone(),
                                                                            "wifi",
                                                                            &mac,
                                                                        )
                                                                        .await;
                                                                        i += 1;
                                                                    }
                                                                } else {
                                                                    // DEBUG
                                                                    // TODO: remove after tests by Whoosh
                                                                    println!("none mac: {}", mac);
                                                                    println!("none tl: {:?}", tl);
                                                                    del_wifi_one(
                                                                        tx_t38_conn.clone(),
                                                                        "wifi",
                                                                        &mac,
                                                                    )
                                                                    .await;
                                                                    i += 1;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
                // DEBUG
                // TODO: remove after tests by Whoosh
                println!("count del wifi: {}", i);
                return Ok(());
            }
        }
    }
}

pub async fn scan_ids(
    tx_t38_conn: flume::Sender<T38ConnectionManageMessage>,
    collection: &str,
    limit: u64,
) -> Result<Vec<String>, RedisError> {
    let cmd_arg = redis::cmd("SCAN")
        .arg(collection)
        .arg("LIMIT")
        .arg(limit)
        .arg("IDS")
        .to_owned();

    let mut connection = get_connection(tx_t38_conn.clone(), None).await.unwrap();
    let mut i: u16 = 0;

    loop {
        match cmd_arg
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
        {
            Err(e) => {
                if e.to_string().contains(ERROR_ID_NOT_FOUND)
                    || e.to_string().contains(ERROR_KEY_NOT_FOUND)
                {
                    // id was not found in the tile38 database.
                    return Err(e);
                }

                if e.category() == "busy loading" {
                    error!("Tile38 is unavailable because it is loading the dataset into memory");
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "Unable to connect to the Tile38 store",
                    )
                    .into());
                }

                if i > COUNT_ATTEMPTS_RUN_CMD {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        format!("Unable to connect to the Redis store: {}", e.to_string()),
                    )
                    .into());
                }

                tokio::time::sleep(Duration::from_secs(TIMEOUT)).await;

                match get_connection(tx_t38_conn.clone(), Some(e.to_string())).await {
                    Err(e) => {
                        error!("get_connecton: {}", e);
                        continue;
                    }
                    Ok(c) => {
                        connection = c;
                    }
                }
                i += 1;
            }
            Ok(values) => {
                for v in values {
                    match v {
                        redis::Value::Array(array) => {
                            let mut ids = Vec::with_capacity(array.len());
                            for id_value in array {
                                match id_value {
                                    redis::Value::BulkString(bulk_string) => {
                                        let id = String::from_utf8(bulk_string).unwrap();
                                        ids.push(id);
                                    }
                                    _ => {}
                                }
                            }
                            return Ok(ids);
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        services::locate::dbscan::Point,
        tasks::t38::{T38ConnectionManageMessage, connection_manage_task},
    };
    use clusters::Proximity;

    async fn connection_manager() -> flume::Sender<T38ConnectionManageMessage> {
        let (tx_t38_conn, rx_t38_conn) = flume::unbounded::<T38ConnectionManageMessage>();
        let _connection_manage_t38_handle =
            connection_manage_task(rx_t38_conn, tx_t38_conn.clone())
                .await
                .unwrap();
        tx_t38_conn
    }

    #[tokio::test]
    async fn test_scan_ids() {
        let tx_t38_conn = connection_manager().await;

        let limit = 3;
        match super::scan_ids(tx_t38_conn.clone(), "lbs:yandex:wifi", limit).await {
            Err(e) => {
                println!("error scan_ids: {}", e);
            }
            Ok(ids) => {
                let p_moscow = Point {
                    id: 0,
                    lat: 55.758608,
                    lon: 37.616500,
                };
                for id in ids {
                    match crate::db::t38::get_yandex_lbs_wifi_one(
                        tx_t38_conn.clone(),
                        "lbs:yandex:wifi",
                        &id,
                    )
                    .await
                    {
                        Err(e) => {
                            println!("error get_yandex_lbs_wifi_one: {}", e);
                        }
                        Ok(ylr_opt) => {
                            if let Some(ylr) = ylr_opt {
                                let p_yandex = Point {
                                    id: 1,
                                    lat: ylr.location.point.lat,
                                    lon: ylr.location.point.lon,
                                };
                                let d = p_yandex.distance(&p_moscow);
                                if d > 120000.0 {
                                    let id_no_colon = id.replace(":", "");
                                    println!(
                                        "MAC: {} / {}, coordinates: ({},{}), d: {}",
                                        id, id_no_colon, p_yandex.lat, p_yandex.lon, d
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
