// POST http://127.0.0.1:8080/api/v1/locate
// -H "Authorization: Bearer <SECRET-TOKEN>"

interface locate_request {
    timestamp: number,          // Milliseconds count since 1970-01-01 00:00:00 UTC 
    device_id: string,          // Device unique identifier (IMEI, UUID, etc.)
    gnss?: GNSS,                // Location measured by GNSS
    wifi?: WiFi[],              // WiFi access points observed
    cell?: {                    // Mobile base station observed
        gsm?: GSM[],            // 2G base stations observed
        wcdma?: WCDMA[],        // 3G base stations observed
        lte?: LTE[],            // 4G base stations observed
        nr?: NR[],              // 5G base stations observed
    },
}

interface locate_response {
    location: {                 // Predicted location
        longitude: number,      // Longitude (WGS84), deg 
        latitude: number,       // Latitude (WGS84), deg 
        altitude?: number,      // Altitude above sea level, m
    },
    accuracy: number,           // Prediction accuracy, m
}

// POST http://127.0.0.1:8080/api/v1/report
// -H "Authorization: Bearer <SECRET-TOKEN>"
interface report_request {
    items: {
        timestamp: number,      // Measurement timestamp, ms
        device_id: string,      // Device unique identifier (IMEI, UUID, etc.)
        gnss: GNSS,             // Location measured by GNSS
        wifi?: WiFi[],          // WiFi access points observed
        cell?: {                // Mobile base station observed
            gsm?: GSM[],        // 2G base stations observed
            wcdma?: WCDMA[],    // 3G base stations observed
            lte?: LTE[],        // 4G base stations observed
            nr?: NR[],          // 5G base stations observed
        },
    }[]
}

type GNSS = {
    longitude: number,      // Longitude (WGS84), deg 
    latitude: number,       // Latitude (WGS84), deg 
    altitude?: number,      // Altitude above sea level, m
    accuracy?: number,      // Indicative accuracy, m
    bearing?: number,       // Moving direction (azimuth), deg
    speed?: number,         // Moving speed, m/s
}

type WiFi = {
    mac: string,            // BSSID formatted as xx:xx:xx:xx:xx:xx or as xxxxxxxxxxxx
    rssi: number            // Received Signal Strength Indicator, dBm
    ssid?: string           // SSID (ESSID)
    channel?: number,       // Frequency channel number (1...233)
    frequency?: number,     // Signal frequency, MHz
    snr?: number            // Signal-to-Noise Ratio, dB
    bandwidth?: number      // Channel bandwidth, MHz
    age?: number            // Measurement age relative to the timestamp, ms
}

type GSM = {
    mcc: number,        // Mobile Country Code (1...999))
    mnc: number,        // Mobile Network Code (1...999)
    lac: number,        // Location Area Code (16 bit unsigned integer)
    ci: number,         // Cell Identity (16 bit unsigned integer)
    rxlev: number       // Received Signal Level, dBm 
    age?: number        // Measurement age relative to the timestamp, ms
    bsic?: number       // Base Station Identity Code (6 bit unsigned integer)
    arfcn?: number,     // Absolute Radio Frequency Channel Number (1...1024)
    ta?: number,        // Timing advance, ms
}

type WCDMA = {
    mcc: number,        // Mobile Country Code (1...999))
    mnc: number,        // Mobile Network Code (1...999)
    lac: number,        // Location Area Code (16 bit unsigned integer)
    ci: number,         // Cell Identity (28 bit unsigned integer)
    rscp: number        // Received Signal Code Power, dBm 
    age?: number        // Measurement age relative to the timestamp, ms
    psc?: number        // Primary Scrambling Code (9 bit unsigned integer)
    uarfcn?: number,    // UTRA Absolute Radio Frequency Channel Number (1...16383)
}

type LTE = {
    mcc: number,        // Mobile Country Code (1...999))
    mnc: number,        // Mobile Network Code (1...999)
    tac: number,        // Tracking Area Code (16 bit unsigned integer)
    eci: number,        // E-UTRAN Cell Identity (28 bit unsigned integer)
    rsrp: number,       // Received Signal Received Power, dBm 
    age?: number,       // Measurement age relative to the timestamp, ms
    rsrq?: number,      // Reference Signal Received Quality, dB
    pci?: number,       // Physical Cell Identity (0...503)
    earfcn?: number,    // E-UTRAN Absolute Radio Frequency Channel Number (16 bit unsigned integer)
    ta?: number,        // Timing advance, ms
}

type NR = {
    mcc: number,        // Mobile Country Code (1...999))
    mnc: number,        // Mobile Network Code (1...999)
    tac: number,        // Tracking Area Code (24 bit integer)
    nci: number,        // NR Cell Identity (36 bit integer)
    ss_rsrp: number,    // Synchronization Signal Reference Signal Received Power, dBm 
    age?: number,       // Measurement age relative to the timestamp, ms
    rsrq?: number,      // Reference Signal Received Quality, dB
    pci?: number,       // Physical Cell Identity (0...503)
    arcfn?: number,     // NR Absolute Radio Frequency Channel Number
    ssbi?: number,      // Synchronization Signal Block Index (8 bit unsigned integer)
}
