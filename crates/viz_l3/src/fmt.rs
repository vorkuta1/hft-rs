//! Display formatting for fixed-point prices/quantities, ages and timestamps.

use l3lab::types::{Px, Qty, Scale, Ts};

pub fn px(scale: Scale, px: Px) -> String {
    format!("{:.*}", scale.px_decimals as usize, scale.px_f64(px))
}

/// Exact decimal quantity with trailing zeros trimmed.
pub fn qty(scale: Scale, qty: Qty) -> String {
    let div = 10u64.pow(scale.qty_decimals);
    let (int, frac) = (qty / div, qty % div);
    if frac == 0 {
        return int.to_string();
    }
    let frac = format!("{frac:0width$}", width = scale.qty_decimals as usize);
    format!("{int}.{}", frac.trim_end_matches('0'))
}

pub fn age(us: u64) -> String {
    let ms = us / 1000;
    match ms {
        0..1_000 => format!("{ms} ms"),
        1_000..60_000 => format!("{:.1} s", ms as f64 / 1000.0),
        60_000..3_600_000 => format!("{}m{:02}s", ms / 60_000, ms / 1000 % 60),
        _ => format!("{}h{:02}m", ms / 3_600_000, ms / 60_000 % 60),
    }
}

/// `YYYY-MM-DD HH:MM:SS.mmm UTC` for a microsecond Unix timestamp.
pub fn utc(ts: Ts) -> String {
    let secs = ts / 1_000_000;
    let ms = ts % 1_000_000 / 1000;
    let (y, m, d) = civil_from_days((secs / 86_400) as i64);
    let sod = secs % 86_400;
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{ms:03} UTC",
        sod / 3600,
        sod / 60 % 60,
        sod % 60
    )
}

/// Days since 1970-01-01 → proleptic Gregorian (year, month, day).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}
