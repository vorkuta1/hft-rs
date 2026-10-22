use std::collections::BTreeMap;
use std::collections::btree_map;
use std::iter::Rev;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};

/// Price in integer ticks (`price * 10^px_decimals`).
pub type Px = u64;
/// Quantity in integer lots (`qty * 10^qty_decimals`).
pub type Qty = u64;
/// Exchange timestamp in microseconds since the Unix epoch.
pub type Ts = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub const BOTH: [Side; 2] = [Side::Bid, Side::Ask];
}

/// Account that placed an order (L4 venues only), e.g. a Hyperliquid address.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Owner(pub [u8; 20]);

impl Owner {
    /// Parses a `0x`-prefixed 40-hex-digit address.
    pub fn parse(raw: &str) -> Result<Self> {
        let hex = raw.strip_prefix("0x").unwrap_or(raw);
        if hex.len() != 40 {
            bail!("address {raw:?} is not 20 bytes");
        }
        let mut out = [0u8; 20];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
                .map_err(|_| anyhow!("address {raw:?} is not hex"))?;
        }
        Ok(Self(out))
    }
}

impl std::fmt::Display for Owner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("0x")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Owner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Serialize for Owner {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

/// Iterates one side of a price-keyed book from the touch outward.
pub enum BestFirst<'a, V> {
    Bids(Rev<btree_map::Iter<'a, Px, V>>),
    Asks(btree_map::Iter<'a, Px, V>),
}

impl<'a, V> BestFirst<'a, V> {
    pub(crate) fn new(side: Side, bids: &'a BTreeMap<Px, V>, asks: &'a BTreeMap<Px, V>) -> Self {
        match side {
            Side::Bid => Self::Bids(bids.iter().rev()),
            Side::Ask => Self::Asks(asks.iter()),
        }
    }
}

impl<'a, V> Iterator for BestFirst<'a, V> {
    type Item = (Px, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Bids(it) => it.next().map(|(px, v)| (*px, v)),
            Self::Asks(it) => it.next().map(|(px, v)| (*px, v)),
        }
    }
}

/// Fixed-point scale used to turn decimal strings into exact integers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scale {
    pub px_decimals: u32,
    pub qty_decimals: u32,
}

impl Scale {
    pub fn px(&self, raw: &str) -> Result<Px> {
        parse_fixed(raw, self.px_decimals)
    }

    pub fn qty(&self, raw: &str) -> Result<Qty> {
        parse_fixed(raw, self.qty_decimals)
    }

    pub fn px_f64(&self, px: Px) -> f64 {
        px as f64 / 10f64.powi(self.px_decimals as i32)
    }

    pub fn qty_f64(&self, qty: Qty) -> f64 {
        qty as f64 / 10f64.powi(self.qty_decimals as i32)
    }
}

/// Parses a non-negative decimal string into an integer scaled by `10^decimals`.
/// Digits beyond `decimals` must be zero, so the conversion is always exact.
pub fn parse_fixed(raw: &str, decimals: u32) -> Result<u64> {
    let (int_part, frac_part) = raw.split_once('.').unwrap_or((raw, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        bail!("empty decimal");
    }
    let mut value: u64 = 0;
    for b in int_part.bytes() {
        value = push_digit(value, b, raw)?;
    }
    let mut seen = 0;
    for b in frac_part.bytes() {
        if seen < decimals {
            value = push_digit(value, b, raw)?;
            seen += 1;
        } else if b != b'0' {
            bail!("{raw:?} has more than {decimals} significant decimals");
        }
    }
    for _ in seen..decimals {
        value = value
            .checked_mul(10)
            .ok_or_else(|| anyhow!("{raw:?} overflows u64"))?;
    }
    Ok(value)
}

fn push_digit(acc: u64, b: u8, raw: &str) -> Result<u64> {
    if !b.is_ascii_digit() {
        bail!("invalid decimal {raw:?}");
    }
    acc.checked_mul(10)
        .and_then(|v| v.checked_add(u64::from(b - b'0')))
        .ok_or_else(|| anyhow!("{raw:?} overflows u64"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_fixed_is_exact_and_rejects_lossy_input() {
        assert_eq!(parse_fixed("121789.44000000", 2).unwrap(), 12_178_944);
        assert_eq!(parse_fixed("0.00400000", 8).unwrap(), 400_000);
        assert_eq!(parse_fixed("7", 2).unwrap(), 700);
        assert_eq!(parse_fixed(".5", 1).unwrap(), 5);
        assert!(parse_fixed("1.005", 2).is_err());
        assert!(parse_fixed("-1", 2).is_err());
        assert!(parse_fixed("1e5", 2).is_err());
        assert!(parse_fixed("", 2).is_err());
    }
}
