//! Deterministic 1-D k-means over min-max normalised order sizes, after
//! binance_l3_est's `kmeans.rs` (quantile initialisation, labels ranked by
//! centroid so label 0 is the smallest-size cluster). Full-batch Lloyd
//! iterations replace upstream's random mini-batches, so a frame's colouring
//! depends only on the book.

pub const K: usize = 10;
const MAX_ITER: usize = 50;

#[derive(Default)]
pub struct KMeans {
    points: Vec<f64>,
    sorted: Vec<f64>,
}

impl KMeans {
    /// Writes each value's cluster rank (`0..K`, by ascending centroid) into `labels`.
    pub fn fit(&mut self, values: impl Iterator<Item = f64>, labels: &mut Vec<u8>) {
        self.points.clear();
        self.points.extend(values);
        labels.clear();
        let n = self.points.len();
        if n == 0 {
            return;
        }
        let (min, max) = self
            .points
            .iter()
            .fold((f64::MAX, f64::MIN), |(lo, hi), &q| (lo.min(q), hi.max(q)));
        if max > min {
            let inv = 1.0 / (max - min);
            for q in &mut self.points {
                *q = (*q - min) * inv;
            }
        }

        self.sorted.clear();
        self.sorted.extend_from_slice(&self.points);
        self.sorted.sort_unstable_by(f64::total_cmp);
        let mut centroids = [0.0; K];
        for (i, c) in centroids.iter_mut().enumerate() {
            *c = self.sorted[i * (n - 1) / (K - 1)];
        }

        labels.resize(n, 0);
        for iter in 0..MAX_ITER {
            let mut sums = [0.0; K];
            let mut counts = [0u32; K];
            let mut changed = false;
            for (q, label) in self.points.iter().zip(labels.iter_mut()) {
                let c = nearest(&centroids, *q);
                changed |= *label != c;
                *label = c;
                sums[c as usize] += q;
                counts[c as usize] += 1;
            }
            if iter > 0 && !changed {
                break;
            }
            for ((c, sum), count) in centroids.iter_mut().zip(sums).zip(counts) {
                if count > 0 {
                    *c = sum / f64::from(count);
                }
            }
        }

        let mut order: [u8; K] = std::array::from_fn(|i| i as u8);
        order.sort_by(|&a, &b| centroids[a as usize].total_cmp(&centroids[b as usize]));
        let mut rank = [0u8; K];
        for (r, &c) in order.iter().enumerate() {
            rank[c as usize] = r as u8;
        }
        for label in labels.iter_mut() {
            *label = rank[*label as usize];
        }
    }
}

fn nearest(centroids: &[f64; K], q: f64) -> u8 {
    let mut best = (f64::INFINITY, 0u8);
    for (i, c) in centroids.iter().enumerate() {
        let d = (q - c).abs();
        if d < best.0 {
            best = (d, i as u8);
        }
    }
    best.1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separates_sizes_and_ranks_labels() {
        let mut km = KMeans::default();
        let mut labels = Vec::new();
        let values = [1.0, 1.1, 0.9, 50.0, 51.0, 1.0, 100.0];
        km.fit(values.iter().copied(), &mut labels);
        assert_eq!(labels.len(), values.len());
        // Ranks follow size: a bigger order never gets a smaller label.
        for (a, la) in values.iter().zip(&labels) {
            for (b, lb) in values.iter().zip(&labels) {
                if a < b {
                    assert!(la <= lb, "{a}→{la} vs {b}→{lb}");
                }
            }
        }
        assert_eq!(labels[0], labels[5]);
        assert!(labels[3] > labels[1]);
        assert_eq!(labels[6], (K - 1) as u8);
    }

    #[test]
    fn empty_and_constant_inputs() {
        let mut km = KMeans::default();
        let mut labels = vec![3];
        km.fit(std::iter::empty(), &mut labels);
        assert!(labels.is_empty());
        km.fit([2.0; 5].into_iter(), &mut labels);
        assert!(labels.iter().all(|&l| l == labels[0]));
    }
}
