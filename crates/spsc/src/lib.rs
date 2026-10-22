//! Bounded single-producer/single-consumer ring buffer.
//!
//! Soundness rests on there being exactly one `Producer` and one `Consumer`:
//! neither handle is `Clone`, and both are `!Sync` so a handle cannot be shared
//! by reference across threads. Each may be moved to its own thread (`Send`).

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Ring<T, const N: usize> {
    buf: [UnsafeCell<Option<T>>; N],
    /// Next slot to write; only the producer stores it.
    head: AtomicUsize,
    /// Next slot to read; only the consumer stores it.
    tail: AtomicUsize,
}

// SAFETY: slot `i` is written only by the producer while `head - tail < N`
// guarantees the consumer is not reading it, and read only by the consumer after
// the Release store of `head` published it. The handle types enforce one of each.
unsafe impl<T: Send, const N: usize> Sync for Ring<T, N> {}

/// `PhantomData<Cell<()>>` makes a handle `Send + !Sync`.
type NotSync = PhantomData<Cell<()>>;

pub struct Producer<T, const N: usize> {
    ring: Arc<Ring<T, N>>,
    _not_sync: NotSync,
}

pub struct Consumer<T, const N: usize> {
    ring: Arc<Ring<T, N>>,
    _not_sync: NotSync,
}

pub fn channel<T, const N: usize>() -> (Producer<T, N>, Consumer<T, N>) {
    assert!(N > 0, "spsc capacity must be non-zero");
    let ring = Arc::new(Ring {
        buf: std::array::from_fn(|_| UnsafeCell::new(None)),
        head: AtomicUsize::new(0),
        tail: AtomicUsize::new(0),
    });
    (
        Producer {
            ring: Arc::clone(&ring),
            _not_sync: PhantomData,
        },
        Consumer {
            ring,
            _not_sync: PhantomData,
        },
    )
}

impl<T, const N: usize> Producer<T, N> {
    /// Returns the value back if the ring is full.
    #[inline]
    pub fn push(&self, val: T) -> Result<(), T> {
        let ring = &*self.ring;
        let h = ring.head.load(Ordering::Relaxed);
        let t = ring.tail.load(Ordering::Acquire);
        if h.wrapping_sub(t) == N {
            return Err(val);
        }
        // SAFETY: single producer; the consumer has released this slot (see `Ring`).
        unsafe { *ring.buf[h % N].get() = Some(val) };
        ring.head.store(h.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        N
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring.len() == 0
    }
}

impl<T, const N: usize> Consumer<T, N> {
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let ring = &*self.ring;
        let t = ring.tail.load(Ordering::Relaxed);
        let h = ring.head.load(Ordering::Acquire);
        if t == h {
            return None;
        }
        // SAFETY: single consumer; the producer published this slot (see `Ring`).
        let val = unsafe { (*ring.buf[t % N].get()).take() };
        ring.tail.store(t.wrapping_add(1), Ordering::Release);
        val
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        N
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ring.len() == 0
    }
}

impl<T, const N: usize> Ring<T, N> {
    #[inline]
    fn len(&self) -> usize {
        let t = self.tail.load(Ordering::Acquire);
        let h = self.head.load(Ordering::Acquire);
        h.wrapping_sub(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_ring_rejects_and_returns_value() {
        let (tx, rx) = channel::<u32, 2>();
        assert_eq!(tx.push(1), Ok(()));
        assert_eq!(tx.push(2), Ok(()));
        assert_eq!(tx.push(3), Err(3));
        assert_eq!(rx.pop(), Some(1));
        assert_eq!(tx.push(3), Ok(()));
        assert_eq!(rx.pop(), Some(2));
        assert_eq!(rx.pop(), Some(3));
        assert_eq!(rx.pop(), None);
    }

    #[test]
    fn cross_thread_delivery_is_ordered_and_lossless() {
        const COUNT: u64 = 200_000;
        let (tx, rx) = channel::<u64, 8>();
        let producer = std::thread::spawn(move || {
            for i in 0..COUNT {
                let mut v = i;
                while let Err(back) = tx.push(v) {
                    v = back;
                    std::hint::spin_loop();
                }
            }
        });
        let mut expected = 0;
        while expected < COUNT {
            if let Some(v) = rx.pop() {
                assert_eq!(v, expected);
                expected += 1;
            }
        }
        producer.join().unwrap();
        assert!(rx.is_empty());
    }
}
