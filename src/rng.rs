//! Internal jitter PRNG: SplitMix64 — dependency-free, `rand`-optional.
//!
//! Jitter is schedule decorrelation, not security: the only contract is
//! the *bound* of each draw, which [`JitterSource::random_range`]
//! guarantees. Crates standardising on the `rand` stack can enable the
//! `rand` feature to draw through `SmallRng` instead.

/// SplitMix64 — 13 machine instructions per 64 bits, full-period,
/// passes gjrand; ideal for non-cryptographic schedule decorrelation.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Seed from an arbitrary `u64` (wall-clock nanos, a name hash, …).
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Next raw 64 bits.
    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// The bound-only RNG contract the scheduler draws through.
pub trait JitterSource {
    /// Uniform-ish draw in `[0, u64::MAX]` (modulo mapping happens at the
    /// call site).
    fn next_u64(&mut self) -> u64;

    /// Uniform draw in `[lo, hi]` (inclusive). Default maps a full-width
    /// draw by proportion; the modulo bias is negligible at these ranges
    /// and the contract is the *bound*, not uniformity guarantees.
    fn random_range(
        &mut self,
        range: std::ops::RangeInclusive<std::time::Duration>,
    ) -> std::time::Duration {
        let lo = range.start().as_nanos() as u64;
        let hi = range.end().as_nanos() as u64;
        let draw = self.next_u64();
        let span = hi.saturating_sub(lo);
        let nanos = lo.wrapping_add(if span == u64::MAX {
            draw
        } else {
            draw % (span + 1)
        });
        std::time::Duration::from_nanos(nanos)
    }
}

impl JitterSource for SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.next_u64()
    }
}
