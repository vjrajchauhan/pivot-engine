#[derive(Debug, Clone)]
pub struct NullBitmask {
    bits: Vec<bool>,
}

impl NullBitmask {
    pub fn new() -> Self { Self { bits: Vec::new() } }
    pub fn push(&mut self, is_valid: bool) { self.bits.push(is_valid); }
    pub fn get(&self, idx: usize) -> bool { self.bits.get(idx).copied().unwrap_or(false) }
    pub fn set(&mut self, idx: usize, is_valid: bool) {
        if idx < self.bits.len() { self.bits[idx] = is_valid; }
    }
    pub fn len(&self) -> usize { self.bits.len() }
    pub fn count_valid(&self) -> usize { self.bits.iter().filter(|&&b| b).count() }
    pub fn count_null(&self) -> usize { self.bits.iter().filter(|&&b| !b).count() }
}
