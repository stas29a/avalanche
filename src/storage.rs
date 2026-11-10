use bytes::Bytes;
use papaya::HashMap;

#[derive(Clone, Debug)]
pub struct Storage {
    maps: [HashMap<Bytes, Bytes>; 256],
}

impl Storage {
    pub fn new() -> Self {
        let maps = std::array::from_fn(|_| HashMap::new());
        Self { maps }
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        if key.is_empty() {
            return None;
        }

        let index = self.get_index(&key);
        self.maps[index].pin().get(&key).cloned()
    }

    pub fn set(&self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        if key.is_empty() {
            return Err(anyhow::Error::msg("key cannot be empty"));
        }

        let index = self.get_index(&key);
        self.maps[index].pin().insert(key, value);
        anyhow::Ok(())
    }

    #[inline(always)]
    pub fn get_index(&self, key: &Bytes) -> usize {

        if key.len() > 1 {
            let mut index = usize::from(key[0]);
            index += usize::from(key[key.len() - 1]);
            index % 256
        } else {
            self.get_index(&key)
        }
    }
}
