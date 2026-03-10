use std::sync::Arc;

use memsecurity::EncryptedMem;
use zeroize::Zeroizing;

use crate::error::Error;

/// A 32-byte cryptographic key stored encrypted in memory using ASCON AEAD (`memsecurity`).
///
/// Cloning is cheap — all clones share the same `Arc<EncryptedMem>`. Decrypted
/// bytes are returned in a [`Zeroizing`] container that wipes memory on drop,
/// minimizing the window where the raw key is exposed.
#[derive(Clone)]
pub struct EncryptedKey {
    key: Arc<EncryptedMem>,
}

impl EncryptedKey {
    /// Encrypts `key` and stores it in secure memory.
    ///
    /// Returns an error if the underlying ASCON encryption fails, which typically
    /// indicates an out-of-memory or platform security error.
    pub fn new(key: &[u8; 32]) -> Result<Self, Error> {
        let mut encrypted_mem = EncryptedMem::new();

        encrypted_mem.encrypt(key).map_err(|_| {
            tracing::error!("Key encryption failed");
            Error::Helper {
                name: "encryption".to_owned(),
                reason: "Failed to encrypt key".to_owned(),
            }
        })?;

        tracing::debug!("EncryptedKey created");
        Ok(Self {
            key: Arc::new(encrypted_mem),
        })
    }

    /// Decrypts and returns the key in a [`Zeroizing`] container.
    ///
    /// The returned bytes are automatically wiped from memory when the container
    /// is dropped. Returns an error if decryption fails, which indicates memory
    /// corruption or tampering.
    pub fn key(&self) -> Result<Zeroizing<[u8; 32]>, Error> {
        let decrypted = self.key.decrypt().map_err(|_| {
            tracing::error!(
                "Key decryption failed, possible memory corruption"
            );
            Error::Helper {
                name: "decryption".to_owned(),
                reason: "Failed to decrypt key".to_owned(),
            }
        })?;

        let mut key_array = Zeroizing::new([0u8; 32]);
        key_array.copy_from_slice(decrypted.as_ref());

        tracing::debug!("Key accessed");
        Ok(key_array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypted_hash() {
        let key = [1u8; 32];
        let eh = EncryptedKey::new(&key).unwrap();
        let key1 = eh.key().unwrap();
        let eh_cloned = eh.clone();
        let key2 = eh_cloned.key().unwrap();
        assert_eq!(*key1, *key2);
    }

    #[test]
    fn test_encrypted_hash_deterministic() {
        // Same key should produce same decrypted value
        let key = [42u8; 32];
        let eh1 = EncryptedKey::new(&key).unwrap();
        let eh2 = EncryptedKey::new(&key).unwrap();

        let key1 = eh1.key().unwrap();
        let key2 = eh2.key().unwrap();

        assert_eq!(*key1, *key2);
    }

    #[test]
    fn test_encrypted_hash_different() {
        // Different keys should be different
        let key1_data = [1u8; 32];
        let key2_data = [2u8; 32];
        let eh1 = EncryptedKey::new(&key1_data).unwrap();
        let eh2 = EncryptedKey::new(&key2_data).unwrap();

        let key1 = eh1.key().unwrap();
        let key2 = eh2.key().unwrap();

        assert_ne!(*key1, *key2);
    }

    #[test]
    fn test_key_access() {
        let original = [123u8; 32];
        let eh = EncryptedKey::new(&original).unwrap();
        let decrypted = eh.key().unwrap();

        // Key should match original
        assert_eq!(*decrypted, original);

        // Verify we can still decrypt after first access
        let decrypted2 = eh.key().unwrap();
        assert_eq!(*decrypted2, original);
    }

    #[test]
    fn test_zeroizing() {
        let original = [99u8; 32];
        let eh = EncryptedKey::new(&original).unwrap();

        {
            let key = eh.key().unwrap();
            assert_eq!(*key, original);
            // When key is dropped here, memory should be zeroized
        }

        // Should still be able to decrypt
        let key2 = eh.key().unwrap();
        assert_eq!(*key2, original);
    }

    #[test]
    fn test_clone_is_cheap() {
        // Cloning should share the same Arc, not duplicate encrypted data
        let original = [77u8; 32];
        let eh = EncryptedKey::new(&original).unwrap();
        let eh_clone = eh.clone();

        // Both should point to the same Arc (same strong count)
        assert_eq!(Arc::strong_count(&eh.key), 2);
        assert_eq!(Arc::strong_count(&eh_clone.key), 2);

        // Both should decrypt to the same value
        let key1 = eh.key().unwrap();
        let key2 = eh_clone.key().unwrap();
        assert_eq!(*key1, *key2);
    }
}
