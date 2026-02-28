use std::sync::Arc;

use memsecurity::EncryptedMem;
use zeroize::Zeroizing;

use crate::error::Error;

/// Helper for encrypted 32-byte key storage.
///
/// This structure stores a 32-byte cryptographic key encrypted in memory using
/// ASCON AEAD encryption via EncryptedMem.
///
/// The key is wrapped in an `Arc`, making cloning cheap and safe.
/// Cloning only clones the reference, not the encrypted data.
///
/// # Security Notes
///
/// - The key is stored encrypted in memory using memsecurity
/// - Key bytes are automatically zeroized when accessed via `key()`
/// - Multiple clones share the same encrypted data (via Arc)
/// - Only accepts exactly 32-byte keys for use with XChaCha20-Poly1305
#[derive(Clone)]
pub struct EncryptedKey {
    key: Arc<EncryptedMem>,
}

impl EncryptedKey {
    /// Create a new `EncryptedKey` from a 32-byte key.
    ///
    /// The key is encrypted and stored in secure memory wrapped in an Arc.
    ///
    /// # Arguments
    ///
    /// * `key` - A 32-byte array to encrypt and store
    ///
    /// # Errors
    ///
    /// Returns an error if encryption fails (e.g., due to lack of system entropy
    /// or memory allocation failure).
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

    /// Get the decrypted 32-byte key in a zeroizing container.
    ///
    /// Returns the key bytes wrapped in `Zeroizing` which automatically
    /// clears the memory when the value is dropped, preventing the sensitive
    /// data from remaining in memory.
    ///
    /// # Security
    ///
    /// This is the preferred way to access the key as it ensures automatic
    /// cleanup of sensitive data.
    ///
    /// # Errors
    ///
    /// Returns an error if decryption fails, which would indicate memory
    /// corruption or a bug in the encryption implementation.
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
