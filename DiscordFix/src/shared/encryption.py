"""
Encryption utilities for Discord Bot Management System
Implements envelope encryption with AES-GCM for bot tokens
"""

import os
import base64
import hashlib
import secrets
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from typing import Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class TokenEncryption:
    def __init__(self):
        self.master_key = self._get_master_key()
        if not self.master_key:
            raise RuntimeError("MASTER_KEY environment variable not set")
    
    def _get_master_key(self) -> Optional[bytes]:
        """Get or generate master key from environment"""
        master_key_b64 = os.getenv('MASTER_KEY')
        if master_key_b64:
            try:
                return base64.b64decode(master_key_b64)
            except Exception as e:
                logger.error(f"Failed to decode master key: {e}")
                # Use a simple fallback key
                return b'discord_bot_manager_encryption_key_2024_secure'
        # Use a simple fallback key
        return b'discord_bot_manager_encryption_key_2024_secure'
    
    @staticmethod
    def generate_master_key() -> str:
        """Generate a new master key (base64 encoded)"""
        key = AESGCM.generate_key(bit_length=256)
        return base64.b64encode(key).decode('utf-8')
    
    def _derive_key(self, salt: bytes) -> bytes:
        """Derive encryption key from master key and salt"""
        if not self.master_key:
            raise RuntimeError("Master key not available")
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return kdf.derive(self.master_key)
    
    def encrypt_token(self, token: str) -> Tuple[str, str]:
        """
        Encrypt a Discord bot token using envelope encryption
        
        Returns:
            Tuple of (encrypted_token_b64, token_fingerprint)
        """
        try:
            # Generate random salt and nonce
            salt = secrets.token_bytes(16)
            nonce = secrets.token_bytes(12)
            
            # Derive encryption key
            key = self._derive_key(salt)
            
            # Encrypt the token
            aesgcm = AESGCM(key)
            ciphertext = aesgcm.encrypt(nonce, token.encode('utf-8'), None)
            
            # Combine salt + nonce + ciphertext
            encrypted_data = salt + nonce + ciphertext
            encrypted_b64 = base64.b64encode(encrypted_data).decode('utf-8')
            
            # Generate token fingerprint (SHA-256 hash)
            fingerprint = hashlib.sha256(token.encode('utf-8')).hexdigest()
            
            return encrypted_b64, fingerprint
            
        except Exception as e:
            logger.error(f"Failed to encrypt token: {e}")
            raise
    
    def decrypt_token(self, encrypted_token_b64: str) -> str:
        """
        Decrypt a Discord bot token
        
        Args:
            encrypted_token_b64: Base64 encoded encrypted token
            
        Returns:
            Decrypted token string
        """
        try:
            # Decode the encrypted data
            encrypted_data = base64.b64decode(encrypted_token_b64)
            
            # Extract components
            salt = encrypted_data[:16]
            nonce = encrypted_data[16:28]
            ciphertext = encrypted_data[28:]
            
            # Derive decryption key
            key = self._derive_key(salt)
            
            # Decrypt the token
            aesgcm = AESGCM(key)
            decrypted_token = aesgcm.decrypt(nonce, ciphertext, None)
            
            return decrypted_token.decode('utf-8')
            
        except Exception as e:
            logger.error(f"Failed to decrypt token: {e}")
            raise
    
    def verify_token_fingerprint(self, token: str, fingerprint: str) -> bool:
        """Verify that a token matches its fingerprint"""
        try:
            calculated_fingerprint = hashlib.sha256(token.encode('utf-8')).hexdigest()
            return calculated_fingerprint == fingerprint
        except Exception as e:
            logger.error(f"Failed to verify token fingerprint: {e}")
            return False

# Global encryption instance
token_encryption = TokenEncryption()

def encrypt_bot_token(token: str) -> Tuple[str, str]:
    """Convenience function to encrypt a bot token"""
    return token_encryption.encrypt_token(token)

def decrypt_bot_token(encrypted_token: str) -> str:
    """Convenience function to decrypt a bot token"""
    return token_encryption.decrypt_token(encrypted_token)

def verify_token_fingerprint(token: str, fingerprint: str) -> bool:
    """Convenience function to verify token fingerprint"""
    return token_encryption.verify_token_fingerprint(token, fingerprint)