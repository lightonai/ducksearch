import hashlib
import secrets


def generate_random_hash() -> str:
    """Generate a random SHA-256 hash."""
    random_data = secrets.token_bytes(32)
    hash_obj = hashlib.sha256()
    hash_obj.update(random_data)
    random_hash = hash_obj.hexdigest()
    return random_hash
