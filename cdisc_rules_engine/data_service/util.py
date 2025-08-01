import hashlib
import base64
import re


def generate_hash(input_string: str, length: int = 60) -> str:
    hash_bytes = hashlib.sha256(input_string.encode("utf-8")).digest()
    hash_b64 = base64.urlsafe_b64encode(hash_bytes).decode("utf-8")
    alphanum_hash = re.sub(r"[^a-zA-Z0-9]", "", hash_b64).lower()
    return "v" + alphanum_hash[:length]
