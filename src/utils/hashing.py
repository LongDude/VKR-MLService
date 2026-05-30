from __future__ import annotations

from hashlib import sha256


def calculate_text_hash(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


hash_text = calculate_text_hash
stable_text_hash = calculate_text_hash


__all__ = ["calculate_text_hash", "hash_text", "stable_text_hash"]
