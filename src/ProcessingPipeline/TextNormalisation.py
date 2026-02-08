"""Utility to reconstruct readable text from noisy PDF extraction output."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable, List, Optional

try:
    import wordninja
except ImportError:  # pragma: no cover
    wordninja = None  # type: ignore[assignment]


class TextNormalisation:
    """Heuristic text normaliser tailored for scientific PDF content."""

    def __init__(
        self,
        enable_segmentation: bool = True,
        max_compound_length: int = 18,
        min_alpha_ratio: float = 0.65,
    ) -> None:
        self.enable_segmentation = enable_segmentation and wordninja is not None
        self.max_compound_length = max_compound_length
        self.min_alpha_ratio = min_alpha_ratio

    def normalize(self, text: str) -> str:
        if not text:
            return ""

        text = self._basic_cleanup(text)
        text = self._remove_headers_and_footers(text)
        text = self._fix_hyphenation(text)
        text = self._restore_spacing(text)
        text = self._normalize_punctuation(text)
        text = self._split_compound_tokens(text)
        text = self._collapse_whitespace(text)
        return text.strip()

    # ------------------------------------------------------------------ helpers
    def _basic_cleanup(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKC", text)
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
        normalized = re.sub(r"\u00ad", "", normalized)  # soft hyphen
        return normalized

    def _remove_headers_and_footers(self, text: str) -> str:
        lines: List[str] = []
        seen_snippets: dict[str, int] = {}

        for raw_line in text.split("\n"):
            line = raw_line.strip()
            if not line:
                lines.append("")
                continue
            lower = line.lower()
            if re.match(r"^page\s+\d+\s+of\s+\d+$", lower):
                continue
            if lower.startswith("arxiv:") and len(line) < 40:
                continue
            if re.match(r"^\d+\s*/\s*\d+$", lower):
                continue
            if lower in seen_snippets:
                seen_snippets[lower] += 1
            else:
                seen_snippets[lower] = 1
            if seen_snippets[lower] > 5 and len(line) < 60:
                continue
            lines.append(raw_line)
        return "\n".join(lines)

    def _fix_hyphenation(self, text: str) -> str:
        text = re.sub(r"(?<=\w)-\s*\n\s*(?=\w)", "", text)
        text = re.sub(r"(?<=\w)-\s+(?=\w)", "", text)
        return text

    def _restore_spacing(self, text: str) -> str:
        paragraph_token = "\u2029"
        text = re.sub(r"\n{2,}", paragraph_token, text)
        text = text.replace("\n", " ")
        text = text.replace(paragraph_token, "\n\n")
        return text

    def _normalize_punctuation(self, text: str) -> str:
        text = re.sub(r"\s*([–—-])\s*", r" \1 ", text)
        text = re.sub(r"\s+([,.;:!?])", r"\1", text)
        text = re.sub(r"([(\[]) ", r"\1", text)
        text = re.sub(r" ([)\]])", r"\1", text)
        return text

    def _split_compound_tokens(self, text: str) -> str:
        if not self.enable_segmentation:
            return text

        pattern = re.compile(
            rf"(?<![A-Za-z])([A-Za-z]{{{self.max_compound_length},}})(?![A-Za-z])"
        )

        def _ratio(token: str) -> float:
            alpha = sum(ch.isalpha() for ch in token)
            return alpha / max(1, len(token))

        def _replacer(match: re.Match[str]) -> str:
            token = match.group(1)
            if not token or _ratio(token) < self.min_alpha_ratio:
                return token
            pieces: List[str] = wordninja.split(token)  # type: ignore[arg-type]
            if len(pieces) <= 1:
                return token
            if token.isupper():
                transformed = " ".join(piece.upper() for piece in pieces)
            elif token[0].isupper():
                transformed = " ".join(
                    [pieces[0].capitalize()] + [piece.lower() for piece in pieces[1:]]
                )
            else:
                transformed = " ".join(pieces)
            return transformed

        return pattern.sub(_replacer, text)

    def _collapse_whitespace(self, text: str) -> str:
        text = re.sub(r"[ \t]{2,}", " ", text)
        text = re.sub(r" ?\n ?", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text


__all__ = ["TextNormalisation"]
