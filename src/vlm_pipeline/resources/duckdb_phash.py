"""DuckDB pHash 관련 유틸리티 mixin."""

from __future__ import annotations

from itertools import combinations


class DuckDBPhashMixin:
    """pHash prefix 후보 생성 등 perceptual-hash 관련 메서드."""

    @staticmethod
    def _phash_prefix_candidates(phash_hex: str, threshold: int, prefix_hex_len: int = 2) -> list[str]:
        """prefix hamming prefilter 후보(prefix) 생성."""
        if not phash_hex:
            return []
        normalized = str(phash_hex).strip().lower()
        prefix_hex_len = max(1, min(4, int(prefix_hex_len)))
        prefix = normalized[:prefix_hex_len]
        if len(prefix) < prefix_hex_len:
            return []

        try:
            base = int(prefix, 16)
        except ValueError:
            return []

        total_bits = prefix_hex_len * 4
        max_flip = max(0, min(int(threshold), total_bits))
        all_count = 1 << total_bits
        if max_flip >= total_bits:
            return [f"{value:0{prefix_hex_len}x}" for value in range(all_count)]

        values: set[int] = set()
        for flip_count in range(max_flip + 1):
            for bit_positions in combinations(range(total_bits), flip_count):
                candidate = base
                for pos in bit_positions:
                    candidate ^= 1 << pos
                values.add(candidate)

        return [f"{value:0{prefix_hex_len}x}" for value in sorted(values)]
