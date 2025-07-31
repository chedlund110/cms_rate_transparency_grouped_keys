from collections import defaultdict
from term_bundle import TermBundle
from typing import Optional, Dict, Set, Tuple

class RateGroupKey:
    def __init__(
        self,
        key: str,
        qualifiers: Optional[Dict[str, Dict[str, Dict]]] = {}
    ):
        self.key = key
        self.qualifiers = qualifiers  # None = fallback / no filters


class RateGroupKeyFactory:
    def __init__(self):
        # rate_sheet_code → { group_key → RateGroupKey }
        self.store: Dict[str, Dict[str, RateGroupKey]] = defaultdict(dict)
        self.key_counter: int = 0

    def generate_key(self, base: str, qualifier: Optional[Dict] = None, suffix: str = "Q") -> str:
        if qualifier:
            self.key_counter += 1
            return f"{base}#{suffix}{self.key_counter}"
        return base  # standard fallback grouping

    def add_key(
        self,
        base: str,
        qualifiers: Optional[Dict[str, Dict[str, Dict]]] = {},
        suffix: str = "Q"
    ) -> str:
        key = self.generate_key(base, qualifiers, suffix)
        self.store[base][key] = RateGroupKey(key=key, qualifiers=qualifiers)
        return key

    def register_standard_group(self, term_bundle: TermBundle) -> str:
        base = term_bundle.rate_sheet_code
        return self.add_key(base, qualifiers=None, suffix="S")

    def register_qualified_group(self, term_bundle: TermBundle) -> str:
        base = term_bundle.rate_sheet_code
        qualifiers = term_bundle.provider_code_ranges
        return self.add_key(base, qualifiers=qualifiers, suffix="Q")

    def register_remainder_group(self, term_bundle: TermBundle) -> str:
        base = term_bundle.rate_sheet_code
        return self.add_key(base, qualifiers=None, suffix="R")

    def get_keys_for_rate_sheet(self, rate_sheet_code: str) -> Dict[str, RateGroupKey]:
        return self.store.get(rate_sheet_code, {})

    def all_keys(self) -> Dict[str, Dict[str, RateGroupKey]]:
        return self.store

    def merge(self, other: "RateGroupKeyFactory"):
        if other is None or other.store is None:
            return
        for rate_sheet_code, keys_dict in other.store.items():
            self.store[rate_sheet_code].update(keys_dict)

def merge_rate_group_key_factories(factories: list[RateGroupKeyFactory]) -> RateGroupKeyFactory:
    merged = RateGroupKeyFactory()
    for factory in factories:
        merged.merge(factory)
    return merged

