from collections import defaultdict
from term_bundle import TermBundle
from typing import Optional, Dict, Set, Tuple


class RateGroupKey:
    def __init__(
        self,
        key: str,
        codes: Optional[Set[Tuple[str, str, str]]] = None,
        qualifiers: Optional[Dict[str, Dict[str, Dict]]] = None
    ):
        self.key = key
        self.codes = codes or set()
        self.qualifiers = qualifiers  # None = standard grouping with no filters

    def add_code(self, proc_code: str, modifier: str, pos: str):
        self.codes.add((proc_code, modifier, pos))

    def has_code(self, proc_code: str, modifier: str, pos: str) -> bool:
        return (proc_code, modifier, pos) in self.codes


class RateGroupKeyFactory:
    def __init__(self):
        # Outer dict = rate_sheet_code → Inner dict = key → RateGroupKey
        self.store: Dict[str, Dict[str, RateGroupKey]] = defaultdict(dict)
        self.key_counter: int = 0

    def generate_key(self, base: str, qualifier: Optional[Dict] = None) -> str:
        if qualifier:
            self.key_counter += 1
            return f"{base}#Q{self.key_counter}"
        return base  # standard grouping key

    def add_key(
        self,
        base: str,
        codes: Set[Tuple[str, str, str]],
        qualifiers: Optional[Dict[str, Dict[str, Dict]]] = None
    ) -> str:
        key = self.generate_key(base, qualifiers)
        self.store[base][key] = RateGroupKey(key=key, codes=codes, qualifiers=qualifiers)
        return key

    def get_keys_for_rate_sheet(self, rate_sheet_code: str) -> Dict[str, RateGroupKey]:
        return self.store.get(rate_sheet_code, {})

    def all_keys(self) -> Dict[str, Dict[str, RateGroupKey]]:
        return self.store

    def merge(self, other: "RateGroupKeyFactory"):
        if other is None or other.store is None:
            return
        for rate_sheet_code, keys_dict in other.store.items():
            for key, incoming_group in keys_dict.items():
                if key not in self.store[rate_sheet_code]:
                    self.store[rate_sheet_code][key] = incoming_group
                else:
                    self.store[rate_sheet_code][key].codes.update(incoming_group.codes)
                    # Optional: merge qualifiers if needed

    def register_standard_group(self, term_bundle: TermBundle, codes: Set[Tuple[str, str, str]]) -> str:
        base = term_bundle.rate_sheet_code
        return self.add_key(base, codes, qualifiers=None)

    def register_qualified_group(self, term_bundle: TermBundle, codes: Set[Tuple[str, str, str]]) -> str:
        base = term_bundle.rate_sheet_code
        qualifiers = term_bundle.provider_code_ranges
        return self.add_key(base, codes, qualifiers)

    def register_remainder_group(self, term_bundle: TermBundle, codes: Set[Tuple[str, str, str]]) -> str:
        base = term_bundle.rate_sheet_code
        self.key_counter += 1
        remainder_key = f"{base}#R{self.key_counter}"
        self.store[base][remainder_key] = RateGroupKey(key=remainder_key, codes=codes, qualifiers=None)
        return remainder_key

    def get_keys_for_ratesheet(self, rate_sheet_code: str) -> dict[str, RateGroupKey]:
        return self.store.get(rate_sheet_code, {})

# Utility function to merge a list of factories
def merge_rate_group_key_factories(factories: list[RateGroupKeyFactory]) -> RateGroupKeyFactory:
    merged = RateGroupKeyFactory()
    for factory in factories:
        merged.merge(factory)
    return merged