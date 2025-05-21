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
        self.qualifiers = qualifiers  # None means it's a standard key with no qualifiers

    def add_code(self, proc_code: str, modifier: str, pos: str):
        self.codes.add((proc_code, modifier, pos))

    def has_code(self, proc_code: str, modifier: str, pos: str) -> bool:
        return (proc_code, modifier, pos) in self.codes


class RateGroupKeyFactory:
    def __init__(self):
        self.store: Dict[str, RateGroupKey] = {}
        self.key_counter: int = 0

    def generate_key(self, base: str, qualifier: Optional[Dict] = None) -> str:
        if qualifier:
            self.key_counter += 1
            return f"{base}#Q{self.key_counter}"
        return base  # base is the standard key

    def add_key(
        self,
        base: str,
        codes: Set[Tuple[str, str, str]],
        qualifiers: Optional[Dict[str, Dict[str, Dict]]] = None
    ) -> str:
        key = self.generate_key(base, qualifiers)
        self.store[key] = RateGroupKey(key=key, codes=codes, qualifiers=qualifiers)
        return key

    def get(self, key: str) -> Optional[RateGroupKey]:
        return self.store.get(key)
    
    def merge(self, other: "RateGroupKeyFactory"):
        for key, rate_group in other.store.items():
            if key not in self.store:
                self.store[key] = rate_group
    def all_keys(self) -> Dict[str, RateGroupKey]:
        return self.store
    
    def register_standard_group(self, term_bundle: TermBundle, codes: Set[Tuple[str, str, str]]) -> str:
        base = term_bundle.rate_sheet_code
        return self.add_key(base, codes, qualifiers=None)

    def register_qualified_group(self, term_bundle: TermBundle, codes: Set[Tuple[str, str, str]]) -> str:
        base = term_bundle.rate_sheet_code
        qualifiers = term_bundle.provider_code_ranges
        return self.add_key(base, codes, qualifiers)

    def register_remainder_group(self, term_bundle: TermBundle, codes: Set[Tuple[str, str, str]]) -> str:
        # Use the same logic as qualified to generate a unique remainder key
        base = term_bundle.rate_sheet_code
        # Just to keep it unique, we treat it as a special case with no qualifiers
        self.key_counter += 1
        remainder_key = f"{base}#R{self.key_counter}"
        self.store[remainder_key] = RateGroupKey(key=remainder_key, codes=codes, qualifiers=None)
        return remainder_key