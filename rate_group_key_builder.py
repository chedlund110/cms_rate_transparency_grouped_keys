from rate_group_key_factory import RateGroupKeyFactory
from term_bundle import TermBundle

from rate_group_key_factory import RateGroupKeyFactory
from term_bundle import TermBundle

def build_group_keys_for_term(term_bundle: TermBundle, factory: RateGroupKeyFactory) -> None:
    if not term_bundle.service_mod_pos_list:
        return

    has_qualifiers = bool(term_bundle.provider_code_ranges)

    if has_qualifiers:
        term_bundle.qualified_rate_key = factory.register_qualified_group(term_bundle)
        term_bundle.remainder_rate_key = factory.register_remainder_group(term_bundle)
    else:
        term_bundle.standard_rate_key = factory.register_standard_group(term_bundle)
        
