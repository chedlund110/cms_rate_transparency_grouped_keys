from term_bundle import TermBundle
from rate_group_key_factory import RateGroupKeyFactory

def build_group_keys_for_term(term_bundle: TermBundle, factory: RateGroupKeyFactory) -> None:
    standard_codes = set()
    qualified_codes = set()
    has_provider_qualifiers = bool(term_bundle.provider_code_ranges)

    for proc_code, modifier, pos in term_bundle.service_mod_pos_list:
        key = f"{proc_code}|{modifier}|{pos}"
        if has_provider_qualifiers:
            qualified_codes.add(key)
        else:
            standard_codes.add(key)

    if standard_codes:
        factory.register_standard_group(term_bundle, standard_codes)

    if has_provider_qualifiers:
        factory.register_qualified_group(term_bundle, qualified_codes)

        remainder_codes = standard_codes - qualified_codes
        if remainder_codes:
            factory.register_remainder_group(term_bundle, remainder_codes)