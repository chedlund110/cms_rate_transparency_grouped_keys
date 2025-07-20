from rate_group_key_factory import RateGroupKeyFactory
from term_bundle import TermBundle

def build_group_keys_for_term(term_bundle: TermBundle, factory: RateGroupKeyFactory) -> None:
    if not term_bundle.service_mod_pos_list:
        return

    all_codes: set[tuple[str, str, str]] = set()
    qualified_codes: set[tuple[str, str, str]] = set()
    has_qualifiers = bool(term_bundle.provider_code_ranges)

    for proc_code, modifier, pos, code_type in term_bundle.service_mod_pos_list:
        key = (proc_code, modifier, pos, code_type)
        all_codes.add(key)
        if has_qualifiers:
            qualified_codes.add(key)

    standard_codes = all_codes - qualified_codes

    # Register the standard group key
    if standard_codes:
        standard_key = factory.register_standard_group(term_bundle, standard_codes)
        term_bundle.standard_rate_key = standard_key

    # Register qualified and remainder keys
    if has_qualifiers:
        qualified_key = factory.register_qualified_group(term_bundle, qualified_codes)
        term_bundle.qualified_rate_key = qualified_key

        remainder_codes = standard_codes  # already calculated
        if remainder_codes:
            remainder_key = factory.register_remainder_group(term_bundle, remainder_codes)
            term_bundle.remainder_rate_key = remainder_key