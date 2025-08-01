from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from provider_bundle import ProviderBundle
from rate_group_key_factory import RateGroupKeyFactory
from rate_group_utilities import build_rate_group_key_if_needed
from rate_storage import ensure_rate_cache_index, store_rate_record
from term_bundle import TermBundle
from utilities import get_pos_and_type
from decimal import Decimal, ROUND_HALF_UP

def process_percent_of_allowed(context: Context, term_bundle: TermBundle,
                                rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:

    ensure_rate_cache_index(context, rate_cache)
    rate_index = context.rate_cache_index

    has_service_filters = bool(term_bundle.service_mod_pos_list)
    has_provider_filters = bool(term_bundle.provider_ranges)

    term_bundle.was_poa = True

    if has_provider_filters:
        # Handles provider-level filters like Taxonomy/NPI/ZIP
        if has_service_filters:
            filtered_keys = _filter_rate_keys_by_service(term_bundle, rate_index)
        else:
            filtered_keys = list(rate_cache.keys())
        _process_poa_by_copy(context, term_bundle, rate_cache, filtered_keys, rate_group_key_factory)
    else:
        # Optimized flow for service+modifier only (no provider filters)
        _process_poa_without_provider_filters(context, term_bundle, rate_cache, rate_group_key_factory)


def _process_poa_without_provider_filters(context: Context, term_bundle: TermBundle,
                                          rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    base_pct = term_bundle.base_pct_of_charge or 1.0
    rate_sheet_code = term_bundle.rate_sheet_code

    for proc_code, modifier, pos, code_type in term_bundle.service_mod_pos_list:
        base_key = (rate_sheet_code, proc_code, '', pos, code_type)
        base_entry = rate_cache.get(base_key)

        if not base_entry:
            continue

        fee, fee_type = _calculate_poa_rate(base_entry, base_pct)
        base_rate_key = base_entry.get("prov_grp_contract_key", "")
        rate_key = build_rate_group_key_if_needed(term_bundle, base_rate_key, rate_group_key_factory)

        _build_and_store_rate(context, term_bundle, base_entry,
                              proc_code, modifier, pos, code_type,
                              fee, fee_type, rate_key,
                              rate_cache, rate_group_key_factory)


def _filter_rate_keys_by_service(term_bundle: TermBundle, rate_index: dict) -> list:
    rate_sheet_code = term_bundle.rate_sheet_code
    service_list = term_bundle.service_mod_pos_list

    by_proc = rate_index.get("by_proc", {}).get(rate_sheet_code, {})
    by_mod = rate_index.get("by_modifier", {}).get(rate_sheet_code, {})
    by_pos = rate_index.get("by_pos", {}).get(rate_sheet_code, {})

    result = set()

    for proc_code, modifier, pos, code_type in service_list:
        proc_keys = set(by_proc.get(proc_code, []))
        mod_keys = set(by_mod.get(modifier, []))
        pos_keys = set(by_pos.get(pos, []))

        matched = proc_keys & mod_keys & pos_keys
        result.update(matched)

    return list(result)


def _calculate_poa_rate(base_entry: dict, base_pct: float) -> tuple[float, str]:
    allow_amt = float(base_entry.get("rate") or 0)

    if base_pct > 0:
        fee = round(base_pct * allow_amt, 2)
        fee_type = "negotiated"
    else:
        fee = allow_amt
        fee_type = "negotiated"

    return fee, fee_type


def _build_and_store_rate(context: Context, term_bundle: TermBundle,
                          base_entry: dict, proc_code: str, modifier: str, pos: str,
                          code_type: str, fee: float, fee_type: str, rate_key: str,
                          rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    section_id = term_bundle.section_id
    _, rate_type_desc = get_pos_and_type(section_name="")
    calc_bean = term_bundle.calc_bean
        
    rate_dict = rate_template.copy()
    rate_dict.update({
        "update_type": "A",
        "insurer_code": context.insurer_code,
        "prov_grp_contract_key": rate_key,
        "negotiation_arrangement": "ffs",
        "billing_code_type": base_entry.get("billing_code_type", ""),
        "billing_code_type_ver": "10",
        "billing_code": proc_code,
        "pos_collection_key": pos,
        "negotiated_type": fee_type,
        "rate": str(fee),
        "modifier": modifier,
        "billing_class": rate_type_desc,
        "expiration_date": base_entry.get("expiration_date", DEFAULT_EXP_DATE),
        "full_term_section_id": section_id,
        "calc_bean": calc_bean
    })

    code_tuple = (proc_code, modifier, pos)
    dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos, code_type, term_bundle.term_id)
    
    store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple,
                      context.shared_config.valid_service_codes, context.rate_cache_index,
                      term_bundle=term_bundle)


def _process_poa_by_copy(context: Context, term_bundle: TermBundle,
                         rate_cache: dict, filtered_keys: list,
                         rate_group_key_factory: RateGroupKeyFactory) -> None:
    base_pct = term_bundle.base_pct_of_charge or 1.0

    for key in filtered_keys:
        if len(key) == 5:
            rate_sheet_code, proc_code, modifier, pos, code_type = key
        else:
            rate_sheet_code, proc_code, modifier, pos, code_type, _ = key

        base_entry = rate_cache.get(key)
        if not base_entry:
            continue

        fee, fee_type = _calculate_poa_rate(base_entry, base_pct)
        standard_key = base_entry.get("prov_grp_contract_key", "")
        rate_key = build_rate_group_key_if_needed(term_bundle, standard_key, rate_group_key_factory)

        _build_and_store_rate(context, term_bundle, base_entry,
                              proc_code, modifier, pos, code_type,
                              fee, fee_type, rate_key,
                              rate_cache, rate_group_key_factory)


def process_percent_of_allowed_plus_fd_amt(context: Context, term_bundle: TermBundle,
                                           rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    process_percent_of_allowed(context, term_bundle, rate_cache, rate_group_key_factory)
