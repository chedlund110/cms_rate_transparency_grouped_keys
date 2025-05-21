from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from provider_bundle import ProviderBundle
from rate_group_key_factory import RateGroupKeyFactory
from rate_storage import build_partial_indexes, store_rate_record
from term_bundle import TermBundle
from utilities import get_pos_and_type
from decimal import Decimal, ROUND_HALF_UP

def process_percent_of_allowed(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    if term_bundle.service_mod_pos_list:
        process_percent_of_allowed_ranges(context, term_bundle, rate_cache, {}, rate_group_key_factory)
    else:
        process_percent_of_allowed_full(context, term_bundle, rate_cache, rate_group_key_factory)

def process_percent_of_allowed_plus_fd_amt(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    if term_bundle.service_mod_pos_list:
        process_percent_of_allowed_ranges(context, term_bundle, rate_cache, {}, rate_group_key_factory)
    else:
        process_percent_of_allowed_full(context, term_bundle, rate_cache, rate_group_key_factory)

def process_percent_of_allowed_ranges(context: Context, term_bundle: TermBundle, rate_cache: dict,
                                      proc_code_amount_xref, rate_group_key_factory: RateGroupKeyFactory) -> None:
    base_pct = term_bundle.base_pct_of_charge
    section_id = term_bundle.section_id
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')
    calc_bean = term_bundle.calc_bean
    fee_schedule_name = term_bundle.fee_schedule_name

    rate_key = f"{term_bundle.rate_sheet_code}#{fee_schedule_name}"

    for proc_code, modifier, pos in term_bundle.service_mod_pos_list:
        full_key = (fee_schedule_name, proc_code, modifier, pos)

        if full_key in rate_cache:
            base_entry = rate_cache[full_key]
        else:
            base_entry = proc_code_amount_xref.get(proc_code)
            if not base_entry:
                continue

            new_entry = {
                "rate": base_entry.get("rate"),
                "percentage": base_entry.get("percentage"),
                "billing_code": proc_code,
                "billing_code_type": base_entry.get("billing_code_type", ""),
                "modifier": modifier,
                "pos": pos,
                "expiration_date": base_entry.get("expiration_date", DEFAULT_EXP_DATE),
                "rate_key": rate_key,
            }
            rate_cache[full_key] = new_entry
            base_entry = new_entry

        allow_amt = round(float(base_entry.get("rate", 0)), 2)
        percentage = float(base_entry.get("percentage", 0))
        term_date = base_entry.get("expiration_date", DEFAULT_EXP_DATE)
        proc_code_type = base_entry.get("billing_code_type", "")
        rate_key = base_entry.get("rate_key", "")

        if percentage > 0:
            fee = round(percentage * 100, 2)
            fee_type = "percentage"
        else:
            fee = allow_amt
            fee_type = "negotiated"

        if base_pct:
            fee = round(fee * base_pct, 2)

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": proc_code_type,
            "billing_code_type_ver": "10",
            "billing_code": proc_code,
            "pos_collection_key": pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": term_date,
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        })
        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple)

def process_percent_of_allowed_full(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    base_pct = term_bundle.base_pct_of_charge
    for full_key, proc_details in rate_cache.items():
        raw_rate = proc_details.get("rate", 0)
        rate = float(str(raw_rate)) if raw_rate else 0.0
        proc_details["rate"] = rate * base_pct
