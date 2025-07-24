from context import Context
from constants import DEFAULT_EXP_DATE
from rate_group_key_factory import RateGroupKeyFactory
from rate_group_utilities import build_rate_group_key_if_needed
from rate_storage import store_rate_record
from term_bundle import TermBundle
from utilities import get_pos_and_type

def process_fee_schedule(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    if term_bundle.service_mod_pos_list:
        process_fee_schedule_ranges(context, term_bundle, rate_cache, rate_group_key_factory)
    else:
        process_fee_schedule_full(context, term_bundle, rate_cache, rate_group_key_factory)


def process_fee_schedule_full(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')

    def get_schedules():
        if hasattr(term_bundle, "locality_fee_schedule_keys") and term_bundle.locality_fee_schedule_keys:
            for key in term_bundle.locality_fee_schedule_keys:
                schedule_values = context.shared_config.locality_fee_schedules.get(key, {})
                if schedule_values:
                    yield (key, schedule_values)
        else:
            fee_schedule_name = term_bundle.fee_schedule_name
            schedule_values = context.fee_schedules.get(fee_schedule_name, {})
            if schedule_values:
                yield ((fee_schedule_name,), schedule_values)

    for key_tuple, schedule_values in get_schedules():
        if len(key_tuple) == 3:
            schedule_name, carrier_number, locality_number = key_tuple
            rate_key = f"{term_bundle.rate_sheet_code}#{schedule_name}#{carrier_number}#{locality_number}#locality"
        else:
            schedule_name = key_tuple[0]
            rate_key = f"{term_bundle.rate_sheet_code}#{schedule_name}"
        rate_key = build_rate_group_key_if_needed(term_bundle, rate_key, rate_group_key_factory)
        for modifier, proc_codes in schedule_values.items():
            for proc_code, proc_details in proc_codes.items():
                code_type = proc_details.get("proc_code_type", "")
                allow_amt = round(proc_details.get("allowed", 0), 2)
                percentage = proc_details.get("percentage", 0)

                if allow_amt > 0:
                    fee = allow_amt
                    fee_type = "fee schedule"
                elif percentage > 0:
                    fee = percentage * 100
                    fee_type = "percentage"
                else:
                    fee = 0
                    fee_type = "fee schedule"

                if term_bundle.base_pct_of_charge:
                    fee = round(allow_amt * term_bundle.base_pct_of_charge, 2)

                dict_key = (schedule_name, proc_code, modifier, rate_pos, code_type)
                rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": code_type,
                    "billing_code_type_ver": "10",
                    "billing_code": proc_code,
                    "pos_collection_key": rate_pos,
                    "negotiated_type": fee_type,
                    "rate": str(fee),
                    "modifier": modifier,
                    "billing_class": rate_type_desc,
                    "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                    "full_term_section_id": section_id,
                    "calc_bean": calc_bean
                }
                code_tuple = (proc_code, modifier, rate_pos)
                store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes)

def process_fee_schedule_ranges(
    context: Context,
    term_bundle: TermBundle,
    rate_cache: dict,
    rate_group_key_factory: RateGroupKeyFactory
) -> None:
    if getattr(term_bundle, "locality_fee_schedule_keys", None):
        process_locality_fee_schedule_ranges(context, term_bundle, rate_cache, rate_group_key_factory)
    else:
        process_standard_fee_schedule_ranges(context, term_bundle, rate_cache, rate_group_key_factory)

def process_standard_fee_schedule_ranges(
    context: Context,
    term_bundle: TermBundle,
    rate_cache: dict,
    rate_group_key_factory: RateGroupKeyFactory
) -> None:
    schedule_name = term_bundle.fee_schedule_name
    schedule_values = context.fee_schedules.get(schedule_name, {})
    if not schedule_values:
        return

    raw_key = f"{term_bundle.rate_sheet_code}#{schedule_name}"
    rate_key = build_rate_group_key_if_needed(term_bundle, raw_key, rate_group_key_factory)

    _process_fee_schedule_range_common(
        context,
        term_bundle,
        schedule_name,
        schedule_values,
        rate_key,
        rate_cache,
        rate_group_key_factory
    )

def process_locality_fee_schedule_ranges(
    context: Context,
    term_bundle: TermBundle,
    rate_cache: dict,
    rate_group_key_factory: RateGroupKeyFactory
) -> None:
    for key_tuple in term_bundle.locality_fee_schedule_keys:
        schedule_values = context.shared_config.locality_fee_schedules.get(key_tuple, {})
        if not schedule_values:
            continue

        schedule_name, carrier_number, locality_number = key_tuple
        raw_key = f"{term_bundle.rate_sheet_code}#{schedule_name}#{carrier_number}#{locality_number}#locality"
        rate_key = build_rate_group_key_if_needed(term_bundle, raw_key, rate_group_key_factory)

        _process_fee_schedule_range_common(
            context,
            term_bundle,
            schedule_name,
            schedule_values,
            rate_key,
            rate_cache,
            rate_group_key_factory
        )

def _process_fee_schedule_range_common(
    context: Context,
    term_bundle: TermBundle,
    schedule_name: str,
    schedule_values: dict,
    rate_key: str,
    rate_cache: dict,
    rate_group_key_factory: RateGroupKeyFactory
) -> None:
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name="")
    service_mod_pos_list = list(term_bundle.service_mod_pos_list or [])

    for proc_code, modifier, pos, _ in service_mod_pos_list:
        pos = pos or "11"
        proc_maps = []

        if proc_code:
            proc_detail = schedule_values.get(modifier, {}).get(proc_code)
            if proc_detail:
                proc_maps.append((proc_code, modifier, proc_detail))
        elif modifier:
            proc_map = schedule_values.get(modifier, {})
            for code, detail in proc_map.items():
                if code:
                    proc_maps.append((code, modifier, detail))
        elif pos:
            for mod_key, proc_map in schedule_values.items():
                for code, detail in proc_map.items():
                    if code:
                        proc_maps.append((code, '', detail))

        for code, mod, detail in proc_maps:
            code_type = detail.get("proc_code_type", "")
            allow_amt = round(detail.get("allowed", 0), 2)
            percentage = detail.get("percentage", 0)
            fee = allow_amt if allow_amt > 0 else (percentage * 100 if percentage > 0 else 0)
            fee_type = "fee schedule" if allow_amt > 0 else "percentage" if percentage > 0 else "fee schedule"

            dict_key = (schedule_name, code, mod, pos, code_type)
            rate_dict = {
                "update_type": "A",
                "insurer_code": context.insurer_code,
                "prov_grp_contract_key": rate_key,
                "negotiation_arrangement": "ffs",
                "billing_code_type": code_type,
                "billing_code_type_ver": "10",
                "billing_code": code,
                "pos_collection_key": pos,
                "negotiated_type": fee_type,
                "rate": str(fee),
                "modifier": mod,
                "billing_class": rate_type_desc,
                "expiration_date": detail.get("termdate", DEFAULT_EXP_DATE),
                "full_term_section_id": section_id,
                "calc_bean": calc_bean
            }

            store_rate_record(
                rate_cache,
                dict_key,
                rate_dict,
                rate_key,
                rate_group_key_factory,
                (code, mod, rate_pos),
                context.shared_config.valid_service_codes
            )
