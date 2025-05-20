from context import Context
from constants import DEFAULT_EXP_DATE
from rate_storage import store_rate_record
from term_bundle import TermBundle
from utilities import get_pos_and_type

def process_fee_schedule(context: Context, term_bundle: TermBundle, rate_cache: dict) -> None:
    if term_bundle.service_mod_pos_list:
        process_fee_schedule_ranges(context, term_bundle, rate_cache)
    else:
        process_fee_schedule_full(context, term_bundle, rate_cache)

def process_fee_schedule_full(context: Context, term_bundle: TermBundle, rate_cache: dict) -> None:
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')

    schedule_values = context.fee_schedules.get(fee_schedule_name, {})
    if not schedule_values:
        return

    for modifier, proc_codes in schedule_values.items():
        for proc_code, proc_details in proc_codes.items():
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

            dict_key = (fee_schedule_name, proc_code, modifier, rate_pos)
            rate_dict = {
                "update_type": "A",
                "insurer_code": context.insurer_code,
                "prov_grp_contract_key": f"{term_bundle.rate_sheet_code}#{fee_schedule_name}",
                "negotiation_arrangement": "ffs",
                "billing_code_type": proc_details.get("proc_code_type", ""),
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
            store_rate_record(rate_cache, dict_key, rate_dict)

def process_fee_schedule_ranges(context: Context, term_bundle: TermBundle, rate_cache: dict) -> None:
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')
    rate_key = f"{term_bundle.rate_sheet_code}#{fee_schedule_name}"

    schedule_values = context.fee_schedules.get(fee_schedule_name, {})
    if not schedule_values:
        return

    for proc_code, modifier, pos in term_bundle.service_mod_pos_list:
        pos = pos or '11'
        if proc_code:
            proc_details = schedule_values.get(modifier, {}).get(proc_code, {})
            if not proc_details:
                continue
        else:
            proc_details = schedule_values.get(modifier, {}).get("", {})

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
            fee = round(fee * term_bundle.base_pct_of_charge, 2)

        dict_key = (fee_schedule_name, proc_code, modifier, pos)
        rate_dict = {
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": proc_details.get("proc_code_type", ""),
            "billing_code_type_ver": "10",
            "billing_code": proc_code,
            "pos_collection_key": pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        }
        store_rate_record(rate_cache, dict_key, rate_dict)
