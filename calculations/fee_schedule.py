from context import Context
from constants import DEFAULT_EXP_DATE
from rate_group_key_factory import RateGroupKeyFactory
from rate_storage import store_rate_record
from term_bundle import TermBundle
from utilities import get_pos_and_type

def process_fee_schedule(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    if term_bundle.service_mod_pos_list:
        process_fee_schedule_ranges(context, term_bundle, rate_cache, rate_group_key_factory)
    else:
        process_fee_schedule_full(context, term_bundle, rate_cache, rate_group_key_factory)

def process_fee_schedule_full(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')

    schedule_values = context.fee_schedules.get(fee_schedule_name, {})
    if not schedule_values:
        return

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

            dict_key = (fee_schedule_name, proc_code, modifier, rate_pos, code_type)
            rate_dict = {
                "update_type": "A",
                "insurer_code": context.insurer_code,
                "prov_grp_contract_key": f"{term_bundle.rate_sheet_code}#{fee_schedule_name}",
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
            rate_key: str = f"{term_bundle.rate_sheet_code}#{fee_schedule_name}"
            code_tuple = (proc_code, modifier, rate_pos)
            store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes)

def process_fee_schedule_ranges(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')
    rate_key = f"{term_bundle.rate_sheet_code}#{fee_schedule_name}"

    schedule_values = context.fee_schedules.get(fee_schedule_name, {})
    if not schedule_values:
        return

    for proc_code, modifier, pos, code_type in term_bundle.service_mod_pos_list:
        pos = pos or '11'

        # CASE 1: Fully specified proc_code
        if proc_code:
            proc_details = schedule_values.get(modifier, {}).get(proc_code)
            if not proc_details:
                continue

            code_type = proc_details.get("proc_code_type", "")
            allow_amt = round(proc_details.get("allowed", 0), 2)
            percentage = proc_details.get("percentage", 0)
            fee = allow_amt if allow_amt > 0 else (percentage * 100 if percentage > 0 else 0)
            fee_type = "fee schedule" if allow_amt > 0 else "percentage" if percentage > 0 else "fee schedule"

            dict_key = (fee_schedule_name, proc_code, modifier, pos, code_type)
            rate_dict = {
                "update_type": "A",
                "insurer_code": context.insurer_code,
                "prov_grp_contract_key": rate_key,
                "negotiation_arrangement": "ffs",
                "billing_code_type": code_type,
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
            store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, (proc_code, modifier, rate_pos), context.shared_config.valid_service_codes)

        # CASE 2: No proc_code, but valid modifier (modifier-only combo)
        elif modifier:
            proc_map = schedule_values.get(modifier, {})
            for procedure_code, proc_details_for_code in proc_map.items():
                if not procedure_code:
                    continue
                code_type = proc_details_for_code.get("proc_code_type", "")
                allow_amt = round(proc_details_for_code.get("allowed", 0), 2)
                percentage = proc_details_for_code.get("percentage", 0)
                fee = allow_amt if allow_amt > 0 else (percentage * 100 if percentage > 0 else 0)
                fee_type = "fee schedule" if allow_amt > 0 else "percentage" if percentage > 0 else "fee schedule"

                dict_key = (fee_schedule_name, procedure_code, modifier, pos, code_type)
                rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": code_type,
                    "billing_code_type_ver": "10",
                    "billing_code": procedure_code,
                    "pos_collection_key": pos,
                    "negotiated_type": fee_type,
                    "rate": str(fee),
                    "modifier": modifier,
                    "billing_class": rate_type_desc,
                    "expiration_date": proc_details_for_code.get("termdate", DEFAULT_EXP_DATE),
                    "full_term_section_id": section_id,
                    "calc_bean": calc_bean
                }
                store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, (procedure_code, modifier, rate_pos), context.shared_config.valid_service_codes)

        # CASE 3: POS-only — no proc_code and no modifier
        elif pos and not proc_code and not modifier:
            for mod_key, proc_map in schedule_values.items():
                for procedure_code, proc_details_for_code in proc_map.items():
                    if not procedure_code:
                        continue
                    code_type = proc_details_for_code.get("proc_code_type", "")
                    allow_amt = round(proc_details_for_code.get("allowed", 0), 2)
                    percentage = proc_details_for_code.get("percentage", 0)
                    fee = allow_amt if allow_amt > 0 else (percentage * 100 if percentage > 0 else 0)
                    fee_type = "fee schedule" if allow_amt > 0 else "percentage" if percentage > 0 else "fee schedule"

                    dict_key = (fee_schedule_name, procedure_code, '', pos, code_type)
                    rate_dict = {
                        "update_type": "A",
                        "insurer_code": context.insurer_code,
                        "prov_grp_contract_key": rate_key,
                        "negotiation_arrangement": "ffs",
                        "billing_code_type": code_type,
                        "billing_code_type_ver": "10",
                        "billing_code": procedure_code,
                        "pos_collection_key": pos,
                        "negotiated_type": fee_type,
                        "rate": str(fee),
                        "modifier": '',
                        "billing_class": rate_type_desc,
                        "expiration_date": proc_details_for_code.get("termdate", DEFAULT_EXP_DATE),
                        "full_term_section_id": section_id,
                        "calc_bean": calc_bean
                    }
                    store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, (procedure_code, '', rate_pos), context.shared_config.valid_service_codes)

"""
def process_fee_schedule_ranges(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')
    rate_key = f"{term_bundle.rate_sheet_code}#{fee_schedule_name}"

    schedule_values = context.fee_schedules.get(fee_schedule_name, {})
    if not schedule_values:
        return

    for proc_code, modifier, pos, code_type in term_bundle.service_mod_pos_list:
        pos = pos or '11'
        if proc_code:
            # Get procedure details for specific proc_code
            proc_details = schedule_values.get(modifier, {}).get(proc_code, {})
            if not proc_details:
                continue
        else:
            # When proc_code is blank, we need to add every procedure code under modifier
            proc_details = schedule_values.get(modifier, {})
            if not proc_details:
                continue

            # Iterate through all procedure codes under the modifier and add them to the cache
            for procedure_code in schedule_values.get(modifier, {}).keys():
                if procedure_code:  # Ignore empty keys
                    proc_details_for_code = schedule_values[modifier].get(procedure_code, {})
                    code_type = proc_details_for_code.get("proc_code_type", "")
                    allow_amt = round(proc_details_for_code.get("allowed", 0), 2)
                    percentage = proc_details_for_code.get("percentage", 0)
                    
                    # Cache this procedure code's details
                    dict_key = (fee_schedule_name, procedure_code, modifier, pos, code_type)
                    rate_dict = {
                        "update_type": "A",
                        "insurer_code": context.insurer_code,
                        "prov_grp_contract_key": rate_key,
                        "negotiation_arrangement": "ffs",
                        "billing_code_type": code_type,
                        "billing_code_type_ver": "10",
                        "billing_code": procedure_code,
                        "pos_collection_key": pos,
                        "negotiated_type": "fee schedule" if allow_amt > 0 else "percentage",
                        "rate": str(allow_amt if allow_amt > 0 else percentage * 100),
                        "modifier": modifier,
                        "billing_class": rate_type_desc,
                        "expiration_date": proc_details_for_code.get("termdate", DEFAULT_EXP_DATE),
                        "full_term_section_id": section_id,
                        "calc_bean": calc_bean
                    }
                    store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, (procedure_code, modifier, rate_pos), context.shared_config.valid_service_codes)

        # After processing all procedure codes under the modifier, process the specific proc_code and fallback
        if proc_code and proc_details:
            code_type = proc_details.get("proc_code_type", "")
            allow_amt = round(proc_details.get("allowed", 0), 2)
            percentage = proc_details.get("percentage", 0)
            fee = allow_amt if allow_amt > 0 else (percentage * 100 if percentage > 0 else 0)
            fee_type = "fee schedule" if allow_amt > 0 else "percentage" if percentage > 0 else "fee schedule"

            # Store the rate record for the specific proc_code
            dict_key = (fee_schedule_name, proc_code, modifier, pos, code_type)
            rate_dict = {
                "update_type": "A",
                "insurer_code": context.insurer_code,
                "prov_grp_contract_key": rate_key,
                "negotiation_arrangement": "ffs",
                "billing_code_type": code_type,
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
            store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, (proc_code, modifier, rate_pos), context.shared_config.valid_service_codes)
"""