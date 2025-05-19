from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from provider_bundle import ProviderBundle
from rate_storage import store_rate_record
from term_bundle import TermBundle
from file_writer import write_provider_identifiers_record
from utilities import get_pos_and_type, update_prov_grp_contract_keys

def process_fee_schedule(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:
    fee_schedule_name = term_bundle.fee_schedule_name
    if fee_schedule_name in provider_bundle.provider_fee_schedules:
        if not provider_bundle.processing_exclusions:
            return
    else:
        provider_bundle.provider_fee_schedules.append(fee_schedule_name)

    if term_bundle.service_mod_pos_list:
        process_fee_schedule_ranges(context, provider_bundle, term_bundle)
    else:
        process_fee_schedule_full(context, provider_bundle, term_bundle)

def process_fee_schedule_full(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:
    prov_id = provider_bundle.provid
    fee_schedule_name = term_bundle.fee_schedule_name
    rate_sheet_code = provider_bundle.rate_sheet_code
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')

    rate_key = f"{prov_id}#{rate_sheet_code}#{fee_schedule_name}"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    schedule_values = context.fee_schedules.get(fee_schedule_name, {})
    if not schedule_values:
        return

    for modifier, proc_codes in schedule_values.items():
        for proc_code, proc_details in proc_codes.items():
            allow_amt = round(proc_details.get("allowed"), 2)
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
                allow_amt = round(allow_amt * term_bundle.base_pct_of_charge, 2)

            dict_key = (fee_schedule_name, proc_code, modifier, rate_pos)
            if dict_key in provider_bundle.provider_rates_temp:
                provider_bundle.provider_rates_temp[dict_key].update({
                    "rate": str(fee),
                    "negotiated_type": fee_type,
                    "modifier": modifier,
                    "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                    "calc_bean": calc_bean
                })
            elif provider_bundle.processing_exclusions or dict_key not in provider_bundle.provider_rates_temp:
                rate_dict = {
                    "update_date": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": proc_details.get("proc_code_type", ''),
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
                store_rate_record(provider_bundle, dict_key, rate_dict)

def process_fee_schedule_ranges(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:
    prov_id = provider_bundle.provid
    fee_schedule_name = term_bundle.fee_schedule_name
    rate_sheet_code = provider_bundle.rate_sheet_code
    section_id = term_bundle.section_id
    calc_bean = term_bundle.calc_bean
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')
    rate_key = f"{prov_id}#{rate_sheet_code}#{fee_schedule_name}"

    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    schedule_values = context.fee_schedules.get(fee_schedule_name, {})
    if not schedule_values:
        return

    for proc_code, modifier, pos in term_bundle.service_mod_pos_list:
        if pos == '':
            pos = '11'
        if proc_code:
            proc_details = schedule_values.get(modifier, {}).get(proc_code, {})
            if not proc_details:
                continue
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
            if dict_key in provider_bundle.provider_rates_temp:
                provider_bundle.provider_rates_temp[dict_key].update({
                    "rate": str(fee),
                    "negotiated_type": fee_type,
                    "modifier": modifier,
                    "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                    "calc_bean": calc_bean
                })
            elif provider_bundle.processing_exclusions or dict_key not in provider_bundle.provider_rates_temp:
                rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": proc_details.get("proc_code_type", ''),
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
                store_rate_record(provider_bundle, dict_key, rate_dict)

        elif modifier:
            proc_codes_by_modifier = schedule_values.get(modifier, {})
            for code, proc_details in proc_codes_by_modifier.items():
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

                dict_key = (fee_schedule_name, code, modifier, pos)
                if dict_key in provider_bundle.provider_rates_temp:
                    provider_bundle.provider_rates_temp[dict_key].update({
                        "rate": str(fee),
                        "negotiated_type": fee_type,
                        "modifier": modifier,
                        "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                        "calc_bean": calc_bean
                    })
                elif provider_bundle.processing_exclusions or dict_key not in provider_bundle.provider_rates_temp:
                    rate_dict = {
                        "update_type": "A",
                        "insurer_code": context.insurer_code,
                        "prov_grp_contract_key": rate_key,
                        "negotiation_arrangement": "ffs",
                        "billing_code_type": proc_details.get("proc_code_type", ''),
                        "billing_code_type_ver": "10",
                        "billing_code": code,
                        "pos_collection_key": pos,
                        "negotiated_type": fee_type,
                        "rate": str(fee),
                        "modifier": modifier,
                        "billing_class": rate_type_desc,
                        "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                        "full_term_section_id": section_id,
                        "calc_bean": calc_bean
                    }
                    store_rate_record(provider_bundle, dict_key, rate_dict)

        else:
            for mod, proc_codes_by_modifier in schedule_values.items():
                for code, proc_details in proc_codes_by_modifier.items():
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

                    dict_key = (fee_schedule_name, code, mod, pos)
                    if dict_key in provider_bundle.provider_rates_temp:
                        provider_bundle.provider_rates_temp[dict_key].update({
                            "rate": str(fee),
                            "negotiated_type": fee_type,
                            "modifier": mod,
                            "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                            "calc_bean": calc_bean
                        })
                    elif provider_bundle.processing_exclusions or dict_key not in provider_bundle.provider_rates_temp:
                        rate_dict = {
                            "update_type": "A",
                            "insurer_code": context.insurer_code,
                            "prov_grp_contract_key": rate_key,
                            "negotiation_arrangement": "ffs",
                            "billing_code_type": proc_details.get("proc_code_type", ''),
                            "billing_code_type_ver": "10",
                            "billing_code": code,
                            "pos_collection_key": pos,
                            "negotiated_type": fee_type,
                            "rate": str(fee),
                            "modifier": mod,
                            "billing_class": rate_type_desc,
                            "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                            "full_term_section_id": section_id,
                            "calc_bean": calc_bean
                        }
                        store_rate_record(provider_bundle, dict_key, rate_dict)

