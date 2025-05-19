"""
calculations.py

All calculations are housed here.
Called from term_handler, process_term()

"""

from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from provider_bundle import ProviderBundle
from term_bundle import TermBundle
from file_writer import write_provider_identifiers_record
from utilities import get_dict_value, get_fee_and_type, get_pos_and_type, get_service_code_type, update_prov_grp_contract_keys

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
            allow_amt = round(proc_details.get("allowed", 0), 2)
            percentage = proc_details.get("percentage", 0)
            if allow_amt > 0:
                fee = allow_amt
                fee_type = "allowed"
            elif percentage > 0:
                fee = percentage
                fee_type = "percentage"
            else:
                fee = 0
                fee_type = "unknown"

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
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": proc_details.get("proc_code_type", ''),
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
                provider_bundle.provider_rates_temp[dict_key] = rate_dict

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
        if proc_code:
            proc_details = schedule_values.get(modifier, {}).get(proc_code, {})
            if not proc_details:
                continue
            allow_amt = round(proc_details.get("allowed", 0), 2)
            percentage = proc_details.get("percentage", 0)
            if allow_amt > 0:
                fee = allow_amt
                fee_type = "allowed"
            elif percentage > 0:
                fee = percentage
                fee_type = "percentage"
            else:
                fee = 0
                fee_type = "unknown"

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
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": proc_details.get("proc_code_type", ''),
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
                provider_bundle.provider_rates_temp[dict_key] = rate_dict

        elif modifier:
            proc_codes_by_modifier = schedule_values.get(modifier, {})
            for code, proc_details in proc_codes_by_modifier.items():
                allow_amt = round(proc_details.get("allowed", 0), 2)
                percentage = proc_details.get("percentage", 0)
                if allow_amt > 0:
                    fee = allow_amt
                    fee_type = "allowed"
                elif percentage > 0:
                    fee = percentage
                    fee_type = "percentage"
                else:
                    fee = 0
                    fee_type = "unknown"

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
                        "insurer_code": context.insurer_code,
                        "prov_grp_contract_key": rate_key,
                        "negotiation_arrangement": "ffs",
                        "billing_code_type": proc_details.get("proc_code_type", ''),
                        "billing_code": code,
                        "pos_collection_key": rate_pos,
                        "negotiated_type": fee_type,
                        "rate": str(fee),
                        "modifier": modifier,
                        "billing_class": rate_type_desc,
                        "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                        "full_term_section_id": section_id,
                        "calc_bean": calc_bean
                    }
                    provider_bundle.provider_rates_temp[dict_key] = rate_dict

        else:
            for mod, proc_codes_by_modifier in schedule_values.items():
                for code, proc_details in proc_codes_by_modifier.items():
                    allow_amt = round(proc_details.get("allowed", 0), 2)
                    percentage = proc_details.get("percentage", 0)
                    if allow_amt > 0:
                        fee = allow_amt
                        fee_type = "allowed"
                    elif percentage > 0:
                        fee = percentage
                        fee_type = "percentage"
                    else:
                        fee = 0
                        fee_type = "unknown"

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
                            "insurer_code": context.insurer_code,
                            "prov_grp_contract_key": rate_key,
                            "negotiation_arrangement": "ffs",
                            "billing_code_type": proc_details.get("proc_code_type", ''),
                            "billing_code": code,
                            "pos_collection_key": rate_pos,
                            "negotiated_type": fee_type,
                            "rate": str(fee),
                            "modifier": mod,
                            "billing_class": rate_type_desc,
                            "expiration_date": proc_details.get("termdate", DEFAULT_EXP_DATE),
                            "full_term_section_id": section_id,
                            "calc_bean": calc_bean
                        }
                        provider_bundle.provider_rates_temp[dict_key] = rate_dict


def process_percent_of_allowed(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:
    if term_bundle.service_mod_pos_list:
        process_percent_of_allowed_ranges(context, provider_bundle, term_bundle)
    else:
        process_percent_of_allowed_full(context, provider_bundle, term_bundle)


def process_percent_of_allowed_ranges(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:
    fee_schedule_name = term_bundle.fee_schedule_name
    base_pct = term_bundle.base_pct_of_charge
    section_id = term_bundle.section_id
    rate_pos, rate_type_desc = get_pos_and_type(section_name='')  # Still blank?
    calc_bean = term_bundle.calc_bean

    for proc_code, modifier, pos in term_bundle.service_mod_pos_list:
        proc_details = (
            provider_bundle.provider_rates_temp
            .get(fee_schedule_name, {})
            .get(modifier, {})
            .get(proc_code, {})
            .get(pos)
        )
        if not proc_details:
            continue

        proc_code_type = proc_details.get("proc_code_type",'')
        modifier = proc_details.get("modifier",'')
        term_date = proc_details.get("termdate",DEFAULT_EXP_DATE)
        allow_amt = round(proc_details.get("allowed", 0), 2)
        percentage = proc_details.get("percentage", 0)

        fee, fee_type = get_fee_and_type(allow_amt, percentage)

        if base_pct:
            fee = round(fee * base_pct, 2)

        rate_key = ''  # FIXME later if needed

        rate_dict = rate_template.copy()
        rate_dict.update({
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": proc_code_type,
            "billing_code": proc_code,
            "pos_collection_key": rate_pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": term_date,
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        })

        dict_key = (fee_schedule_name, proc_code, modifier, rate_pos)
        provider_bundle.provider_rates_temp[dict_key] = rate_dict


def process_percent_of_allowed_full(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:
    base_pct = term_bundle.base_pct_of_charge
    for (_, _, _, _), proc_details in provider_bundle.provider_rates_temp.items():
        rate = get_dict_value(proc_details, "rate", default=0.0)
        proc_details["rate"] = round(rate * base_pct, 2)

def process_percent_of_charges(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:
    if not term_bundle.base_pct_of_charge:
        return

    prov_id = provider_bundle.provid
    rate_sheet_code = provider_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{prov_id}#{rate_sheet_code}#{fee_schedule_name}"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    if not ranges:
        return

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge
        allow_amt = 0

        fee, fee_type = get_fee_and_type(allow_amt, percentage)

        rate_dict = rate_template.copy()
        rate_dict.update({
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": proc_code_type,
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

        dict_key = (fee_schedule_name, proc_code, modifier, pos)
        if dict_key not in provider_bundle.provider_rates_temp or provider_bundle.processing_exclusions:
                provider_bundle.provider_rates_temp[dict_key] = rate_dict


def process_case_rate(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:
    if term_bundle.disabled:
        return

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{provider_bundle.provid}#{provider_bundle.rate_sheet_code}#CASE_RATE"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    fee, fee_type = get_fee_and_type(base_rate, 0)

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        rate_dict = {
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
                    "billing_code": proc_code,
                    "pos_collection_key": pos,
                    "negotiated_type": fee_type,
                    "rate": str(fee),
                    "modifier": modifier,
                    "billing_class": rate_type_desc,
                    "expiration_date": term_date,
                    "full_term_section_id": section_id,
                    "calc_bean": calc_bean
                }

        dict_key = (provider_bundle.rate_sheet_code, proc_code, modifier, pos)
        if dict_key not in provider_bundle.provider_rates_temp or provider_bundle.processing_exclusions:
            provider_bundle.provider_rates_temp[dict_key] = rate_dict


def process_per_diem(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:

    if term_bundle.disabled:
        return

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{provider_bundle.provid}#{provider_bundle.rate_sheet_code}#PER_DIEM"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    fee, fee_type = get_fee_and_type(base_rate, 0)

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        rate_dict = {
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
                    "billing_code": proc_code,
                    "pos_collection_key": pos,
                    "negotiated_type": fee_type,
                    "rate": str(fee),
                    "modifier": modifier,
                    "billing_class": rate_type_desc,
                    "expiration_date": term_date,
                    "full_term_section_id": section_id,
                    "calc_bean": calc_bean
                }

        dict_key = (provider_bundle.rate_sheet_code, proc_code, modifier, pos)
        if dict_key not in provider_bundle.provider_rates_temp or provider_bundle.processing_exclusions:
            provider_bundle.provider_rates_temp[dict_key] = rate_dict
