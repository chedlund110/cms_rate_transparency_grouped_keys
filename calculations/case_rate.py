from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from provider_bundle import ProviderBundle
from rate_storage import store_rate_record
from term_bundle import TermBundle
from file_writer import write_provider_identifiers_record
from utilities import get_service_code_type, update_prov_grp_contract_keys


def process_case_rate(context: Context, term_bundle: TermBundle) -> None:

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#case_rate"

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    if term_bundle.base_pct_of_charge > 0:
        fee = term_bundle.base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = term_bundle.base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'
        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(context.rate_sheet_rate_cache, dict_key, rate_dict)

            
def process_case_rate_limit(context: Context, term_bundle: TermBundle) -> None:

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#case_rate"

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    if term_bundle.base_pct_of_charge > 0:
        fee = term_bundle.base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = term_bundle.base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'
        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(context.rate_sheet_rate_cache, dict_key, rate_dict)

def process_cr_ltd_by_pct_of_chg(context: Context, term_bundle: TermBundle) -> None:

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#case_rate"

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"

    fee = term_bundle.base_rate
    fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'
        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(context.rate_sheet_rate_cache, dict_key, rate_dict)

def process_case_rate_two_lev_per_diem_limit(context: Context, term_bundle: TermBundle) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#case_rate"

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    if term_bundle.base_pct_of_charge > 0:
        fee = term_bundle.base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = max(x for x in [term_bundle.base_rate, term_bundle.base_rate1] if x is not None)
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(context.rate_sheet_rate_cache, dict_key, rate_dict)

def process_case_rate_three_lev_per_diem_limit(context: Context, term_bundle: TermBundle) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#case_rate"

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    if term_bundle.base_pct_of_charge > 0:
        fee = term_bundle.base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = max(x for x in [term_bundle.base_rate, term_bundle.base_rate1, term_bundle.base_rate2] if x is not None)
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'
                
        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(context.rate_sheet_rate_cache, dict_key, rate_dict)
