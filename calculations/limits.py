from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from rate_group_key_factory import RateGroupKeyFactory
from rate_storage import store_rate_record
from term_bundle import TermBundle
from utilities import get_service_code_type

def process_limit(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#limit"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    base_pct_of_charge = term_bundle.base_pct_of_charge
    fee = base_rate
    fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = rate_template.copy()
        rate_dict.update({
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
        })
        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes)

def process_limit_allowed(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#limit_allowed"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = rate_template.copy()
        rate_dict.update({
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
        })

        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes)

def process_limit_allowed_percent(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#limit_allowed_pct"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = rate_template.copy()
        rate_dict.update({
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
        })

        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes)

def process_limit_allowed_same_dos(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#limit_allowed_same_dos"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = rate_template.copy()
        rate_dict.update({
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
        })

        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes)
