# some of these calculation functions are the same
# I still split them out into their own functions
# since they could have modifications independently

from calculations.fee_schedule import process_fee_schedule
from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from rate_group_key_factory import RateGroupKeyFactory
from rate_storage import store_rate_record
from term_bundle import TermBundle
from utilities import get_service_code_type

def process_per_item(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#per_item"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            pos = '21' if rate_type_desc == 'institutional' else '11'

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
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple)

def process_unit_ltd_by_chg(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#per_unit"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge

    fee = base_pct_of_charge * 100 if base_pct_of_charge > 0 else base_rate
    fee_type = "percentage" if base_pct_of_charge > 0 else "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            pos = '21' if rate_type_desc == 'institutional' else '11'

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
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple)

def process_percent_plus_excess(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#per_unit"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_rate = term_bundle.base_rate
    base_pct_of_charge = term_bundle.base_pct_of_charge

    fee = base_pct_of_charge * 100 if base_pct_of_charge > 0 else base_rate
    fee_type = "percentage" if base_pct_of_charge > 0 else "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            pos = '21' if rate_type_desc == 'institutional' else '11'

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
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple)

def process_visit_plus_rate_per_hour(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#per_item"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_rate = term_bundle.base_rate
    base_pct_of_charge = term_bundle.base_pct_of_charge

    fee = base_pct_of_charge * 100 if base_pct_of_charge > 0 else base_rate
    fee_type = "percentage" if base_pct_of_charge > 0 else "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            pos = '21' if rate_type_desc == 'institutional' else '11'

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
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple)

def process_flat_dollar_discount(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#per_item"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_pct_of_charge = term_bundle.base_pct_of_charge or 1.0

    fee = base_pct_of_charge * 100
    fee_type = "percentage"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            pos = '21' if rate_type_desc == 'institutional' else '11'

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
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple)

def process_ndc(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory):
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{term_bundle.rate_sheet_code}#ndc"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_pct_of_charge = term_bundle.base_pct_of_charge
    fee_type = "negotiated"

    modifier = ''
    pos = '11'
    if pos == '' or pos == '11':
        if rate_type_desc == 'institutional':
            pos = '21'

    for ndc_code, unit_price in context.shared_config.ndc_codes.items():
        fee = round(base_pct_of_charge * unit_price, 2) if base_pct_of_charge else unit_price

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": get_service_code_type(ndc_code),
            "billing_code_type_ver": "10",
            "billing_code": ndc_code,
            "pos_collection_key": pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": term_date,
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        })

        code_tuple = (ndc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, ndc_code, modifier, pos)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple)

def process_optum_physician_pricer(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    # for the professional optim pricer, we just need to use the global rate sheet
    # on the term and extract the fee schedules using the existing function
    process_fee_schedule(context, term_bundle, rate_cache, rate_group_key_factory)

