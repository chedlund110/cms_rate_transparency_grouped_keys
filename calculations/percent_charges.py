from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from provider_bundle import ProviderBundle
from rate_group_key_factory import RateGroupKeyFactory
from rate_storage import store_rate_record
from term_bundle import TermBundle
from file_writer import write_provider_identifiers_record
from utilities import get_fee_and_type, get_service_code_type, update_prov_grp_contract_keys

def process_percent_of_charges(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    if not term_bundle.base_pct_of_charge:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#pct_chgs"

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

        if percentage > 0:
            fee = percentage * 100
            fee_type = "percentage"
        else:
            fee = allow_amt
            fee_type = "negotiated"

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

def process_pct_of_chrg_flat_amt(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#{fee_schedule_name}"

    if not ranges:
        return

    if base_pct_of_charge:
        fee = base_pct_of_charge * 100
        fee_type = 'percentage'
    else:
        fee = term_bundle.base_rate1
        fee_type = 'negotiated'

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge

        fee, fee_type = get_fee_and_type(fee, percentage)

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

def process_percent_of_charges_max(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#{fee_schedule_name}"

    if not ranges:
        return

    fee = term_bundle.base_rate
    fee_type = 'negotiated'

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge

        fee, fee_type = get_fee_and_type(fee, percentage)

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

def process_percent_of_charges_max_01(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#{fee_schedule_name}"

    if not ranges:
        return

    fee = term_bundle.base_rate
    fee_type = 'negotiated'

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge

        fee, fee_type = get_fee_and_type(fee, percentage)

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

def process_pct_chg_pd_max(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#{fee_schedule_name}"

    if not ranges:
        return

    fee = term_bundle.base_rate
    fee_type = 'negotiated'

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge

        fee, fee_type = get_fee_and_type(fee, percentage)

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

def process_pct_chg_pd_max_01(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#{fee_schedule_name}"

    if not ranges:
        return

    fee = term_bundle.base_rate
    fee_type = 'negotiated'

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge

        fee, fee_type = get_fee_and_type(fee, percentage)

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


def process_pct_chg_per_proc_max(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#{fee_schedule_name}"

    if not ranges:
        return

    fee = term_bundle.base_rate
    fee_type = 'negotiated'

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge

        fee, fee_type = get_fee_and_type(fee, percentage)

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

def process_pct_chg_per_unit_threshold(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#{fee_schedule_name}"

    if not ranges:
        return

    fee = base_pct_of_charge * 100
    fee_type = 'percentage'

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge

        fee, fee_type = get_fee_and_type(fee, percentage)

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

def process_percent_threshold(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_sheet_code = term_bundle.rate_sheet_code
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    base_pct_of_charge = term_bundle.base_pct_of_charge
    ranges = term_bundle.service_mod_pos_list

    rate_key = f"{rate_sheet_code}#{fee_schedule_name}"

    if not ranges:
        return

    fee = base_pct_of_charge * 100
    fee_type = 'percentage'

    for proc_code, modifier, pos in ranges:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        proc_code_type = get_service_code_type(proc_code)
        term_date = "99991231"
        percentage = base_pct_of_charge

        fee, fee_type = get_fee_and_type(fee, percentage)

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