from constants import GROUPER_COLUMN_MAP, rate_template, DEFAULT_EXP_DATE
from context import Context
from rate_storage import store_rate_record
from term_bundle import TermBundle
from utilities import get_service_code_type

def process_limit(context: Context, term_bundle: TermBundle) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#limit"

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = DEFAULT_EXP_DATE
    fee = term_bundle.base_rate
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

        dict_key = (rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(context.rate_sheet_rate_cache, dict_key, rate_dict)
