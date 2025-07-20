from codegroup_tree import build_code_group_tree_from_term
from codegroup_tree import generate_service_combinations
from context import Context
from provider_bundle import ProviderBundle
from provider_exclusions import check_provider_level_exclusions
from rate_group_key_builder import build_group_keys_for_term
from rate_group_key_factory import RateGroupKeyFactory
from term_bundle import TermBundle
from fee_schedule_loader import load_fee_schedule
from calculation_router import CALCULATION_ROUTER

from utilities import get_dict_value
from utilities import get_pos_and_type
import time

def process_term(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    # terms with subterms won't have a calculation method - skip them
    # the subterms will have the calculations
    calc_bean: str = term_bundle.calc_bean
    seq_number: str = term_bundle.section_seq_number
    disabled: bool = term_bundle.disabled
    if disabled or not calc_bean or not seq_number:
        return
    
    # if a code group exists on the term and has not been populated
    # populate the code group information
    if term_bundle.service_mod_pos_list is None and (term_bundle.code_group_id or term_bundle.code_low):
        term_bundle.code_group_tree = build_code_group_tree_from_term(context, term_bundle)
        service_combinations = generate_service_combinations(context, term_bundle.code_group_tree)

        term_bundle.service_mod_pos_list = service_combinations["combinations"]
        term_bundle.has_services = service_combinations["has_services"]

    # fee schedule name - load the values - will be used in the calculation routines
    # action_parm1 contains the fee_schedule name
    # in most cases, fee schedules are only used for Outpatient
    fee_schedule_name: str = term_bundle.fee_schedule_name
    if fee_schedule_name:
        if fee_schedule_name not in context.fee_schedules:
            locality_keys = load_fee_schedule(context, fee_schedule_name)
            if locality_keys:
                term_bundle.locality_fee_schedule_keys = locality_keys
    
    # see the calculation_router module
    # each calculation type is in there
    # along with calc function to call
    calc_handler = CALCULATION_ROUTER.get(calc_bean)
    if calc_handler:
        calc_handler(context, term_bundle, rate_cache, rate_group_key_factory)

    
