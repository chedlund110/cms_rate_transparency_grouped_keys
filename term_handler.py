from codegroup_tree import (
    build_code_group_tree_from_term,
    generate_service_combinations,
    extract_provider_ranges_from_tree
)
from context import Context
from rate_group_key_factory import RateGroupKeyFactory
from term_bundle import TermBundle
from fee_schedule_loader import load_fee_schedule
from calculation_router import CALCULATION_ROUTER
import time

def process_term(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    """
    start_time = time.perf_counter()
    print(f"Term Number: {term_bundle.full_term_display_id}")
    before_size = len(rate_cache)
    print(f"Rate Cache Size: {len(rate_cache)}")
    """
    # terms with subterms won't have a calculation method - skip them
    # the subterms will have the calculations
    calc_bean: str = term_bundle.calc_bean
    seq_number: str = term_bundle.section_seq_number
    disabled: bool = term_bundle.disabled
    if disabled or not calc_bean or not seq_number:
        return
    
    term_bundle.code_group_tree = build_code_group_tree_from_term(context, term_bundle)
    term_bundle.provider_ranges = extract_provider_ranges_from_tree(term_bundle.code_group_tree, context)
    service_combinations = generate_service_combinations(context, term_bundle.code_group_tree)

    # if a code group exists on the term and has not been populated
    # populate the code group information
    if term_bundle.service_mod_pos_list is None and (term_bundle.code_group_id or term_bundle.code_low):
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

    """
    end_time = time.perf_counter()
    after_size = len(rate_cache)
    delta = after_size - before_size
    elapsed = end_time - start_time
    print(f"Term {term_bundle.term_id} added {delta} records (total: {after_size}) in {elapsed:.2f} seconds")
    print()
    """
