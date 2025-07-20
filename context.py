from statistics_tracker import StatisticsTracker
from shared_config import SharedConfig

class Context:
    def __init__(
        self,
        shared_config,
        reporting_entity,
        reporting_entity_type,
        insurer_code,
        program_list,
        provider_code_range_types,
        service_code_range_types,
        service_code_companion_range_types,
        networx_conn,
        qnxt_conn,
        mrf_target_directory,
        mrf_file_prefixes,
        codegroups,
        fee_schedules,
        ratesheets,
        provider_identifier_output_file=None,
        prov_grp_contract_output_file=None
        ):

        self.reporting_entity = reporting_entity
        self.reporting_entity_type = reporting_entity_type
        self.insurer_code = insurer_code
        self.program_list = program_list

        self.provider_code_range_types = provider_code_range_types
        self.service_code_range_types = service_code_range_types
        self.service_code_companion_range_types = service_code_companion_range_types

        self.networx_conn = networx_conn
        self.qnxt_conn = qnxt_conn

        self.mrf_target_directory = mrf_target_directory
        self.mrf_file_prefixes = mrf_file_prefixes
        self.codegroups = {}
        self.fee_schedules = {}
        self.fee_schedule_types = {}
        self.ratesheets = {}
        self.provider_identifier_output_file = provider_identifier_output_file
        self.prov_grp_contract_output_file = prov_grp_contract_output_file
        self.shared_config = shared_config
        self.statistics = StatisticsTracker()
        self.codegroup_trees: dict[int, dict] = {}
