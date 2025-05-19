# shared_config.py

from typing import Any

class SharedConfig:
    def __init__(
        self,
        reporting_entity: str,
        reporting_entity_type: str,
        insurer_code: str,
        program_list: list[str],
        provider_code_range_types: list[str],
        service_code_range_types: list[str],
        service_code_companion_range_types: list[str],
        mrf_target_directory: str,
        mrf_file_prefixes: list[str],
        provider_identifier_full_path: str,
        prov_grp_contract_full_path: str,
        networx_connection_string: str,
        qnxt_connection_string: str,
        directory_structure: dict = {}
    ):
        self.reporting_entity = reporting_entity
        self.reporting_entity_type = reporting_entity_type
        self.insurer_code = insurer_code
        self.program_list = program_list

        self.provider_code_range_types = provider_code_range_types
        self.service_code_range_types = service_code_range_types
        self.service_code_companion_range_types = service_code_companion_range_types
        self.mrf_target_directory = mrf_target_directory
        self.mrf_file_prefixes = mrf_file_prefixes
        self.provider_identifier_full_path = provider_identifier_full_path
        self.prov_grp_contract_full_path = prov_grp_contract_full_path
        self.networx_connection_string = networx_connection_string
        self.qnxt_connection_string = qnxt_connection_string
        self.directory_structure = directory_structure