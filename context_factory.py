from context import Context

def build_context(
    shared_config,
    networx_conn,
    qnxt_conn
):
    return Context(shared_config=shared_config,
        reporting_entity=shared_config.reporting_entity,
        reporting_entity_type=shared_config.reporting_entity_type,
        insurer_code=shared_config.insurer_code,
        program_list=shared_config.program_list,
        provider_code_range_types=shared_config.provider_code_range_types,
        service_code_range_types=shared_config.service_code_range_types,
        service_code_companion_range_types=shared_config.service_code_companion_range_types,
        networx_conn=networx_conn,
        qnxt_conn=qnxt_conn,
        mrf_target_directory=shared_config.mrf_target_directory,
        mrf_file_prefixes=shared_config.mrf_file_prefixes,
        codegroups=shared_config.codegroups,
        fee_schedules={},
        ratesheets={}
    )