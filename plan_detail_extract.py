from constants import FIELD_DELIM
from typing import Any
from utilities import build_in_clause_from_list

class PlanDetailExtract:

    def __init__(self, params):
        # Call the base class __init__ method
        self.conn = params['db_conn']
        self.output_file = params['output_file']
        self.program_list = params["program_list"]
        self.insurer_code = params["insurer_code"]
        self.reporting_entity = params["reporting_entity"]
        self.reporting_entity_type = params["reporting_entity_type"]
        self.plans_processed = []
        self.records_processed = 0

    def extract_data(self) -> None:
        members_ssn_set: set = self.build_mem_ssn_xref()
        #query = """
        #SELECT * FROM benefitplan
        #WHERE status = 'Active'
        #AND programid in 
        #"""
        query: str = """
        SELECT * FROM ENROLLKEYS
        WHERE GETDATE() BETWEEN effdate and termdate and 
        programid in 
        """
        
        query = query + build_in_clause_from_list(self.program_list).strip()
        enroll_keys: list[dict[str,Any]] = self.conn.execute_query_with_columns(query)
        for enroll_key in enroll_keys:
            program_id: str = enroll_key["programid"].strip()
            plan_id: str = enroll_key["planid"].strip()
            xref_id: str = program_id + FIELD_DELIM + plan_id
            if xref_id in self.plans_processed:
                continue
            
            self.plans_processed.append(xref_id)
            plan_id_type: str = ''
            eligible_org_id: str = enroll_key["eligibleorgid"]   # Group Id
            plan_market_type: str = "group" if eligible_org_id else "individual"
            rate_id: str = enroll_key["rateid"].strip()
            query: str = f"""
            SELECT ratecode FROM ratesuffixdef WHERE rateid = '{rate_id}' 
            AND GETDATE() BETWEEN effdate AND termdate 
            """.strip()

            plan_id: str = ""
            hios_id: str = ""
            rate_suffix_defs: list[dict[str,Any]] = self.conn.execute_query_with_columns(query)
            for rate_suffix_def in rate_suffix_defs:
                hios_id: str = rate_suffix_def["ratecode"].strip()
                if hios_id:
                    break

            query = f"""
            SELECT fedid FROM eligibilityorg WHERE eligibleorgid = '{eligible_org_id}'
            """
            eligibility_orgs: list[dict[str,Any]] = self.conn.execute_query_with_columns(query)
            fed_id: str = ""
            for eligibility_org in eligibility_orgs:
                fed_id = eligibility_org["fedid"].strip()
                if fed_id in members_ssn_set:
                    continue
                
                if fed_id:
                    break

            if not hios_id and not fed_id:
                continue

            if hios_id:
                plan_id = hios_id
                plan_id_type = 'HIOS'
            else:
                plan_id = fed_id
                plan_id_type = 'EIN'
                
            plan_det: str = ""
            fields = [
                self.insurer_code,
                self.insurer_code,
                program_id,
                "",
                self.reporting_entity,
                self.reporting_entity_type,
                "IO",
                "",
                plan_id,
                plan_id_type,
                self.reporting_entity,
                plan_market_type,
                self.insurer_code,
                '\n'
            ]
            plan_det = FIELD_DELIM.join(fields)
            
            self.output_file.write(plan_det)
            self.records_processed += 1

    def build_mem_ssn_xref(self) -> set:
        query = """
        select distinct ssn from member 
        left join enrollkeys
        on member.memid = enrollkeys.memid 
        where ssn is not null and programid in 
        """
        query += build_in_clause_from_list(self.program_list)
        results = self.conn.execute_query(query)
        members_ssn = {row[0].strip() for row in results}
        return members_ssn