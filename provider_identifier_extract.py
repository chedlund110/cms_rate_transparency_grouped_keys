from constants import FIELD_DELIM
from utilities import build_in_clause_from_list
from typing import Any

class ProviderIdentifierExtract:
    def __init__(self, params):
        self.conn = params['db_conn']
        self.output_file = params['output_file']
        self.program_list = params["program_list"]
        self.insurer_code = params["insurer_code"]
        self.prov_grp_contract_keys = params["prov_grp_contract_keys"]
        self.records_processed = 0
        self.unique_xref_ids = []

    def extract_data(self) -> None:
        skip_cnt = 0
        
        query: str = """
        select 
        PROV.npi
        ,PROV.ssn
        ,PROV.fedid
        ,AFF.affiliationid 
        ,AFF.provid
        ,CTR.networkid
        ,CTR.contractid
        ,CTR.programid
        ,CTRNX.NxRateSheetId
        from affiliation AFF
        left join provider PROV on PROV.provid = aff.provid
        left join contractinfo CTR on CTR.affiliationid = aff.affiliationid
        left join ContractNxRateSheet CTRNX on CTRNX.ContractId = CTR.contractid
        where CTRNX.NxRateSheetId is not null
        and NxRateSheetId like 'av%'
        and prov.status = 'Active' 
        and GETDATE() BETWEEN CTR.effdate AND CTR.termdate 
	    and programid in 
        """
        query += build_in_clause_from_list(self.program_list)

        prov_clause: str = self.build_provider_clause_from_contract_keys(self.prov_grp_contract_keys)
        query += f" and prov.provid in ({prov_clause})"

        providers: list[dict[str,Any]] = self.conn.execute_query_with_columns(query)
        
        for provider in providers:
            prov_npi: str = (provider["npi"] or "").strip()
            prov_fedid: str = (provider["fedid"] or "").strip()
            prov_id: str = (provider["provid"] or "").strip()
            contract_id: str = (provider["contractid"] or "").strip()
            program_id: str = (provider["programid"] or "").strip()
            rate_sheet_code: str = (provider["NxRateSheetId"] or "").strip()

            for prov_grp_contract_key in self.prov_grp_contract_keys[prov_id]:
                program_id, rate_id = prov_grp_contract_key.split("|")
                if rate_id in self.unique_xref_ids:
                    continue
                self.unique_xref_ids.append(rate_id)
                line: str = ""
                fields = [
                    "A",
                    self.insurer_code,
                    prov_id,
                    prov_id,
                    prov_fedid,
                    prov_npi,
                    rate_id,
                    "99991231",
                    '\n'
                ]
                line = FIELD_DELIM.join(fields)

                self.output_file.write(line)
                self.records_processed += 1

    def build_provider_clause_from_contract_keys(self, prov_grp_contract_keys: dict) -> str:
        provider_ids = list(prov_grp_contract_keys.keys())
        provider_ids_str = ', '.join(f"'{provider_id}'" for provider_id in provider_ids)
        return provider_ids_str