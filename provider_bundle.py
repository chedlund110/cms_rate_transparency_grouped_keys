from typing import Any
import re

class ProviderBundle:
    def __init__(self, provider_data: dict[str, Any]) -> None:
        # Core identifiers
        self.provid: str = (provider_data.get("provid") or "").strip()
        self.fedid: str = (provider_data.get("fedid") or "").strip()
        self.prov_ssn: str = (provider_data.get("ssn") or "").strip()
        self.npi: str = (provider_data.get("npi") or "").strip()
        self.taxonomy: str = (provider_data.get("taxonomy") or "").strip()
        self.fullname: str = (provider_data.get("fullname") or "").strip()

        # Contract and program context
        self.program_id: str = (provider_data.get("programid") or "").strip()
        self.program_list: list[str] = []
        self.contract_id: str = (provider_data.get("contractid") or "").strip()
        self.rate_sheet_code: str = (provider_data.get("NxRateSheetId") or "").strip()
        self.network_id: str = (provider_data.get("networkid") or "").strip()
        self.affiliation_id: str = (provider_data.get("affiliationid") or "").strip()

        # Address or other optional fields
        zip_raw = (provider_data.get("prov_zip") or "").strip()
        self.zip: str = re.match(r"\d{5}", zip_raw).group(0) if re.match(r"\d{5}", zip_raw) else ""
        self.provtype: str = (provider_data.get("provtype") or "").strip()

        # Dynamic data structures (used during processing)
        self.provider_fee_schedules: list[str] = []
        self.procedure_fee_schedule_xref: dict[str, list[str]] = {}
        self.provider_rates_temp: dict[tuple, dict[str, Any]] = {}
        self.proc_code_amount_xref: dict[str, dict[str, Any]] = {}
        self.rate_write_batch: list[dict] = []
        self.prov_grp_contract_keys: dict = {}
        self.provider_rate_keys = []
        self.processing_exclusions: bool = False