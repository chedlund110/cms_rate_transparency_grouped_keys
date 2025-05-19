from typing import Any, Dict, Optional
from decimal import Decimal, ROUND_HALF_UP

class TermBundle:
    def __init__(self, term: Dict[str, Any], parent_code_group_id: Optional[int] = 0, rate_type_desc: Optional[str] = 'professional') -> None:
        self.calc_bean: str = (term.get("CALCBEAN") or "").strip()
        self.fee_schedule_name: str = (term.get("ACTIONPARM1") or "").strip()

        self.base_rate: float = self._get_float(term, "BASERATE")
        self.base_rate1: float = self._get_float(term, "BASERATE1")
        self.base_rate2: float = self._get_float(term, "BASERATE2")
        self.base_pct_of_charge: float = self._get_float(term, "BASEPERCENTOFCHGS")
        self.secondary_percent_of_chgs: float = self._get_float(term,"SECONDARYPERCENTOFCHGS")
        self.other_percent_of_chgs: float = self._get_float(term,"OTHERPERCENTOFCHGS")
        self.other_percent_of_chgs1: float = self._get_float(term,"OTHERPERCENTOFCHGS1")
        self.outlier: float = self._get_float(term,"OUTLIER")
        self.outlier_percentage: float = self._get_float(term,"OUTLIERPERCENTAGE")
        self.per_diem: float = self._get_float(term, "PERDIEM")
        self.user_field1: float = self._get_float(term, "USERFIELD1")

        code_group_id_raw = term.get("CODEGROUPID")
        self.code_group_id: Optional[int] = int(code_group_id_raw) if code_group_id_raw else 0

        self.code_low: str = (term.get("CODELOWVALUE") or "").strip()
        self.code_high: str = (term.get("CODEHIGHVALUE") or "").strip()
        self.code_type: str = (term.get("CODETYPEBEAN") or "").strip()

        self.term_id: int = int(term.get("RATESHEETTERMID") or 0)
        self.term: dict = term
        self.rate_type_desc = rate_type_desc
        self.display_section_number: int = int(term.get("DISPLAYSECTIONNUMBER") or 0)
        self.section_seq_number: int = int(term.get("SEQNUMBER") or 0)
        self.parent_section_number: int = int(term.get("PARENTSECTIONNUMBER") or 0)
        self.parent_seq_number: int = int(term.get("PARENTSEQNUMBER") or 0)

        self.parent_code_group_id = parent_code_group_id
        self.section_id: str = self._build_section_id()
        self.qual_type: str = "G" if self.code_group_id else "T"
        self.disabled: int = int(term.get("DISABLED") or 0)

        self.subterms: Optional[list["TermBundle"]] = None
        self.service_mod_pos_list: Optional[Dict[str, Any]] = None
        self.has_services: Optional[bool]
        self.code_group_tree: Optional[dict] = None

    def _get_decimal(self, term: Dict[str, Any], key: str) -> Decimal:
        raw = term.get(key, "")
        if raw == "":
            return Decimal("0")
        return Decimal(str(raw)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    
    def _get_float(self, term: Dict[str, Any], key: str) -> float:
        raw = term.get(key, "")
        if isinstance(raw, Decimal):
            return float(raw)
        if raw == "":
            return float("0")
        return float(str(raw))

    def _build_section_id(self) -> str:
        return f"{self.display_section_number}-{self.section_seq_number}"