class StatisticsTracker:
    def __init__(self):
        self.negotiated_rate_count = 0
        self.provider_identifier_count = 0
        self.prov_grp_contract_xref_count = 0

    def increment_negotiated_rate(self):
        self.negotiated_rate_count += 1

    def increment_provider_identifier(self):
        self.provider_identifier_count += 1

    def increment_prov_grp_contract_xref(self):
        self.prov_grp_contract_xref_count += 1

    def summary(self):
        return {
            "negotiated_rate_count": self.negotiated_rate_count,
            "provider_identifier_count": self.provider_identifier_count,
            "prov_grp_contract_xref_count": self.prov_grp_contract_xref_count,
        }