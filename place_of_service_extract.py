from typing import Any
from constants import FIELD_DELIM

class PlaceOfServiceExtract:

    def __init__(self, params):
        self.conn = params["db_conn"]
        self.output_file = params["output_file"]
        self.records_processed = 0

    def extract_data(self) -> None:
        query: str = "SELECT locationcode, PAYMENTRATE, description FROM hcfaposlocation"
        locations: list[dict[str,Any]] = self.conn.execute_query_with_columns(query)
        place_id: str = ""
        payment_rate: str = ""
        place_type: str = ""
        for location in locations:
            place_id = location["locationcode"].strip()
            payment_rate = location.get('paymentrate',0)
            if not payment_rate or payment_rate == "NF":
                place_type = "11"
            else:
                place_type = "21"

            line: str = ""
            if place_id:
                line = place_id + FIELD_DELIM + place_type + '\n'
                self.output_file.write(line)
                self.records_processed += 1
        