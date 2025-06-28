import csv
import utilities
from drg_code_extract import DRGCodeExtract
from rev_code_extract import RevCodeExtract
from proc_code_extract import ProcCodeExtract
from serv_code_extract import ServCodeExtract

class BillingCodeExtract:
    def __init__(self, db_conn, output_file):
        self.conn = db_conn
        self.output_file = output_file
        self.records_processed = 0

    def extract_data(self) -> set:
        
        rec_cnt: int = 0
        valid_service_codes: set = set()

        drg_code_extract: DRGCodeExtract = DRGCodeExtract(self.conn, self.output_file)
        rec_cnt, valid_codes = drg_code_extract.extract_data()
        self.records_processed += rec_cnt
        valid_service_codes.update(valid_codes)

        rev_code_extract: RevCodeExtract = RevCodeExtract(self.conn, self.output_file)
        rec_cnt, valid_codes = rev_code_extract.extract_data()
        self.records_processed += rec_cnt
        valid_service_codes.update(valid_codes)

        proc_code_extract: ProcCodeExtract = ProcCodeExtract(self.conn, self.output_file)
        rec_cnt, valid_codes = proc_code_extract.extract_data()
        self.records_processed += rec_cnt
        valid_service_codes.update(valid_codes)

        serv_code_extract: ServCodeExtract = ServCodeExtract(self.conn,self.output_file)
        rec_cnt, valid_codes = serv_code_extract.extract_data()
        self.records_processed += rec_cnt
        valid_service_codes.update(valid_codes)

        return valid_service_codes
