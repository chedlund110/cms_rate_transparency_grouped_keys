import os
from datetime import datetime
from constants import FIELD_DELIM, MAX_FILE_SIZE, rate_template
from io import BytesIO, BufferedWriter

class RateFileWriter:
    def __init__(self, target_directory: str, file_prefix: str):
        self.target_directory = target_directory
        self.file_prefix = file_prefix
        self.current_file_index = 1
        self.current_file_size = 0
        self.records_processed = 0
        self.buffer = BytesIO()
        self._open_new_file()

    def _open_new_file(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.file_prefix}_{timestamp}_{self.current_file_index}.TXT"
        self.current_file_path = os.path.join(self.target_directory, filename)
        self.current_file_index += 1
        self.current_file_size = 0
        self.buffer = BytesIO()

    def write(self, rate_record_dict: dict) -> None:
        rate_record_line = FIELD_DELIM.join(
            str(rate_record_dict.get(field, ""))
            for field in rate_template.keys()
        ) + FIELD_DELIM + "\n"

        encoded_line = rate_record_line.encode("utf-8")
        self.current_file_size += len(encoded_line)
        self.buffer.write(encoded_line)
        self.records_processed += 1

        if self.current_file_size >= 100_000_000:
            self.flush()
            self._open_new_file()

    def flush(self):
        if self.buffer.tell() == 0:
            return

        with open(self.current_file_path, "ab") as f:
            with BufferedWriter(f, buffer_size=10_000_000) as buffered:
                buffered.write(self.buffer.getvalue())
                buffered.flush()
        self.buffer = BytesIO()

    def close_all_files(self):
        self.flush()
