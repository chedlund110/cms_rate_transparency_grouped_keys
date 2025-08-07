import os
from datetime import datetime
from constants import FIELD_DELIM, MAX_FILE_SIZE, rate_template
from io import BytesIO, BufferedWriter

class BufferedRateFileWriter:
    def __init__(self, target_directory: str, file_prefix: str):
        self.target_directory = os.path.normpath(target_directory)
        self.file_prefix = file_prefix
        self.closed = False
        self.current_file_index = 1
        self.current_file_size = 0
        self.records_processed = 0
        self.buffer = BytesIO()
        self._open_new_file()

    def _open_new_file(self):
        os.makedirs(self.target_directory, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.file_prefix}_{timestamp}_{self.current_file_index}.TXT"
        self.current_file_path = os.path.join(self.target_directory, filename)
        self.current_file_index += 1
        self.current_file_size = 0
        self.buffer = BytesIO()

    def write(self, rate_record_dict: dict) -> None:
        
        line = FIELD_DELIM.join(
            str(rate_record_dict.get(field, ""))
            for field in rate_template.keys()
        ) + FIELD_DELIM + rate_record_dict["full_term_display_id"] + "\n"
        """
        line = FIELD_DELIM.join(
            str(rate_record_dict.get(field, ""))
            for field in rate_template.keys()
        ) + "\n"
        """
        encoded_line = line.encode("utf-8")
        line_size = len(encoded_line)

        if self.current_file_size + line_size > MAX_FILE_SIZE:
            self.flush()
            self._open_new_file()

        self.buffer.write(encoded_line)
        self.current_file_size += line_size
        self.records_processed += 1

    def flush(self):
    # Check if buffer is present and not closed
        if not self.buffer or getattr(self.buffer, "closed", False):
            return

        if self.buffer.tell() == 0:
            return

        os.makedirs(os.path.dirname(self.current_file_path), exist_ok=True)
        with open(self.current_file_path, "ab") as f:
            with BufferedWriter(f, buffer_size=10_000_000) as buffered:
                buffered.write(self.buffer.getvalue())
                buffered.flush()

        # Reset the buffer (creates a new BytesIO to avoid reuse of a closed one)
        self.buffer.close()
        self.buffer = BytesIO()

    def flush_cache(self, rate_cache: dict) -> None:
        for group_dict in rate_cache.values():
            for rate_dict in group_dict.values():
                self.write(rate_dict)
        self.flush()

    def close_all_files(self):
        if self.closed:
            return
        self.flush()
        if self.buffer and not self.buffer.closed:
            self.buffer.close()
        self.closed = True

    def _create_mms_file(self):
        mms_filename = os.path.splitext(self.current_file_path)[0] + ".MMS"
        os.makedirs(os.path.dirname(mms_filename), exist_ok=True)
        with open(mms_filename, "w", encoding="utf-8") as f:
            f.write("Number of Records:" + str(self.records_processed))