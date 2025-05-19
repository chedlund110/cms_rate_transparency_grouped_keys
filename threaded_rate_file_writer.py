import os
import threading
import queue
from datetime import datetime
from constants import FIELD_DELIM, MAX_FILE_SIZE, rate_template

class ThreadedRateFileWriter:
    def __init__(self, target_directory: str, file_prefix: str):
        self.target_directory = target_directory
        self.file_prefix = file_prefix
        self.queue = queue.Queue()
        self.shutdown_flag = threading.Event()
        self.writer_thread = threading.Thread(target=self._background_writer, daemon=True)

        self.current_file_index = 1
        self.current_file_size = 0
        self.records_processed = 0
        self.current_file = None
        self._open_new_file()
        self.writer_thread.start()

    def _open_new_file(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.file_prefix}_{timestamp}_{self.current_file_index}.TXT"
        self.current_file_path = os.path.join(self.target_directory, filename)
        self.current_file_index += 1
        self.current_file_size = 0
        self.current_file = open(self.current_file_path, "ab")

    def write(self, rate_record_dict: dict) -> None:
        """Optionally buffered single-record writer."""
        encoded_line = self._encode_record(rate_record_dict)
        self.queue.put([encoded_line])  # wrap in list for consistency
        self.records_processed += 1

    def write_batch(self, batch_of_dicts: list[dict]) -> None:
        """Batch version for high-throughput writing."""
        encoded_lines = [self._encode_record(d) for d in batch_of_dicts]
        self.queue.put(encoded_lines)
        self.records_processed += len(batch_of_dicts)

    def _encode_record(self, record: dict) -> bytes:
        line = FIELD_DELIM.join(str(record.get(field, "")) for field in rate_template.keys()) + FIELD_DELIM + "\n"
        return line.encode("utf-8")

    def _background_writer(self):
        while not self.shutdown_flag.is_set() or not self.queue.empty():
            try:
                encoded_batch = self.queue.get(timeout=0.5)
                for encoded_line in encoded_batch:
                    self.current_file.write(encoded_line)
                    self.current_file_size += len(encoded_line)

                if self.current_file_size >= MAX_FILE_SIZE:
                    self.current_file.flush()
                    self.current_file.close()
                    self._open_new_file()
            except queue.Empty:
                continue
        self.current_file.flush()
        self.current_file.close()

    def close_all_files(self):
        self.shutdown_flag.set()
        self.writer_thread.join()