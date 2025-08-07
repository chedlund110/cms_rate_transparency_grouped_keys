from datetime import datetime
import json
import os
from typing import List

class RateSheetBatchTracker:
    def __init__(self, json_path: str):
        self.json_path = json_path
        self._load()

    def _load(self):
        if os.path.exists(self.json_path):
            with open(self.json_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        else:
            self.data = {}

    def _save(self):
        tmp_path = self.json_path + ".tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2)
        os.replace(tmp_path, self.json_path)

    def get_pending_rate_sheets(self) -> List[str]:
        return [
            code for code, info in self.data.items()
            if info['status'] in ('pending', 'failed')
        ]

    def mark_in_progress(self, ratesheet_code: str):
        self.data[ratesheet_code] = {
            "status": "in_progress",
            "last_updated": datetime.utcnow().isoformat()
        }
        self._save()

    def mark_complete(self, ratesheet_code: str):
        self.data[ratesheet_code] = {
            "status": "complete",
            "last_updated": datetime.utcnow().isoformat()
        }
        self._save()

    def mark_failed(self, ratesheet_code: str, error: str):
        self.data[ratesheet_code] = {
            "status": "failed",
            "last_updated": datetime.utcnow().isoformat(),
            "error": error
        }
        self._save()

    def initialize_from_list(self, ratesheet_codes: List[str]):
        for code in ratesheet_codes:
            if code not in self.data:
                self.data[code] = {
                    "status": "pending",
                    "last_updated": datetime.utcnow().isoformat()
                }
        self._save()
