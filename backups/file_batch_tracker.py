from datetime import datetime
import json
import os

from batch_tracker import BatchTracker

class FileBatchTracker(BatchTracker):
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

    def get_pending_providers(self) -> list[str]:
        return [pid for pid, info in self.data.items()
                if info['status'] in ('pending', 'failed')]

    def mark_in_progress(self, provider_id: str):
        self.data[provider_id] = {
            "status": "in_progress",
            "last_updated": datetime.utcnow().isoformat()
        }
        self._save()

    def mark_complete(self, provider_id: str):
        self.data[provider_id] = {
            "status": "complete",
            "last_updated": datetime.utcnow().isoformat()
        }
        self._save()

    def mark_failed(self, provider_id: str, error: str):
        self.data[provider_id] = {
            "status": "failed",
            "last_updated": datetime.utcnow().isoformat(),
            "error": error
        }
        self._save()

    def initialize_from_list(self, provider_ids: list[str]):
        for pid in provider_ids:
            if pid not in self.data:
                self.data[pid] = {"status": "pending", "last_updated": datetime.utcnow().isoformat()}
        self._save()
