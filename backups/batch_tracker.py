from datetime import datetime
from typing import Optional


class BatchTracker:
    def get_pending_providers(self) -> list[str]:
        raise NotImplementedError

    def mark_in_progress(self, provider_id: str):
        raise NotImplementedError

    def mark_complete(self, provider_id: str):
        raise NotImplementedError

    def mark_failed(self, provider_id: str, error: str):
        raise NotImplementedError
