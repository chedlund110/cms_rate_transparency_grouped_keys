import time

class RunStatsLogger:
    def __init__(self, total_providers: int, log_path: str = "run_summary.log"):
        self.total_providers = total_providers
        self.providers_done = 0
        self.start_time = time.time()
        self.log_path = log_path
        with open(self.log_path, 'w') as f:
            f.write(f"Run started: {time.ctime(self.start_time)}\n\n")

    def log_provider(self, provider_id: str, rate_count: int = None):
        self.providers_done += 1
        elapsed = time.time() - self.start_time
        remaining = self.total_providers - self.providers_done
        eta = (elapsed / self.providers_done) * remaining if self.providers_done else 0
        rate_info = f", {rate_count} rates" if rate_count is not None else ""

        msg = (f"[{self.providers_done}/{self.total_providers}] "
               f"Provider {provider_id}{rate_info} - "
               f"Elapsed: {elapsed/60:.1f} min, ETA: {eta/60:.1f} min\n")

        print(msg.strip())  # Optional console output
        with open(self.log_path, 'a') as f:
            f.write(msg)

    def finalize(self):
        total_time = time.time() - self.start_time
        msg = f"\nRun completed in {total_time/3600:.2f} hours ({total_time:.1f} sec).\n"
        print(msg.strip())
        with open(self.log_path, 'a') as f:
            f.write(msg)