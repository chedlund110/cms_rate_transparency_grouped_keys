import time

class Profiler:
    def __init__(self):
        self.timings = {}

    def start(self, label: str):
        self.timings[label] = time.perf_counter()

    def end(self, label: str, print_result=True):
        if label in self.timings:
            elapsed = time.perf_counter() - self.timings[label]
            if print_result:
                print(f"{label}: {elapsed:.4f} seconds")
            return elapsed
        return None