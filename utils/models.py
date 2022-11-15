class FioRun:
    def __init__(self):
        self.reads: int = 0
        self.rd_bandwidth: float = 0
        self.rd_latency: float = 0
        self.writes: int = 0
        self.wrt_bandwidth: float = 0
        self.wrt_latency: float = 0
        self.read_percent: float = 0
