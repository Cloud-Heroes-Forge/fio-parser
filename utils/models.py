class FioBase:
    def __init__(self):
        self.read_bandwidth: float = 0
        self.read_latency: float = 0
        self.read_iops: float = 0
        self.write_bandwidth: float = 0
        self.write_latency: float = 0
        self.write_iops: float = 0
        self.read_percent: float = 0
        self.total_ops = self.writes + self.reads
        self.duration: float = 0
        self.total_iops: float = 0
        self.ERROR_CODE = None

    def summarize(self):
        self.total_iops = self.write_iops + self.read_iops

