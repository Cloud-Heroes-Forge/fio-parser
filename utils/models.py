import json


class FioBase:
    def __init__(self):
        self.read_bandwidth: float = 0
        self.read_latency: float = 0
        self.read_iops: float = 0
        self.write_bandwidth: float = 0
        self.write_latency: float = 0
        self.write_iops: float = 0
        self.read_percent: float = 0
        self.total_bandwidth: float = 0
        self.total_ops: float = 0
        self.duration: float = 0
        self.total_iops: float = 0
        self.ERROR_CODE = None

    def summarize(self) -> None:
        self.total_iops = self.write_iops + self.read_iops
        self.total_bandwidth = self.read_bandwidth + self.write_bandwidth

    def to_json(self):
        return json.dumps(self.__dict__)

    def __str__(self):
        return str(self.to_json())
