import json
from datetime import datetime
import subprocess
class FioBase:
    def __init__(self):
        self.read_bandwidth: float = 0
        self.read_latency: float = 0
        self.read_iops: float = 0
        self.write_bandwidth: float = 0
        self.write_latency: float = 0
        self.write_iops: float = 0
        # self.read_percent: float = 0
        self.total_bandwidth: float = 0
        # self.total_ops: float = 0
        self.timestamp: datetime = None
        self.duration: float = 0
        self.total_iops: float = 0
        self.io_depth: int = 0
        self.jobs: int = 0
        self.ERROR_CODE = None
        self.summarize()

    def summarize(self) -> None:
        self.total_iops = self.write_iops + self.read_iops
        self.total_bandwidth = self.read_bandwidth + self.write_bandwidth

    def to_json(self) -> json:
        return json.dumps(self.__dict__)

    def __str__(self) -> str:
        return str(self.to_json())

    @staticmethod
    def prepare_args(params: dict) -> list:
        # print(params)
        # new_params = {} if params is None else params
        # new_params["--output-format"] = 'json'
        param_list = [f"{k}={v}" if v else f"{k}" for k, v in params.items()]
        # for k, v in new_params.items():
        #     param_list.append("{0}={1}".format(k, v))
        return param_list

    @staticmethod
    def run_fio(params: list) -> object:
        fio_process = subprocess.run(['fio'] + params, capture_output=True)
        print(f"Fio Return code: {fio_process}")
        return fio_process


    def parse_stdout(self, raw_stdout: str) -> None:
        try: 
            json_result = json.loads(raw_stdout)
            self.read_iops = json_result['jobs'][0]['read']['iops']
            self.read_bandwidth = json_result['jobs'][0]['read']['bw']
            self.read_latency = json_result['jobs'][0]['read']['lat']['mean']
            self.write_iops = json_result['jobs'][0]['write']['iops']
            self.write_bandwidth = json_result['jobs'][0]['write']['bw']
            self.write_latency = json_result['jobs'][0]['write']['lat']['mean']
            self.duration = json_result['jobs'][0]['elapsed']
            self.timestamp = json_result['time']
            self.summarize()
        except json.JSONDecodeError:
            raise RuntimeError('Failed to Parse FIO Output')

# region comparison methods
    def __lt__(self, other):
        return (self.total_bandwidth < other.total_bandwidth)
    
    def __le__(self, other):
        return (self.total_bandwidth <= other.total_bandwidth)
    
    def __eq__(self, other):
        return (self.total_bandwidth == other.total_bandwidth)
    
    def __ne__(self, other):
        return (self.total_bandwidth != other.total_bandwidth)
    
    def __gt__(self, other):
        return (self.total_bandwidth > other.total_bandwidth)
    
    def __ge__(self, other):
        return (self.total_bandwidth >= other.total_bandwidth)
# endregion comparison methods