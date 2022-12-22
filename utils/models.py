import json
from datetime import datetime
import subprocess
from utils.converters import average
from typing import Any
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
            print(self)
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

class FioOptimizer:
    def __init__(self,
                 runs: dict = None,
                 best_run: FioBase = None,
                 optimal_queue_depth: int = 0,
                 config: dict = None, 
                 min: int = 1,
                 max: int = 65536, 
                 slices: int = 5):

        self.runs: dict[int, FioBase] = {} if runs is None else runs
        self.config: dict[str, Any] = {} if config is None else config
        self.best_run: FioBase = best_run
        self.optimal_queue_depth: int = optimal_queue_depth
        self.min: int = min
        self.max: int = max
        self.slices: int = slices

    def find_optimal_iodepth(self) -> None:
        is_optimial: bool = False
        starting_io_depths: list = [self.min, self.max]    # gotta start some where
        tested_iodepths: list[int] = []
        for io_depth in starting_io_depths:
            self.config['--iodepth'] = io_depth
            fio_run = self.prepare_and_run_fio(io_depth=io_depth)
            self.runs[io_depth] = fio_run
            tested_iodepths.append(io_depth)

        while not is_optimial: 
           
            if (self.max - self.min) <= 1:
                self.best_run = self.runs[self.max] if self.runs[self.max].total_iops > self.runs[self.min].total_iops else self.runs[self.min] 
                is_optimial: bool = True
            else:
                for next_iodepth in range(self.min, self.max, max(abs((self.max - self.min)//self.slices),1)):
                # next_iodepth: int = floor(average(max.io_depth, min.io_depth))
                    if next_iodepth in tested_iodepths or next_iodepth <= 0:
                        continue
                    self.config['--iodepth'] = next_iodepth
                    fio_run = self.prepare_and_run_fio(io_depth=next_iodepth)
                    self.runs[next_iodepth] = fio_run
                    tested_iodepths.append(next_iodepth)
                    if average(self.runs[self.min].total_iops, fio_run.total_iops) > average(fio_run.total_iops, self.runs[self.max].total_iops):
                        self.max = fio_run.io_depth
                    else: 
                        self.min = fio_run.io_depth

    def prepare_and_run_fio(self, io_depth: int) -> FioBase:
        print("Running Test with IO Depth = {0}".format(io_depth))
        param_list = [f"{k}={v}" if v else f"{k}" for k, v in self.config.items()]
        # print(f"args: {param_list}")
        # fio_run_process: object = FioBase.run_fio(params=param_list)
        fio_run_process = subprocess.run(['fio'] + param_list, capture_output=True)
        fio_run: FioBase = FioBase()
        fio_run.io_depth = io_depth
        print("parsing output for IO Depth = {0}".format(io_depth))
        fio_run.parse_stdout(fio_run_process.stdout)
        return fio_run
