import json
from datetime import datetime
import subprocess
from utils.converters import average
from typing import Any, List
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
        self.iops_latency_ratio: float = 0
        self.avg_latency: float = 0
        self.summarize()

    def summarize(self) -> None:
        self.total_iops = self.write_iops + self.read_iops
        self.total_bandwidth = self.read_bandwidth + self.write_bandwidth
        self.avg_latency = (self.write_latency + self.read_latency) / 2
        self.iops_latency_ratio = self.total_iops / self.avg_latency if self.avg_latency != 0 else 0

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
        return (self.iops_latency_ratio < other.iops_latency_ratio)
    
    def __le__(self, other):
        return (self.iops_latency_ratio <= other.iops_latency_ratio)
    
    def __eq__(self, other):
        return (self.iops_latency_ratio == other.iops_latency_ratio)
    
    def __ne__(self, other):
        return (self.iops_latency_ratio != other.iops_latency_ratio)
    
    def __gt__(self, other):
        return (self.iops_latency_ratio > other.iops_latency_ratio)
    
    def __ge__(self, other):
        return (self.iops_latency_ratio >= other.iops_latency_ratio)
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

        self.runs: dict = {} if runs is None else runs
        self.config: dict = {} if config is None else config
        self.best_run: FioBase = best_run
        self.optimal_queue_depth: int = optimal_queue_depth
        self.min: int = min
        self.max: int = max
        self.slices: int = slices
        self.tested_iodepths: list[int] = []

    def find_optimal_iodepth(self) -> None:
        is_optimial: bool = False
        
        while not is_optimial: 
            # Test minimum io_depth and maximum io_depth
            self.prepare_and_run_fio(io_depths=[self.min, self.max])    
            # Check if min and max are 1 away from each other, if so determine which of the two are better and that is the optimal io depth        
            if (self.max - self.min) <= 1:
                self.best_run = self.runs[self.max] if self.runs[self.max].iops_latency_ratio > self.runs[self.min].iops_latency_ratio else self.runs[self.min] 
                is_optimial: bool = True
                print(f"\nOptimal IO Depth: {self.best_run.io_depth}" + \
                      f"IOPS              : {self.best_run.total_iops}" + \
                      f"Latency           : {self.best_run.avg_latency} µs" + \
                      f"Throughput        : {self.best_run.total_bandwidth}  KiBps")
            else:
                # take a range of values spaced equally between minimum and maximum and test each one
                next_iodepths = range(self.min, self.max, max(abs((self.max - self.min)//self.slices),1))
                self.prepare_and_run_fio(next_iodepths)

            # if average(self.runs[self.max].iops_latency_ratio, fio_run.iops_latency_ratio) > average(fio_run.iops_latency_ratio, self.runs[self.min].iops_latency_ratio):
            #     self.max = (fio_run.io_depth + self.runs[self.max].io_depth) // 2
            # else: 
            #     self.min = (fio_run.io_depth + self.runs[self.min].io_depth) // 2

    def prepare_and_run_fio(self, io_depths: List[int]) -> None:
        for io_depth in io_depths:
            if io_depth in self.tested_iodepths or io_depth <= 0:
                continue
            print("Running Test with IO Depth = {0}".format(io_depth))
            self.config['--iodepth'] = io_depth
 
 
            param_list = [f"{k}={v}" if v else f"{k}" for k, v in self.config.items()]

            fio_run_process = subprocess.run(['fio'] + param_list, capture_output=True)
            fio_run: FioBase = FioBase()
            fio_run.io_depth = io_depth
            print("parsing output for IO Depth = {0}".format(io_depth))

            fio_run.parse_stdout(fio_run_process.stdout)

            self.runs[io_depth] = fio_run
            self.tested_iodepths.append(io_depth)

            if io_depth > (self.max // 2):
                if (fio_run > self.runs[self.max]):
                    self.max = (fio_run.io_depth + self.runs[self.max].io_depth) // 2
                    print(f"Setting Max as: {self.max}")
            else:
                if fio_run > self.runs[self.min]: 
                    self.min = (fio_run.io_depth + self.runs[self.min].io_depth) // 2
                    print(f"Setting Min as: {self.min}")
