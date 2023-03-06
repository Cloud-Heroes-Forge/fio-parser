import json
from datetime import datetime
import subprocess
from typing import Any, List
import pandas as pd
import logging
import configparser
from os import path

class FioBase:
    def __init__(self):
        self.read_throughput: float = 0
        self.read_latency: float = 0
        self.read_iops: float = 0
        self.write_throughput: float = 0
        self.write_latency: float = 0
        self.write_iops: float = 0
        self.read_percent: float = 0
        self.total_throughput: float = 0
        # self.total_ops: float = 0
        self.timestamp: datetime = None
        self.duration: float = 0
        self.blocksize: str = None
        self.total_iops: float = 0
        self.io_depth: int = 0
        # self.jobs: int = 0
        # self.ERROR_CODE = None
        self.iops_latency_ratio: float = 0
        self.avg_latency: float = 0
        self.summarize()

    def summarize(self) -> None:
        self.total_iops = self.write_iops + self.read_iops
        self.total_throughput = self.read_throughput + self.write_throughput
        if self.read_latency == 0:
            self.avg_latency = self.write_latency
        elif self.write_latency == 0:
            self.avg_latency = self.read_latency
        else:
            self.avg_latency = ((self.read_latency * self.read_iops) + (self.write_latency * self.write_iops)) / self.total_iops
        # this is the optimizer metric
        self.iops_latency_ratio = self.total_iops / self.avg_latency if self.avg_latency != 0 else 0
        self.read_percent = (self.read_iops / self.total_iops) * 100 if self.total_iops != 0 else 0


    def to_json(self) -> str:
        try: 
            return json.dumps(self.__dict__)
        except json.JSONDecodeError as e:
            raise Exception(f"Error converting fio output to JSON: {e.msg}")

    def __str__(self) -> str:
        return self.to_json()

    @staticmethod
    def prepare_args(params: dict) -> list:
        param_list: list = [f"{k}={v}" if v else f"{k}" for k, v in params.items()]
        return param_list

    @staticmethod
    def run_fio(params: list) -> object:
        fio_process = subprocess.run(['fio'] + params, capture_output=True)
        logging.INFO(f"Fio Return code: {fio_process}")
        return fio_process


    def parse_stdout(self, raw_stdout: str) -> None:
        try: 
            json_result = json.loads(raw_stdout)
            self.read_iops = json_result['jobs'][0]['read']['iops']
            self.read_throughput = json_result['jobs'][0]['read']['bw']
            self.read_latency = json_result['jobs'][0]['read']['lat']['mean']
            self.write_iops = json_result['jobs'][0]['write']['iops']
            self.write_throughput = json_result['jobs'][0]['write']['bw']
            self.write_latency = json_result['jobs'][0]['write']['lat']['mean']
            self.duration = json_result['jobs'][0]['elapsed']
            self.timestamp = json_result['time']
            self.summarize()
            return
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
                 config: dict = None, 
                 min: int = 1,
                 max: int = 256, 
                 slices: int = 3):

        self.runs: dict = runs if runs else {}
        self.config: dict = config if config else {}
        self.best_run: FioBase = None
        self.optimal_queue_depth: int = None
        self.min: int = min
        self.max: int = max
        self.slices: int = slices
        self.tested_iodepths: list[int] = []
        self.runs_raw: dict = {}

        # store state file (csv maybe), read that state file in on load and just return data 

    def find_optimal_iodepth(self) -> FioBase:
        is_optimial: bool = False
        
        while not is_optimial: 
            # Test minimum io_depth and maximum io_depth
            logging.info(f"min: {self.min}\t max: {self.max}")
            self.prepare_and_run_fio(io_depths=[self.min, self.max])    
            # Check if min and max are 1 away from each other, 
            # if so determine which of the two are better and that is the optimal io depth        
            if (self.max - self.min) <= 1:
                self.best_run = self.runs[self.max] if self.runs[self.max] > self.runs[self.min]else self.runs[self.min] 
                is_optimial: bool = True
                logging.info(f"Optimal IO Depth: {self.best_run.io_depth}")
                logging.info(f"IOPS            : {self.best_run.total_iops}")
                logging.info(f"Latency         : {self.best_run.avg_latency} µs")
                logging.info(f"Throughput      : {self.best_run.total_throughput} KiBps")
            else:
                # take a range of values spaced equally between minimum and maximum and test each one
                next_iodepths = range(self.min, self.max, max(abs((self.max - self.min)//self.slices),1))
                self.prepare_and_run_fio(io_depths=next_iodepths)
                sorted_runs = sorted(self.runs.items(), key=lambda item: item[1], reverse=True)
                self.max = self.max if sorted_runs[0][0] == self.max else self.max - max(1, ((self.max + (sorted_runs[0][0])) // self.slices))
                self.min = self.min if sorted_runs[0][0] == self.min else self.min + max(1, ((self.min + (sorted_runs[0][0])) // self.slices))
        return self.best_run

    def prepare_and_run_fio(self, io_depths: List) -> None:
        # TODO Add config to INI if passing at cmdline doesn't work for multi host 2/21/2023  
        logging.debug(f"Preparing to run FIO with IO Depths: {io_depths}")
        for io_depth in io_depths:
            if io_depth in self.tested_iodepths or io_depth <= 0:
                logging.debug(f"Skipping IO Depth = {io_depth}")
                continue
            logging.info(f"Running Test with IO Depth = {io_depth}")
            self.config['iodepth'] = io_depth
            self.config['output-format'] = 'json'
            
            param_list = [f"--{k}={v}" if v else f"--{k}" for k, v in self.config.items()]

            fio_run_process = subprocess.run(['fio'] + param_list, capture_output=True)
            if fio_run_process.returncode != 0:
                logging.error(f"Error code: {fio_run_process.returncode}\nError Message: {fio_run_process.stderr}")
                raise RuntimeError(f"Error code: {fio_run_process.returncode}\nError Message: {fio_run_process.stderr}")
            
            fio_run: FioBase = FioBase()
            fio_run.parse_stdout(fio_run_process.stdout)
            fio_run.io_depth = io_depth
            fio_run.blocksize = self.config['bs']
            # store current iteration in the "runs" dictionary
            self.runs[io_depth] = fio_run
            self.tested_iodepths.append(io_depth)
            self.runs_raw[io_depth] = fio_run_process.stdout
            logging.debug(f"Test with IO Depth = {io_depth} completed")

    def to_DataFrame(self) -> pd.DataFrame:
        df = pd.DataFrame([x.__dict__ for x in self.runs.values()])
        df.set_index('io_depth', inplace=True)
        return df
    

    def to_csv(self) -> str:
        # call the to_DataFrame function and return a csv
        return self.to_DataFrame().to_csv()

    def to_json(self) -> str:
        # call the to_DataFrame function and return a json
        return self.to_DataFrame().to_json()
    

# create a class that access a configuration ini file and returns a dictionary of the configuration
def parse_fio_config(config_file: str) -> dict:
    if not path.isfile(config_file):
        logging.error(f"File {config_file} not found")
        raise FileNotFoundError(f"File {config_file} not found")
    config_parser = configparser.ConfigParser(allow_no_value=True)
    config_parser.read(config_file)
    if not config_parser.has_section('global'):
        logging.error("Config file does not have a [global] section")
        raise ValueError("Config file does not have a [global] section")
    return config_parser.items('global')


