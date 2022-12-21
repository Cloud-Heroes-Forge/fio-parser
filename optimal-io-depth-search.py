import argparse
from typing import Any, Union
from math import floor
from utils.parsers import get_file, parse_config
from utils.models import FioBase
from utils.converters import average
from argparse import ArgumentParser, Namespace
# Read configs in


class FioOptimizer:
    """
    //TODO//
    """
    best_run: FioBase
    optimal_queue_depth: int
    config: dict
    runs: dict

    def __init__(self,
                 runs: dict = None,
                 best_run: FioBase = None,
                 optimal_queue_depth: int = 0,
                 config: dict = None, 
                 min: int = 1,
                 max: int = 65536):

        if runs is None:
            runs = {}
        if config is None:
            config = {}
        self.min: int = min
        self.max: int = max

    def find_optimal_iodepth(self) -> None:
        is_optimial: bool = False
        starting_io_depths: list = [self.min, self.max]    # gotta start some where

        for io_depth in starting_io_depths:
            self.config['io_depth'] = io_depth
            fio_run = self.prepare_and_run_fio(io_depth=io_depth)
            self.runs[io_depth] = fio_run

        while not is_optimial: 
            if max.io_depth - min.io_depth <= 1:
                self.best_run = max if max.total_iops > min.total_iops else min 
                is_optimial: bool = True
            else:
                next_iodepth: int = floor(average(max.io_depth, min.io_depth))
                self.config['io_depth'] = next_iodepth
                fio_run = self.prepare_and_run_fio(io_depth=next_iodepth)
                self.runs[next_iodepth] = fio_run
                if average(self.min.total_iops, fio_run.total_iops) > average(fio_run.total_iops, self.max.total_iops):
                    self.max = fio_run.io_depth
                else: 
                    self.min = fio_run.io_depth

        
        # test limits (min, max)
        # test midpoint between limits
        # average results of min+midpoint and max+midpoint
        # 
        # divide total from min and max into n buckets, then go test all of them,
        # sort them 
        # range(min, max, (abs(max-min))/5)
        # slices = 5 
        # 10, 20, 2 # ((20-10)/slices = 2)
        # k = [10, 12, 14, 16, 18]
        # when I do the loop, test if k[n] is already tested
        # find the best results from min, max, k[]
        

        # while not is_optimial:
        #     sorted_runs_by_iops: list[FioBase] = sorted(self.runs, key=self.runs.get('total_iops'), reverse=True)
        #     # are we going up or down?
        #     if sorted_runs_by_iops[0].io_depth - sorted_runs_by_iops[1].io_depth <= 1:
        #         is_optimial = True
        #         self.optimal_queue_depth = sorted_runs_by_iops[0]
        #         return 
        #     else: 
        #         next_io_depth = ((sorted_runs_by_iops[0].io_depth + sorted_runs_by_iops[1].io_depth) // 2)
        #         if next_io_depth in self.runs:
        #             next_io_depth = 
        #         else:
        #             self.prepare_and_run_fio(io_depth=next_io_depth)

    def prepare_and_run_fio(self, io_depth: int) -> FioBase:
        print("Running Test with IO Depth = {0}".format(io_depth))
        fio_args: list = FioBase.prepare_args(self.config)
        fio_run_process: object = FioBase.run_fio(params=fio_args)
        fio_run: FioBase = FioBase()
        fio_run.parse_stdout(fio_run_process.stdout)

# pick a job count equal to number of CPUs
#   Grab NIC speed as "target" maximum
# decide which test to iterate next

    def compare_run_throughput(self, new_run: FioBase) -> bool:
        if new_run.total_bandwidth > self.best_run.total_bandwidth:
            self.best_run = new_run
            return True
        return False


# decide if we can/need to iterate more

# parse results
# display/return results


def parse_args() -> Namespace:
    parser = ArgumentParser(description="Optimizer for fio")
    parser.add_argument('-v', '--verbose',
                        action='store_true', help="Displays verbose output")
    parser.add_argument('-bs', '--blocksize', type=int,
                        action='store_const', help='Block Size in Kilobytes')
    parser.add_argument('-min', '--minimum', type=int,
                        action='store_const', help='Minimum IO Depth')
    parser.add_argument('-max', '--maximum', type=int,
                        action='store_const', help='Maximum IO Depth')
    parser.add_argument('-h', '--help', type=int,
                        action='store_true', help='Display Usage')
    parser.add_argument('-s', '--silent',
                        action='store_true', help='Suppresses standard output')
    parser.add_argument('-c', '--config', default='optimal-io-depth-search.config',
                        action='store', help='path to config file')

    args = parser.parse_args()
    return args


def main():
    # region parse input and config file
    arg_parser, args = parse_args()
    fio_config = parse_config(**args.config)
    if not fio_config:
        arg_parser.print_help()
        exit(418)
    # endregion
    # region prep fio objects
    fio = FioOptimizer(config=fio_config,
                       min=args['min'],
                       max=args['max'])
    fio.find_optimal_iodepth()
    # do something with the output

    # endregion


if __name__ == '__main__':
    main()
