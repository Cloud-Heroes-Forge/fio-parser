import argparse
from typing import Any, Union

from utils.parsers import get_file, parse_config
from utils.models import FioBase
from argparse import ArgumentParser, Namespace
from utils.fio_runner import FioRunner
# Read configs in


class FioOptimizer:
    """
    //TODO//
    """
    best_run: FioBase
    optimal_queue_depth: int
    config: list
    runs: list

    def __init__(self,
                 runs: list = None,
                 best_run: FioBase = None,
                 optimal_queue_depth: int = 1,
                 config: list = None):

        if runs is None:
            runs = []
        if config is None:
            config = []

    def find_optimal_iodepth(self):
        FioRunner.run_fio(params=self.config)

# Create obj to store results
# run first "wave" of tests
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


def parse_args() -> (object, Namespace):
    parser = ArgumentParser(description="Optimizer for fio")
    parser.add_argument('-v', '--verbose',
                        action='store_true', help="Displays verbose output")
    parser.add_argument('-bs', '--blocksize', type=int,
                        action='store_const', help='Block Size in Kilobytes')
    parser.add_argument('-h', '--help', type=int,
                        action='store_true', help='Display Usage')
    parser.add_argument('-s', '--silent',
                        action='store_true', help='Suppresses standard output')
    parser.add_argument('-c', '--config', default='optimal-io-depth-search.config',
                        action='store', help='path to config file')

    args = parser.parse_args()
    return parser, args


def main():
    # region parse input and config file
    arg_parser, args = parse_args()
    fio_config = parse_config(**args.config)
    if not fio_config:
        arg_parser.print_help()
        exit(418)
    # endregion
    # region prep fio objects
    fio = FioOptimizer(config=fio_config)

    # endregion


if __name__ == '__main__':
    main()
