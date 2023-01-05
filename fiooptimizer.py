import argparse
import subprocess
from typing import Any, Union
from math import floor
from utils.parsers import get_file, parse_config
from utils.models import FioOptimizer
from argparse import ArgumentParser, Namespace
# Read configs in



# pick a job count equal to number of CPUs
#   Grab NIC speed as "target" maximum
# decide which test to iterate next
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

    #python optimizer.py --min=50 --max=900
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
