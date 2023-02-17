# autofio.py 
# wrapper around the fio tool to automate the process of finding the optimal IO depth

import os
import sys
import time
import json
import pandas as pd
from utils.models import FioBase, FioOptimizer
from argparse import ArgumentParser, Namespace

def arg_parser_seetup() -> Namespace:
    parser = ArgumentParser(description="Optimizer for fio")
    parser.add_argument('-v', '--verbose',
                        action='store_true', help="Displays verbose output")
    parser.add_argument('-bs', '--blocksize', nargs='+', default=["8K"], 
                        action='store_const', help='Block Size in Kilobytes')
    parser.add_argument('-min', '--minimum', type=int, default=1, 
                        action='store_const', help='Minimum IO Depth')
    parser.add_argument('-max', '--maximum', type=int, default=65536, 
                        action='store_const', help='Maximum IO Depth')
    parser.add_argument('-h', '--help', type=int,
                        action='store_true', help='Display Usage')
    parser.add_argument('-c', '--config', default='fio.ini',
                        action='store', help='path to config file. Defaults to fio.ini')
    parser.add_argument('-e', '--email', nargs='+', 
                        action='store', help='list of emails to send notifications')
    parser.add_argument('-s', '--slices', type=int, default=3,
                        action='store_const', help='Number of slices to divide the IO Depth range into')
    args = parser.parse_args()
    return args

def main():
    # do stuff
    args = arg_parser_seetup()
    for blocksize in args.blocksize:
        fio_optimizer = FioOptimizer()
        fio_optimizer.config = args.config if args.config else 'fio.ini'
        fio_optimizer.blocksize = blocksize
        fio_optimizer.minimum = args.minimum if args.minimum > 0 else 1
        fio_optimizer.maximum = args.maximum if args.maximum > 0 else 65536
        fio_optimizer.slices = args.slices if args.slices > 0 else 3
        fio_optimizer.find_optimal_iodepth()
        print(fio_optimizer.best_run)
        # Save the DataFrame to a csv file
        fio_optimizer.to_DataFrame().to_csv(f'fio_{blocksize}.csv')
        # Save the DataFrame to a json file 
        fio_optimizer.to_DataFrame().to_json(f'fio_{blocksize}.json')
        

if __name__ == '__main__':
    main()

