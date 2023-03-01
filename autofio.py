# autofio.py 
# wrapper around the fio tool to automate the process of finding the optimal IO depth

import os
import sys
import time
import json
import pandas as pd
from utils.models import FioBase, FioOptimizer
from utils.parsers import parse_fio_config
from argparse import ArgumentParser, Namespace
import logging
def arg_parser_setup() -> Namespace:
    logging.info("Parsing Arguments")
    parser = ArgumentParser(description="Optimizer for fio")
    parser.add_argument('-v', '--verbose',
                        action='store_true', help="Displays verbose output")
    # TODO take in a list of tuples (blocksize, randrw/rw)
    parser.add_argument('-bs', '--blocksize', nargs='+', default=["8K"], 
                        help='Block Size to test. Defaults to 8K, for multiple block sizes use -bs 8K 16K 32K')
    parser.add_argument('-min', '--minimum', type=int, default=1, 
                        action='store', help='Minimum Queue Depth to test. Defaults to 1')
    parser.add_argument('-max', '--maximum', type=int, default=256, 
                        action='store', help='Maximum Queue Depth to test. Defaults to 256, max recommended is 65536')
    parser.add_argument('-c', '--config', default='fio.ini',
                        help='path to config file. Defaults to fio.ini')
    parser.add_argument('-e', '--email', nargs='+', 
                        action='append', help='list of emails to send notifications, defaults is none')
    parser.add_argument('-s', '--slices', type=int, default=5,
                        help='Number of slices to divide the IO Depth range into, defaults to 5')
    parser.add_argument('-rw', '--readwrite', nargs='+', default=["50"])
    #parser.add_argument('-mode', '--mode', default="max_io_rate", help="Mode to run fio in. Defaults to rw")
    parser.add_argument('-n', '--name', default="job1", help="Name of the fio job(s). Defaults to job1")
    args = parser.parse_args()
    logging.debug(f"Arguments: {args}")
    return args


def main():
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S', 
                        level=logging.INFO,
                        handlers=[
                            logging.FileHandler("autofio.log", mode='w+'),
                            logging.StreamHandler(sys.stdout)
                        ]
    )
    args = arg_parser_setup()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Verbose Output Enabled")
        logging.debug(f"Arguments: {args}")
    for blocksize in args.blocksize:
        fio_optimizer: FioOptimizer = FioOptimizer()
        # read the config file and parse it into a dictionary with ConfigParser
        parsed_config: dict = parse_fio_config(args.config)
        logging.debug(f"Parsed Config: {parsed_config}")
        fio_optimizer.config = parsed_config
        # set other attributes
        fio_optimizer.config['bs'] = blocksize
        fio_optimizer.min = args.minimum if args.minimum > 0 else 1
        fio_optimizer.max = args.maximum if args.maximum > 0 else 65536
        fio_optimizer.slices = args.slices if args.slices > 0 else 5
        logging.info(f"Starting Fio Optimizer for blocksize: {blocksize}")
        logging.debug(f"Optimizer Config: {fio_optimizer.config}")
        fio_optimizer.find_optimal_iodepth()
        
        # Save the DataFrame to a csv/json file
        fio_optimizer_df = fio_optimizer.to_DataFrame()
        logging.info(f"Saving fio_{blocksize}.csv and fio_{blocksize}.json")
        # TODO: add blocksize, rwmix, and to the pandas object
        fio_optimizer_df.to_csv(f'fio_{blocksize}.csv')
        fio_optimizer_df.to_json(f'fio_{blocksize}.json')
        

if __name__ == '__main__':
    main()

