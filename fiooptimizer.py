import argparse
import subprocess
from typing import Any, Union
from math import floor
from utils.parsers import get_file, parse_fio_config
from utils.models import FioOptimizer
from argparse import ArgumentParser, Namespace
import smtplib as mail
# Read configs in

# function that sends json results and notification emails
def send_results(results: any, emails: list) -> None:
    if not emails or not results:
        return
    # send email
    mail = mail.SMTP('smtp.corp.netapp.com', 25)
    mail.sendmail('fioOptimizer@netapp.com', emails, results)
    mail.quit()
    return


# pick a job count equal to number of CPUs
#   Grab NIC speed as "target" maximum
# decide which test to iterate next
# decide if we can/need to iterate more
# parse results
# display/return results


def arg_parser_seetup() -> Namespace:
    parser = ArgumentParser(description="Optimizer for fio")
    parser.add_argument('-v', '--verbose',
                        action='store_true', help="Displays verbose output")
    parser.add_argument('-bs', '--blocksize', type=int, default="8K", 
                        action='store_const', help='Block Size in Kilobytes')
    parser.add_argument('-min', '--minimum', type=int, default=1, 
                        action='store_const', help='Minimum IO Depth')
    parser.add_argument('-max', '--maximum', type=int, default=65536, 
                        action='store_const', help='Maximum IO Depth')
    parser.add_argument('-h', '--help', type=int,
                        action='store_true', help='Display Usage')
    parser.add_argument('-s', '--silent',
                        action='store_true', help='Suppresses standard output')
    parser.add_argument('-c', '--config', default='fio.ini',
                        action='store', help='path to config file. Defaults to fio.ini')
    parser.add_argument('-e', '--email', nargs='+', 
                        action='store', help='list of emails to send notifications')
    args = parser.parse_args()
    return args

    #python optimizer.py --min=50 --max=900
def main():
    # region parse input and config file
    arg_parser, args = arg_parser_seetup()
    fio_config = parse_fio_config(**args['config'])
    if not fio_config:
        arg_parser.print_help()
        exit(418)
    # endregion
    # region prep fio objects
    fio = FioOptimizer(config=fio_config,
                       min=args['min'],
                       max=args['max'])
    fio.find_optimal_iodepth()
    print(fio.best_run)
    # Save the DataFrame to a csv file
    fio.to_DataFrame().to_csv('fio.csv')
    # Save the DataFrame to a json file 
    fio.to_DataFrame().to_json('fio.json')
    # endregion prep fio objects

    # region send email notifications 
    if args['email']:
        send_results(fio.to_DataFrame().to_json(), args['email'])
    # endregion


if __name__ == '__main__':
    main()
