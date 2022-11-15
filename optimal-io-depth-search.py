from utils.parsers import get_file
from argparse import ArgumentParser
# Read configs in
# Create obj to store results
# run first "wave" of tests
# decide which test to iterate next
# decide if we can/need to iterate more
# parse results
# display/return results


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-v', '--verbose',
                        action='store_true', help="Displays verbose output")
    parser.add_argument('-bs', '--blocksize', type=int,
                        action='store_const', help='Block Size in Kilobytes')
    parser.add_argument('-h', '--help', type=int,
                        action='store_true', help='Display Usage')
    parser.add_argument('-s', '--silent',
                        action='store_true', help='Suppresses standard output')

    args = parser.parse_args()
    return args


def main():
    args = parse_args()


if __name__ == '__main__':
    main()
