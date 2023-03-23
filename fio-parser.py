#!/usr/bin/python
import argparse
import os
from utils.converters import *
from os import listdir
from os.path import isfile, join


def command_line():
    parser = argparse.ArgumentParser(prog='fio-parser.py', description='%(prog)s is used to parse fio output files')
    parser.add_argument('--directory', '-d', type=str,
                        help='Specify the directory with fio output files to parse.  If none if provided, ./ is used')
    # parser.add_argument('--output','-rod',type=str,help='Specify the output file to dump results.  If none, output is to screen')
    arg = vars(parser.parse_args())

    if not arg['directory']:
        directory = os.getcwd()
    else:
        directory = arg['directory']
    '''
    if arg['output']:
       if '/' in arg['output']:
           dir = '/'.join(arg['output'].split('/')[:-1])
           output_file = arg['output'].split('/')[-1]
           if ! output_file:
              print('--output %s missing file name, exiting' % (arg['output'])
              exit()
       else:
           dir='./'
           output_file = arg['output']

       if isdir(dir):
    '''

    file_list = get_file_list(directory)
    return file_list


def get_file_list(directory):
    """
    extract the contents of the current file if present to only overwrite the section in play
    """
    if os.path.exists(directory):
        file_list = []
        onlyfiles = [files for files in listdir(directory) if isfile(join(directory, files))]
        # Check for non-binary files and make a list of them
        for files in onlyfiles:
            abs_path = ('%s/%s' % (directory, files))
            # with open( abs_path,'rb') as fh:
            with open(abs_path, 'r') as fh:
                if 'fio-parser.py' not in files:
                    if 'IO depths' in fh.read():
                        file_list.append('%s/%s' % (directory, files))
        if (len(file_list)) > 0:
            file_list
            return file_list
        else:
            print('No text files found')
            exit()
    else:
        print('Directory %s invalid' % directory)
        exit()


def parse_files(file_list):
    total_output_list = ['fio_file,reads,read_bw(MiB/s),read_lat(ms),writes,write_bw(MIB/s),write_lat(ms)']
    for working_file in file_list:
        with open(working_file, 'r') as fh:
            file_content = fh.readlines()
        # extract_content(file_content, working_file, total_output_list)
        parsed_content = extract_content(file_content)
        total_output(parsed_content, total_output_list, working_file)
    return total_output_list


# look for 'All clients in file', if found, this file contains
# The contents ofd a multi host job
def single_or_multi_job(content):
    for line in content:
        if 'All clients:' in line:
            return True
    return False


def search(parsed_content, line):
    if 'read: IOP' in line:
        parsed_content['read_iop'] = io_conversion(line)
        parsed_content['read_bw'] = bandwidth_conversion(line)
    if 'read_iop' in parsed_content.keys():
        if line[0:3] == 'lat' and 'read_lat' not in parsed_content.keys():
            parsed_content['read_lat'] = lat_conversion(line)

    if 'write: IOP' in line:
        parsed_content['write_iop'] = io_conversion(line)
        parsed_content['write_bw'] = bandwidth_conversion(line)
    if 'write_iop' in parsed_content.keys():
        if line[0:3] == 'lat' and 'write_lat' not in parsed_content.keys():
            parsed_content['write_lat'] = lat_conversion(line)


def extract_content(content):
    multihost = single_or_multi_job(content)
    parsed_content = {}
    if multihost:
        begin_search = False
    else:
        begin_search = True

    for line in content:
        line = line.strip()
        if multihost == True:
            if 'All clients:' in line:
                begin_search = True
        if begin_search:
            search(parsed_content, line)
            # Exit search here
        if begin_search and 'IO depths' in line:
            return parsed_content


def total_output(parsed_content, total_output_list, working_file):
    output = working_file.split('/')[-1]
    if 'read_iop' in parsed_content.keys():
        output += ((',%s,%s') % (parsed_content['read_iop'], parsed_content['read_bw']))
    if 'read_lat' in parsed_content.keys():
        output += ((',%s') % (parsed_content['read_lat']))
    if 'write_iop' in parsed_content.keys() and 'read_iop' not in parsed_content.keys():
        output += ((',,,,%s,%s') % (parsed_content['write_iop'], parsed_content['write_bw']))
    elif 'write_iop' in parsed_content.keys():
        output += ((',%s,%s') % (parsed_content['write_iop'], parsed_content['write_bw']))
    if 'write_lat' in parsed_content.keys():
        output += ((',%s') % (parsed_content['write_lat']))
    total_output_list.append(output)


file_list = command_line()
output = parse_files(file_list)
for each_line in output:
    print(each_line)
