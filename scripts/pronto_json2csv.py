#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. module:: pronto_json2csv.py
    :platform: Unix
    :synopsis: Convert Pronto Station Feed to CSV (Tab - Delimited) Data File.
    
.. moduleauthor:: MaptimeSEA

"""

import csv
import os
import simplejson
from simplejson import JSONDecodeError

def subset_filelist_by_time_interval(file_list, time_interval=None):
    """
    Given a list of files, return only those who match a given time interval.
    Currently stubbed, Just reflects input data.

    :param time_interval: Valid values currently are 5, 10, 15, 30, 60 minutes.
    :return: A list of file names matching the request time interval.
    """

    # Didn't get a time_interval, return all the filez.
    if not time_interval:
        return file_list

def flatten_json_data(in_data):
    """
    Given a JSON response formatted under the pronto schema,
    transform the data to return a flattened output prepped for
    writing to CSV.

    :return:
    """
    station_data = in_data['stations']
    for station in station_data:
        station['queried_timestamp'] = in_data['timestamp']
    return station_data

def write_to_csv(in_data, out_data_path):
    """
    Given a list of records, write out to a CSV File.
    :param in_data:
    :return:
    """
    pass

if __name__ == '__main__':

    # Setup
    data_dir = os.path.join(os.path.pardir, 'data_minutely')
    files_of_interest = subset_filelist_by_time_interval(os.listdir(data_dir))
    output_path = os.path.join(os.getcwd(), 'pronto_data_processed.csv')

    files_failed_to_process = []
    finished_rows = []

    for data_file in files_of_interest:
        data_file_path = os.path.join(data_dir, data_file)

        # Create a file handle for each file.
        with open(data_file_path) as data_file_handle:
            # Read JSON File
            try:
                json_data = simplejson.load(data_file_handle)
                # Process
                flattened_json_data = flatten_json_data(json_data)
                # Add to queue for output to CSV
                finished_rows += flattened_json_data
            except JSONDecodeError as e:
                files_failed_to_process.append(data_file)

    # Write to CSV
    write_to_csv(finished_rows, output_path)

    print "Finished. %s files failed to process: %s" % (len(files_failed_to_process), files_failed_to_process)
