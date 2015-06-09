#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. module:: pronto_json2csv.py
    :platform: Unix
    :synopsis: Convert Pronto Station Feed to CSV Data File.
    
.. moduleauthor:: MaptimeSEA

"""

import csv
import os
from datetime import datetime
try:
    import simplejson as json
except:
    import json

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

    filters = {
        5: ('5.json','0.json'),
        10: ('0.json'),
        15: ('15.json','30.json','45.json','00.json'),
        30: ('00.json','30.json'),
        60: ('00.json')
    }
    try:
        return [file for file in file_list if file.endswith(filters[time_interval])]
    except KeyError:
        print "Unsupported Time Interval Given. Values are: 5, 10, 15, 30, 60."

def flatten_json_data(in_data):
    """
    Given a JSON response formatted under the pronto schema,
    transform the data to return a flattened output prepped for
    writing to CSV.

    :return: a list of individual records represented as dictionaries.
    """
    station_data = in_data['stations']
    for station in station_data:
        station['queried_timestamp'] = in_data['timestamp']
    return station_data

def to_readable_keys(in_data):
    """
    Mapping the Field Names to more human readable
    `Name` values from the field dictionary found here:
    https://www.prontocycleshare.com/assets/pdf/JSON.pdf

    :return:
    :param in_data: A list containing a file of data as dictionaries.
    :return: Updated with human readable keys.
    """
    readable_field_names = []

    for row in in_data:
        readable_field_names.append(
            {
                'queried_timestamp': row['queried_timestamp'],
                'blocked': row['b'],
                'available_bikes': row['ba'],
                'bike_key_dispenser': row['bk'],
                'bike_keys_available': row['bl'],
                'unavailable_bikes': row['bx'],
                'available_bike_docks': row['da'],
                'unavailable_bike_docks': row['dx'],
                'bike_station_id': row['id'],
                'latitude': row['la'],
                'lc': row['lc'],
                'longitude': row['lo'],
                'latest_update_time': row['lu'],
                'exposed_as_out_of_service': row['m'],
                'terminal_name': row['n'],
                'public_bike_station_name': row['s'],
                'status': row['st'],
                'suspended': row['su']
            }
        )

    return readable_field_names

def to_readable_timestamp(in_data, in_fields):
    """
    Convert UNIX Epoch Time to Human Readable Timestamp

    :param in_data: List of data as dictionaries
    :param in_fields: Field Names Containing Timestamps
    :return:
    """
    out_data = []

    for row in in_data:
        for field in in_fields:
            time_as_float = row[field] / 1000.0
            converted_time = datetime.fromtimestamp(time_as_float).strftime('%Y-%m-%d %H:%M:%S.%f')
            row[field] = converted_time
        out_data.append(row)

    return out_data

def write_to_csv(in_data, in_fields, out_data_path):
    """
    Given a list of records, write out to a CSV File.

    :param in_data: a list of records as dictionaries.
    :param in_fields: a list of field names.
    :param out_data_path: pathway for output file.
    :return: True if sucessfully executed.
    """
    with open(out_data_path, 'w') as out_file_handle:
        writer = csv.DictWriter(out_file_handle, fieldnames=in_fields)

        writer.writeheader()
        for row in in_data:
            writer.writerow(row)

    return True

if __name__ == '__main__':

    # Setup
    time_interval = 15
    data_dir = os.path.join(os.path.pardir, 'data_minutely')
    files_of_interest = subset_filelist_by_time_interval(os.listdir(data_dir), time_interval)
    output_path = os.path.join(os.getcwd(), 'pronto_data_processed.csv')

    files_failed_to_process = []
    finished_rows = []

    # Begin iteration through files.
    for data_file in files_of_interest:
        data_file_path = os.path.join(data_dir, data_file)

        # Create a file handle for each file.
        with open(data_file_path, 'rb') as data_file_handle:
            # Read JSON File
            try:
                json_data = json.load(data_file_handle)
                # Process
                flattened_json_data = flatten_json_data(json_data)
                flattened_json_data = to_readable_keys(flattened_json_data)
                flattened_json_data = to_readable_timestamp(flattened_json_data, ['queried_timestamp', 'latest_update_time'])
                # Add to queue for output to CSV
                finished_rows += flattened_json_data
            except Exception as e:
                files_failed_to_process.append(data_file)

    # Write to CSV

    fields = [
        'queried_timestamp',
        'blocked',
        'available_bikes',
        'bike_key_dispenser',
        'bike_keys_available',
        'unavailable_bikes',
        'available_bike_docks',
        'unavailable_bike_docks',
        'bike_station_id',
        'latitude',
        'lc',
        'longitude',
        'latest_update_time',
        'exposed_as_out_of_service',
        'terminal_name',
        'public_bike_station_name',
        'status',
        'suspended'
    ]
    write_to_csv(finished_rows, fields, output_path)

    print "Finished. %s files failed to process: %s" % (len(files_failed_to_process), files_failed_to_process)
