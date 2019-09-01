'''
Created On: Aug 29, 2019
Author: Deepti

Build an SQL parser through CLI


'''

import re
import sys
import csv


# First things first : read metadata from metadata.txt
META = './file/metadata.txt'
tables_list = {}            # Since multiple tables

def read_metadata(file):
    '''
        Get structure of multiple tables
    '''
    begin = 0
    table_name = ""
    try:
        struct_file = open(file, 'r')
    except IOError:
        sys.stderr.write("Error: No meta-structure found.\n")
        quit(-1)
    
    for line in struct_file:
        line = line.strip()     # Remove white spaces
        if line == '<begin_table>':
            begin = 1
            continue
        if begin == 1:
            table_name = line
            tables_list[table_name] = []
            begin = 0
        elif line != "<end_table>":
            tables_list[table_name].append(line)
    
    begin = 0
    return tables_list

def get_datacolumns(table_name):
    '''
        Read data in table ka file
    '''
    file = table_name + '.csv'
    data = []
    try:
        src = open(file, 'rb')
    except IOError:
        sys.stderr.write("Error: No corresponding .csv file found.\n")
        quit(-1)
    
    info = csv.reader(src)
    for row in info:
        data.append(row)
    src.close()
    return data

# To implement: Select all from table

def parse_query(line):
    '''
        Parse query
    '''
    words = (re.sub(' +', ' ', line)).strip()
   
    # Check for table name:
    if 'from' not in words.split():
        sys.stderr.write("Error: No table name mentioned in query.\n")
        quit(-1)
    words = words.split('from')
    stripped = [(re.sub(' +', ' ', word)).strip() for word in words]
    clauses = stripped[1].split("where")

    tables_inquery = [(re.sub(' +', ' ', word)).strip() for word in clauses[0].split(",")]
    for table in tables_inquery:
        if table not in 

query = "Select * from table_name         , abc where hallelujah"
parse_query(query)
    