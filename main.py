'''
Created On: Aug 29, 2019
Author: Deepti

Build an SQL parser through CLI


'''


# TO DO : Check errors
# done: 3, 1, 4 + single tables, join without condition
# NEED TO DO : aggregrate function + distinct with where  + error conditions + join condition
# https://github.com/harry-7/minisqlengine

import re
import sys
import csv


# First things first : read metadata from metadata.txt
META = './files/metadata.txt'
tables_list = {}            # Since multiple tables
tables_needed = {}

AGG = ['distinct', 'max', 'sum', 'avg', 'min']

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

def get_datacolumns(table_name):
    '''
        Read data in table ka file
    '''
    file = './files/' + table_name + '.csv'
    data = []
    try:
        src = open(file, "rt")
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
    columns = []
    functions = []
    distincts = []
    dist_pair = []

    tables_inquery = []

    words = (re.sub(' +', ' ', line)).strip()

    # Check for table name:
    if 'from' not in words.split():
        sys.stderr.write("Error: No table name mentioned in query.\n")
        quit(-1)
    words = words.split('from')
    stripped = [(re.sub(' +', ' ', word)).strip() for word in words]
    clauses = [(re.sub(' +', ' ', word)).strip() for word in stripped[1].split("where")]
    tables_inquery = [(re.sub(' +', ' ', word)).strip() for word in clauses[0].split(",")]
    for table in tables_inquery:
        if table not in tables_list:
            sys.stderr.write("Error: Table name is not valid.\n")
            quit(-1)
        tables_needed[table] = get_datacolumns(table)

    needed = [(re.sub(' +', ' ', word)).strip() for word in stripped[0][7:].split(',')]

    for condition in needed:
        if "distinct" in condition.lower():
            dist_pair.append(['distinct', condition.split('distinct')[1].strip(), \
                needed[(needed.index(condition))+1]])
            continue
        if "distinct" in needed[(needed.index(condition))-1].lower():
            continue

        part_of_agg = False
        for func in AGG:
            if func + '(' in condition.lower():
                if ')' not in condition:
                    sys.stderr.write("Error: Closing Bracket not found in query.\n")
                    quit(-1)
                splitter = func + '('
                part_of_agg = True
                if func == 'distinct':
                    distincts.append(condition.strip(')').split(splitter)[1])
                else:
                    functions.append([func, condition.strip(')').split(splitter)[1]])
                break
        if not part_of_agg:
            if condition != '':
                columns.append(condition.strip('()'))

    print(columns, functions, distincts, dist_pair)
    # Now that we got all that we needed
    if len(clauses) > 1:
        execute(columns, functions, distincts, dist_pair, tables_inquery, clauses[1])
    else:
        execute(columns, functions, distincts, dist_pair, tables_inquery)

def execute(columns, functions, distincts, dist_pair, tables_inquery, clauses=[]):
    '''
        Execute depending on parameters passed into this function
    '''
    if len(dist_pair) != 0:
        distinct_pair_process(dist_pair, tables_inquery)
    elif len(tables_inquery) == 1:
        normal_where(clauses, columns, tables_inquery[0])
    elif len(tables_inquery) > 1 and len(clauses) == 0:
        join(columns, tables_inquery)
    elif len(tables_inquery) > 1 and len(clauses) > 0:
        join_where(clauses, columns, tables_inquery)

def distinct_pair_process(dist_pair, tables):
    columns_in_table = {}
    tables_found = []

    for column in [dist_pair[0][1], dist_pair[0][2]]:
        table, column = search_column(column, tables)
        if table not in columns_in_table.keys():
            columns_in_table[table] = []
            tables_found.append(table)
        columns_in_table[table].append(column)
    print(tables_found, columns_in_table)

    if len(tables_found) > 1:
        data_injoin = []
        for item1 in tables_needed[tables_found[0]]:
            for item2 in tables_needed[tables_found[1]]:
                data_injoin.append(item1 + item2)
        display_output(tables_found, columns_in_table, data_injoin, join=True, distinct=True)
    else:
        table = tables_found[0]
        columns = columns_in_table[table]
        print(get_headers(table, columns))
        print("-"*len(get_headers(table, columns)))
        result = []
        for row in tables_needed[table]:
            ans = ''
            for column in columns:
                ans += row[tables_list[table].index(column)] + '\t|'
            if ans.strip('\t|') not in result:
                result.append(ans.strip('\t|'))
        for row in result:
            print(row)


def normal_where(clauses, columns, table):
    if len(columns) == 1 and columns[0] == '*' and len(clauses) == 0:
        columns = tables_list[table]
        print(get_headers(table, columns))
        print("-"*len(get_headers(table, columns)))
        for data in tables_needed[table]:
            ans = ''
            for column in columns:
                ans += data[tables_list[table].index(column)] + '\t|'
            print(ans.strip('\t|'))
    
    elif len(clauses) == 0:
        print(get_headers(table, columns))
        print("-"*len(get_headers(table, columns)))
        for row in tables_needed[table]:
            ans = ''
            for column in columns:
                ans += row[tables_list[table].index(column)] + '\t|'
            print(ans.strip('\t|'))
    
    elif len(clauses) >= 1:
        if len(columns) == 1 and columns[0] == '*':
            columns = tables_list[table]
        print(get_headers(table, columns))
        print("-"*len(get_headers(table, columns)))
        for row in tables_needed[table]:
            evaluator = generate_evals(row, table, clauses)
            ans = ''
            if eval(evaluator):
                for column in columns:
                    ans += row[tables_list[table].index(column)] + '\t|'
                print(ans.strip('\t|'))

def join_where(clauses, columns, tables):
    operators = ['>=', '<=', '>', '<', '=']
    now = ''
    og = clauses
    if 'and' in clauses:
        clauses = clauses.split('and')
        now = 'and'
    elif 'or' in clauses:
        clauses = clauses.split('or')
        now = 'or'
    else:
        clauses = [clauses]
    if len(clauses) > 2:
        sys.stderr.write("Error: Only two clauses joined by ONE or/and is viable.\n")
        quit(-1)
    print(clauses)
    
    condition1 = clauses[0]
    for operator in operators:
        if operator in condition1:
            condition1 = condition1.split(operator)
    # # SAY WHAT
    # if len(condition1) == 2 and '.' in condition1[1]:
    #     normal_join_where([condition, now], columns, tables, tables_data)
    #     return
    join_conditionally(now, clauses, columns, tables)


def join_conditionally(now, clauses, columns, tables):
    data = join_data(clauses, columns, tables)

    columns_in_table = {}
    tables_found = []
    if len(columns) == 1 and columns[0] == '*':
        for table in tables:
            columns_in_table[table] = []
            for column in tables_list[table]:
                columns_in_table[table].append(column)
        tables_found = tables
    else:
        for column in columns:
            table, column = search_column(column, tables)
            if table not in columns_in_table.keys():
                columns_in_table[table] = []
                tables_found.append(table)
            columns_in_table[table].append(column)
    print(columns_in_table, tables_found)

    final_data = []
    if now == 'and':
        for item1 in data[tables[0]]:
            for item2 in data[tables[1]]:
                final_data.append(item1 + item2)
    elif now == 'or':
        for item1 in data[tables[0]]:
            for item2 in tables_needed[tables[1]]:
                if item2 not in data[tables[1]]:
                    final_data.append(item1 + item2)
        for item1 in data[tables[1]]:
            for item2 in tables_needed[tables[0]]:
                if item2 not in data[tables[0]]:
                    final_data.append(item2 + item1)
        for item1 in data[tables[0]]:
            for item2 in data[tables[1]]:
                final_data.append(item1 + item2)
    else:
        table1 = list(data.keys())[0]
        flag = False
        table2 = tables_found[1]
        if table1 == tables_found[1]:
            table2 = tables_found[0]
            flag = True

        for item1 in data[table1]:
            for item2 in tables_needed[table2]:
                if flag:
                    final_data.append(item2 + item1)
                else:
                    final_data.append(item1 + item2)
    display_output(tables_found, columns_in_table, final_data, join=True)

def join_data(clauses, columns, tables):
    operators = ['<=','>=','<', '>', '=']
    needed_data = {}
    for query in clauses:
        needed = []
        for operator in operators:
            if operator in query:
                needed = query.split(operator)
                break
        needed = [(re.sub(' +', ' ', word)).strip() for word in needed]
        table, column = search_column(needed[0], tables)
        needed_data[table] = []
        query = query.replace(needed[0], ' ' + column + ' ')
        for data in tables_needed[table]:
            evaluator = generate_evals(data, table, query)
            try:
                if eval(evaluator):
                    needed_data[table].append(data)
                    # print(needed_data)
            except NameError:
                sys.stderr.write("Error: Invalid condition\n")
                quit(-1)
    return needed_data

def join(columns, tables):
    '''
        Display columns from two or more tables
    '''
    columns_in_table = {}
    tables_found = []
    if len(columns) == 1 and columns[0] == '*':
        for table in tables:
            columns_in_table[table] = []
            for column in tables_list[table]:
                columns_in_table[table].append(column)
        tables_found = tables
    else:
        for column in columns:
            table, column = search_column(column, tables)
            if table not in columns_in_table.keys():
                columns_in_table[table] = []
                tables_found.append(table)
            columns_in_table[table].append(column)
    
    data_injoin = []

    # print(tables_found, columns)
    if len(tables_found) == 2:
        for item1 in tables_needed[tables_found[0]]:
            for item2 in tables_needed[tables_found[1]]:
                data_injoin.append(item1 + item2)
        display_output(tables_found, columns_in_table, data_injoin, join=True)
    else:
        display_output(tables_found, columns_in_table)
        

def get_headers(table, columns):
    string = ''
    for column in columns:
        if string != '':
            string += '|'
        string += table + '.' + column
    return string

def generate_evals(row, table, clauses):
    evaluator = ''
    clauses = [(re.sub(' +', ' ', i)).strip() for i in clauses.split()]
    for condition in clauses:
        if condition == '=':
            evaluator += condition * 2
        elif condition.lower() == 'and' or condition.lower() == 'or':
                evaluator += ' ' + condition.lower() + ' '
        elif '.' in condition:
            table_found, column = search_column(condition, [table])
            # print(tables_list[table_here].index(column))
            # print(row)
            evaluator += row[tables_list[table_found].index(column)]
        elif condition in tables_list[table]:
            evaluator += row[tables_list[table].index(condition)]
        else:
            evaluator += condition
    # print(evaluator)
    return evaluator


def search_column(column, tables):
    if '.' in column:
        table, column = column.split('.')
        table = (re.sub(' +', ' ', table)).strip()
        column = (re.sub(' +', ' ', column)).strip()
        if table not in tables:
            sys.stderr.write("Error: No such table exists.\n")
            quit(-1)
        return table, column
    cnt = 0
    table_found = ''
    for table in tables:
        if column in tables_list[table]:
            cnt += 1
            table_found = table
    if cnt > 1 or cnt == 0:
        sys.stderr.write("Error: Column name not defined correctly.\n")
        quit(-1)
    return table_found, column

def display_output(tables, columns, data = tables_needed, join=False, distinct=False):
    if distinct and join:
        header1 = get_headers(tables[0], columns[tables[0]])
        header2 = get_headers(tables[1], columns[tables[1]])
        print(header1 + '|' + header2)
        print("-"*len(header1 + '|' + header2))
        result = []
        for item in data:
            ans = ''
            for column in columns[tables[0]]:
                ans += item[tables_list[tables[0]].index(column)] + '\t|'
            for column in columns[tables[1]]:
                ans += item[tables_list[tables[1]].index(column) +
                            len(tables_list[tables[0]])] + '\t|'
            if ans.strip('\t|') not in result:
                result.append(ans.strip('\t|'))
        for row in result:
            print(row)
            
            
            
    elif join:
        header1 = get_headers(tables[0], columns[tables[0]])
        header2 = get_headers(tables[1], columns[tables[1]])
        print(header1 + '|' + header2)
        print("-"*len(header1 + '|' + header2))
        for item in data:
            ans = ''
            for column in columns[tables[0]]:
                ans += item[tables_list[tables[0]].index(column)] + '\t|'
            for column in columns[tables[1]]:
                ans += item[tables_list[tables[1]].index(column) +
                            len(tables_list[tables[0]])] + '\t|'
            print(ans.strip('\t|'))

    else:
        for table in tables:
            print(get_headers(table, columns[table]))
            print("-"*len(get_headers(table, columns[table])))
            for data in data[table]:
                ans = ''
                for column in columns[table]:
                    ans += data[tables_list[table].index(column)] + '\t|'
                print(ans.strip('\t|'))
            print("")

read_metadata(META)
query = "Select A, D from table1, table2 where A < 0 and D > 10000"
parse_query(query)
    