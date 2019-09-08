'''
Created On: Aug 29, 2019
Author: Deepti

Build an SQL parser through CLI
'''


# TO DO : Check errors

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

    # print(columns, functions, distincts, dist_pair)
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
    elif len(functions) > 0:
        process_func(clauses, columns, tables_inquery[0], functions) # since only single column
    elif len(tables_inquery) == 1:
        normal_where(clauses, columns, tables_inquery[0])
    elif len(tables_inquery) > 1 and len(clauses) == 0:
        join(columns, tables_inquery)
    elif len(tables_inquery) > 1 and len(clauses) > 0:
        join_where(clauses, columns, tables_inquery)

def process_func(clauses, columns, table, functions):
    '''
        Process Min, Max, Avg, Sum
    '''
    columns = []
    func = functions[0][0]
    columns.append(functions[0][1])
    # print(clauses)
    print(bring_forth(table, columns))
    print("-"*len(bring_forth(table, columns)))

    if(len(clauses) != 0):
        data = []
        for row in tables_needed[table]:
            evaluator = solve(row, table, clauses)
            if eval(evaluator):
                for column in columns:
                    data.append(float(row[tables_list[table].index(column)]))
    else:
        data = []
        for row in tables_needed[table]:
            for column in columns:
                data.append(float(row[tables_list[table].index(column)]))
    
    result = 0
    if func.lower() == 'avg':
        result += sum(data) / len(data)
    elif func.lower() == 'sum':
        result += sum(data)
    elif func.lower() == 'max':
        result = max(data)
    elif func.lower() == 'min':
        result = min(data)

    print(result)

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
        print(bring_forth(table, columns))
        print("-"*len(bring_forth(table, columns)))
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
        print(bring_forth(table, columns))
        print("-"*len(bring_forth(table, columns)))
        for data in tables_needed[table]:
            ans = ''
            for column in columns:
                ans += data[tables_list[table].index(column)] + '\t|'
            print(ans.strip('\t|'))
    
    elif len(clauses) == 0:
        print(bring_forth(table, columns))
        print("-"*len(bring_forth(table, columns)))
        for row in tables_needed[table]:
            ans = ''
            for column in columns:
                ans += row[tables_list[table].index(column)] + '\t|'
            print(ans.strip('\t|'))
    
    elif len(clauses) >= 1:
        if len(columns) == 1 and columns[0] == '*':
            columns = tables_list[table]
        print(bring_forth(table, columns))
        print("-"*len(bring_forth(table, columns)))
        for row in tables_needed[table]:
            evaluator = solve(row, table, clauses)
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
    # print(clauses)
    
    condition1 = clauses[0]
    for operator in operators:
        if operator in condition1:
            condition1 = condition1.split(operator)

    if len(condition1) == 2 and '.' in condition1[1]:
        condition_join(condition1, columns, tables)
    else:
        join_conditionally(now, clauses, columns, tables)

def condition_join(clauses, columns, tables):
    '''
        With a condition
    '''
    if len(clauses) > 2:
        sys.stderr.write("Error: Join condition invalid\n")
        quit(-1)
        
    conditional_columns = [(re.sub(' +', ' ', word)).strip() for word in clauses]
    
    columns_cond = {}
    tables_found = []
    for column in conditional_columns:
        table, column = search_column(column, tables)
        if table not in columns_cond.keys():
            columns_cond[table] = []
            tables_found.append(table)
        columns_cond[table].append(column)
    keep = []

    column1 = tables_list[tables[0]].index(columns_cond[tables[0]][0])
    column2 = tables_list[tables[1]].index(columns_cond[tables[1]][0])

    for data in tables_needed[tables[0]]:
        for row in tables_needed[tables[1]]:
            evaluator = data[column1] + '==' + row[column2]
            if eval(evaluator):
                keep.append(data + row)
    
    final_columns = {}
    final_tables = []

    flag = 0
    if len(columns) == 1 and columns[0] == '*':
        for table in tables:
            final_columns[table] = []
            for column in tables_list[table]:
                if column in columns_cond[table]:
                    if flag == 0:
                        final_columns[table].append(column)
                        flag = 1
                        continue
                else:
                    final_columns[table].append(column)
        final_tables = tables
    else:
        for column in columns:
            table, column = search_column(column, tables)
            if table not in final_columns.keys():
                final_columns[table] = []
                final_tables.append(table)
            final_columns[table].append(column)

    display_output(final_tables, final_columns, keep, join=True)


def join_conditionally(now, clauses, columns, tables):
    '''
        Without a condition
    '''
    data = join_data(clauses, columns, tables)

    columns_in_table = {}
    tables_found = []
    if columns[0] == '*':
        if len(columns != 1):
            sys.stderr.write("Error: Select function invalid\n")
            quit(-1)
        if len(columns) == 0:
            for table in tables:
                columns_in_table[table] = []
                for column in tables_list[table]:
                    columns_in_table[table].append(column)
            tables_found = tables
    else:
        for column in columns:
            table, column = search_column(column, tables)
            if table not in columns_in_table.keys():
                tables_found.append(table)
                columns_in_table[table] = []
            columns_in_table[table].append(column)

    final_data = []
    if now == 'and':
        for obja in data[tables[0]]:
            for objb in data[tables[1]]:
                final_data.append(obja + objb)
    elif now == 'or':
        for obja in data[tables[0]]:
            for objb in tables_needed[tables[1]]:
                if objb not in data[tables[1]]:
                    final_data.append(obja + objb)
        for item1 in data[tables[1]]:
            for item2 in tables_needed[tables[0]]:
                if item2 not in data[tables[0]]:
                    final_data.append(item2 + item1)
        for obja in data[tables[0]]:
            for objb in data[tables[1]]:
                final_data.append(obja + objb)
    else:
        table1 = list(data.keys())[0]
        flag = False
        table2 = tables_found[1]
        if table1 == tables_found[1]:
            table2 = tables_found[0]
            flag = True

        for obja in data[table1]:
            for objb in tables_needed[table2]:
                if flag:
                    final_data.append(objb + obja)
                else:
                    final_data.append(obja + objb)
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
            evaluator = solve(data, table, query)
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
        

def bring_forth(table, columns):
    string = ''
    for column in columns:
        if string != '':
            string += '|'
        string += table + '.' + column
    return string

def solve(row, table, clauses):
    evaluator = ''
    clauses = [(re.sub(' +', ' ', i)).strip() for i in clauses.split()]
    for condition in clauses:
        if condition == '=':
            evaluator += condition * 2
        elif condition.lower() == 'and' or condition.lower() == 'or':
                evaluator += ' ' + condition.lower() + ' '
        elif '.' in condition:
            table_found, column = search_column(condition, [table])
            evaluator += row[tables_list[table_found].index(column)]
        elif condition in tables_list[table]:
            evaluator += row[tables_list[table].index(condition)]
        else:
            evaluator += condition
    return evaluator


def search_column(column, tables):
    table_found = ''
    if '.' in column:
        table, column = column.split('.')
        table = (re.sub(' +', ' ', table)).strip()
        column = (re.sub(' +', ' ', column)).strip()
        if table not in tables:
            sys.stderr.write("Error: No such table exists.\n")
            quit(-1)
        return table, column
    count = 0
    for table in tables:
        if column in tables_list[table]:
            count += 1
            table_found = table
    if count > 1 or count == 0:
        sys.stderr.write("Error: Column name not defined correctly.\n")
        quit(-1)
    return table_found, column

def display_output(tables, columns, data = tables_needed, join=False, distinct=False):
    if distinct and join:
        header1 = bring_forth(tables[0], columns[tables[0]])
        header2 = bring_forth(tables[1], columns[tables[1]])
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
    elif len(tables) == 1 and join:
        header1 = bring_forth(tables[0], columns[tables[0]])
        print(header1)
        print("-"*len(header1))
        for item in data:
            ans = ''
            for column in columns[tables[0]]:
                ans += item[tables_list[tables[0]].index(column)] + '\t|'
            print(ans.strip('\t|'))
            
    elif join:
        header1 = bring_forth(tables[0], columns[tables[0]])
        header2 = bring_forth(tables[1], columns[tables[1]])
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
            print(bring_forth(table, columns[table]))
            print("-"*len(bring_forth(table, columns[table])))
            for data in data[table]:
                ans = ''
                for column in columns[table]:
                    ans += data[tables_list[table].index(column)] + '\t|'
                print(ans.strip('\t|'))
            print("")

read_metadata(META)
query = sys.argv[1]
parse_query(query)
    