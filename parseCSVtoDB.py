from sys import argv

# Using camelCase for functions
# using split_by_underscores for variables

script, filename = argv  
# could stand to have some validation on what arguments are received

# keys: columns from spreadsheet; values: (table, column in table).
possible_data_columns = {'Last_Name': ('t4b', 'last_name'), 
                         'First_Name_MI': ('t4b', 'first_name'), 
                         'Phone1': ('t4b', 'phone'), 
                         'Email1': ('t4b', 'email'),
                         'Address': ('t4b', 'address'),
                         'City': ('t4b', 'city'),
                         'State': ('t4b', 'state'),
                         'Zip': ('t4b', 'zipcode'),
                         'Volunteer': ('t4b', 'will_volunteer'),
                         'Pct': ('t4b', 'precinct'),
                         'Capt': ('t4b', 'will_captain'),
                         'DSA_Aus_Memb': ('affiliations', 'dsa_austin')}


columns_in_file = list()
ordered_columns_for_insert = dict()
def establishColumnOrder(header_row_data):
    for column_header in header_row_data:
        try:
            columns_in_file.append(possible_data_columns[column_header])
        except KeyError:
            print "There is no database column corresponding to data column \'%s\'" % column_header
            print "All data in this column will be ignored!!!"
            columns_in_file.append(None)
        else:
            try:
                ordered_columns_for_insert[columns_in_file[-1][0]] += (columns_in_file[-1][1],)
            except KeyError:
                ordered_columns_for_insert[columns_in_file[-1][0]] = (columns_in_file[-1][1],)


# open file
data_to_import = open(filename, 'r')

first = True

for row in data_to_import:
    row_data = row.rstrip().split(',')
    inserts = dict()
    if first:
        establishColumnOrder(row_data)
        first = False
        continue

    for entry, column_data in zip(row_data, columns_in_file):
        if column_data is None:
            continue
        if entry.rstrip() is '':
            entry = None
        # more processing and validation here
        table, column = column_data
        try:
            inserts[table] += (entry if entry is not None else NULL,)
        except KeyError:
            inserts[table] = (entry,)

