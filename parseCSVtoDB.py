from sys import argv
from peewee import *
# Using camelCase for functions
# using split_by_underscores for variables
from playhouse.shortcuts import *
import copy

script, filename = argv  
# could stand to have some validation on what arguments are received

# establish database connection
# put the real connection data in an unshared file
# t4b_db = MySQLDatabase('t4b', 
#                        host="localhost", 
#                        user="paul", 
#                        password="nothing")
t4b_db = SqliteDatabase('t4b.db')  # A local sqlite database for testing!

# keys: columns from spreadsheet; values: (table, column in table).
possible_data_columns = {'Last_Name': ('person', 'last_name'), 
                         'First_Name_MI': ('person', 'first_name_mi'), 
                         'Phone1': ('person', 'phone'), 
                         'Email1': ('person', 'email'),
                         'Address': ('person', 'address'),
                         'City': ('person', 'city'),
                         'State': ('person', 'state'),
                         'Zip': ('person', 'zipcode'),
                         'Volunteer': ('person', 'will_volunteer'),
                         'Pct': ('person', 'precinct'),
                         'Capt': ('person', 'will_captain'),
                         'DSA_Aus_Memb': ('affiliation', 'dsa_austin')}

insert_priority = ['person', 'affiliation']
boolean_columns = ['dsa_austin', 'will_volunteer', 'will_captain']

natural_column_ordering = ['first_name_mi', 'last_name', 'phone', 'email', 
                           'address', 'city', 'state', 'zipcode', 
                           'will_volunteer', 'precinct', 'will_captain']
valid_columns_in_file = list()
valid_column_headers_in_file = list()

def establishColumnOrder(header_row_data):
    for column_header in header_row_data:
        try:
            valid_columns_in_file.append(possible_data_columns[column_header])
        except KeyError:
            print "There is no database column corresponding to data column '%s'" % column_header
            print "All data in this column will be ignored!!!"
            valid_columns_in_file.append(None)
        else:
            valid_column_headers_in_file.append(column_header) 

def rowInFileColumnOrdering(row):
    returnrow = ""
    for column_data in valid_columns_in_file:
        if column_data:
            table, column = column_data
            try: 
                returnrow += (str(row[table][column]) + ","  if row[table][column] else "")
            except KeyError:
                returnrow += ","
    return returnrow

class BaseModel(Model):
    '''Establish the database from which all other Models will inherit'''

    class Meta:
        database = t4b_db

class Person(BaseModel):
    '''Establish the model for person table'''
    person_id = PrimaryKeyField()
    last_name = CharField(max_length=20, null=True)
    first_name_mi = CharField(max_length=20, null=True)
    # we need to decide which fields can be left empty!!
    phone = CharField(max_length=11, null=True)
    email = CharField(max_length=254, unique=True, null=True)
    address = CharField(max_length=100, null=True)
    city = CharField(max_length=45, null=True)
    state = CharField(max_length=2, null=True)
    zipcode = CharField(max_length=10, null=True)
    precinct = IntegerField(null=True)
    will_volunteer = BooleanField(null=True)
    will_captain = BooleanField(null=True)
    
    def __str__(self):
        returnstring = "%s," % self.person_id
        try:
            affiliations = Affiliation.get(Affiliation.person == self.person_id)
        except Affiliation.DoesNotExist:
            affiliations = None
        for column in valid_columns_in_file:
            if column is not None: 
                if column[0] == "person":
                    returnstring += (str(getattr(self, column[1])) + "," if getattr(self, column[1]) else ",")
                if column[0] == "affiliation":
                    if affiliations:
                        returnstring += (str(getattr(affiliations, column[1])) + "," if getattr(affiliations, column[1]) else ",")
                    else:
                        returnstring += ","

        return returnstring

class Affiliation(BaseModel):
    person = ForeignKeyField(Person)
    # we might consider making this a many-to-one type of table where only
    #    one affiliation is stored per row
    dsa_austin = BooleanField(null=True)

t4b_db.connect()
t4b_db.create_tables([Person, Affiliation], safe=True)

data_to_import = open(filename, 'r')
first = True

def resolveCollision(existing_data, new_data, valid_columns):
    resolve_action = "ignore"
    # when the existing_data holds more information than new data, do nothing
    # when new_data conflicts with existing_data, send it up for review
    # when new_data fills in gaps in existing_data, update existing_data
    return_data = copy.deepcopy(new_data)
    return_data['person']['person_id'] = existing_data['person_id']

    # print new_data
    for column_data in valid_columns:
        if column_data is None: 
            continue
        table, column = column_data
        if column in boolean_columns and column in return_data[table].keys() and return_data[table][column]:
            return_data[table][column] = True
        if existing_data[column] is not None:
            if column in return_data[table].keys() and return_data[table][column] != existing_data[column]:
                resolve_action = "report"
                print "Conflict on column: %s. old value: '%s', new value: '%s'" % (column, existing_data[column], return_data[table][column])
                break
            if column not in return_data[table].keys() or return_data[table][column] is None:
                return_data[table][column] = existing_data[column]
                resolve_action = "update"
        else:
            try:
                if return_data[table][column] is not None:
                    resolve_action = "update"
            except KeyError:
                ''''''
    # print resolve_action
    return (resolve_action, return_data)

def prepareCSV(db_dict, valid_columns):
    return_value = "%s," % db_dict['person_id']
    for column_data in valid_columns:
        if column_data is not None:
            table, column = column_data
            return_value += "%s," % db_dict[column]
    return return_value

collisions_log = ""
for row in data_to_import:
    row_data = row.strip().split(',')
    insertable = dict()
    for table in insert_priority:
        insertable[table] = dict() 
        # this could probably be made more efficient by copying a template
    if first:
        establishColumnOrder(row_data)
        first = False
        continue

    for entry, column_data in zip(row_data, valid_columns_in_file):
        if column_data is None:
            continue
        if entry.strip() is '':
            continue
        # more processing and validation here, 
        # perhaps a data score that determines whether we keep the row
        table, column = column_data
        insertable[table][column] = entry.strip()
    try:
        with t4b_db.transaction():
            person = Person.create(**insertable['person'])
    except IntegrityError:
        duplicate_in_db = (Person
                    .select(Person, Affiliation.id, Affiliation.dsa_austin)
                    .join(Affiliation, join_type=JOIN.LEFT_OUTER)
                    .where(Person.email == insertable['person']['email'])
                    .dicts()
                    .get())

        action, data = resolveCollision(duplicate_in_db, insertable, valid_columns_in_file)
        if action == "report":
            formatted_new_duplicate = rowInFileColumnOrdering(insertable)
            # print "Duplicate email detected: %s" % (formatted_new_duplicate, )
            collisions_log += "new, ,%s \n" % formatted_new_duplicate
            # print "Duplicate to: %s" % (duplicate_in_db, )

            #####  CSV-ify duplicate_in_db (which is a dict)
            collisions_log += "existing, %s \n" % prepareCSV(duplicate_in_db, valid_columns_in_file)
            # put this duplicate in a file for review
        if action == "update":
            ''''''
            # print "should update: %s" % data
            updateable_person = dict_to_model(Person, data['person'])
            updateable_person.save()
            # deal with affiliations
            if data['affiliation']:
                updateable_affiliation = dict_to_model(Affiliation, data['affiliation'])
                updateable_affiliation.person = data['person']['person_id']
                if duplicate_in_db['id'] is not None:
                    updateable_affiliation.id = duplicate_in_db['id']
                updateable_affiliation.save()

    else:
        if insertable['affiliation']:
            affiliation = Affiliation.create(person=person, **insertable['affiliation'])

if collisions_log != "":
    valid_column_headers_csv = ""
    for column_header in valid_column_headers_in_file:
        valid_column_headers_csv += "%s, " % column_header
    collisions_file_name = "collisions_%s" % filename
    collisions_file = open(collisions_file_name, 'w')
    collisions_file.write("Status, ID, %s \n" % valid_column_headers_csv)
    collisions_file.write(collisions_log)
    collisions_file.close() 