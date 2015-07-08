# t4b-database-manager

Right now this is set up to write to a local database.

The script can be called from the command line:

    python2.7 parseCSVtoDB.py csv_file.csv

It will automatically create the tables (will **not** create the database) if they don't exist and populate them with the data in the CSV file. 
The script does not require the columns to be in any order, but the column headings should not be changed. The script does check for data collisions based on the email address; presently it reports collisions to a new file whose name is based on the input file but with "collisions_" prepended.  

This script uses [peewee](http://peewee.readthedocs.org/en/latest/index.html) to interact with the mysql database. As such you will need to have peewee installed in your python distribution as well as [MySQLdb](http://stackoverflow.com/a/7461662/2754587).  