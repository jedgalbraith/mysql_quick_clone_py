#!/usr/bin/env python
# code: utf-8
# python2

import mysql.connector, os, tarfile

# grab tarball file

# untar
tar = tarfile.open("quick_clone.tar.gz")
tar.extractall()
tar.close()

# connection
cnx = mysql.connector.connect(
    user        = 'root',
    password    = 'root',
    host        = '127.0.0.1',
    database    = 'python-quick-clone'
)

# cursor
cursor = cnx.cursor()

# grab table names from dir names
table_list = os.listdir('quick_clone')

for table_name in table_list:
    # change existing table names _old
    cursor.execute("RENAME TABLE " + table_name + " TO " + table_name + "_old")

    # read in table schema
    create_file = open( "quick_clone/" + table_name + "/file_schema.sql", "w")

    create_command = create_file.read()

    create_file.close()

    # rename table to include _tmp
    create_command = create_command.replace("`" + table_name + "`", "`" + table_name + "_tmp`")

    # create table with _tmp
    cursor.execute(create_command)

    cursor.execute("LOAD DATA INFILE '" + os.getcwd() + "/quick_clone/" + table_name + "/data' INTO TABLE " + table_name + "_tmp")

    # confirm checksums
    # get checksum for source
    checksum_src = 

    # get checksum on tmp table
    checksum_tmp = cursor.execute("CHECKSUM TABLE " + table_name + "_tmp")

    # compare checksums
    if checksum_tmp != checksum_src:
        print

    # delete old table

    # rename tmp table

# remove tarball