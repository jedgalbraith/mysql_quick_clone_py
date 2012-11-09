#!/usr/bin/env python
# code: utf-8
# python2

import mysql.connector, os, tarfile

# connection
cnx = mysql.connector.connect(
    user        = 'root',
    password    = 'root',
    host        = '127.0.0.1',
    database    = 'python-quick-clone'
)

# cursor
cursor = cnx.cursor()

cursor.execute("SHOW TABLES")

# build list of db tables
table_list = []
for table in cursor.fetchall():
    table_list += [table[0]]

for table_name in table_list:

    # create a directory based on table name
    os.makedirs('quick_clone/' + table_name)

    # SCHEMA FILE
    # query for show table sql
    cursor.execute("SHOW CREATE TABLE " + table_name)
    # fetch result
    show_create_result = cursor.fetchall()
    # drill into result for raw query
    show_create = show_create_result[0][1]
    # create a file that contains the sql
    file_schema = open('quick_clone/' + table_name + '/file_schema.sql', 'w')
    file_schema.write(show_create)
    file_schema.close()

    # DATA FILE
    # create a file containing the checksum of the table dump
    cursor.execute("SELECT * FROM " + table_name + " INTO OUTFILE '" + os.getcwd() + "/quick_clone/" + table_name + "/data'")

    # CHECKSUM FILE
    # create a file containing the checksum of the table dump
    cursor.execute("CHECKSUM TABLE " + table_name)
    # fetch result
    checksum_result = cursor.fetchall()
    # drill into result for raw query
    checksum = checksum_result[0][1]
    # create a file that contains the sql
    file_checksum = open('quick_clone/' + table_name + '/checksum', 'w')
    file_checksum.write(str(checksum))
    file_checksum.close()

# close cursor
cursor.close()

# end transaction
cnx.close()

# tarball
tar = tarfile.open('quick_clone.tar.gz', 'w:gz')
tar.add('quick_clone')
tar.close()