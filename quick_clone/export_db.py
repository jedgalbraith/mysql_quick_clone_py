# code: utf-8
# python2

import mysql.connector, os, tarfile, shutil

def execute(user, password, host, port, database, file_location):

    if file_location[-1:] != "/":
        file_location += "/"

    if not os.path.isdir(file_location):
        print "ERROR: File Location does not exist"
        quit(1)

    try:
        cnx = mysql.connector.connect(
            user        = user,
            password    = password,
            host        = host,
            database    = database
        )

    except:
        print "ERROR: Could not connect to database"
        quit(1)

    # cursor
    cursor = cnx.cursor()

    export_database(cursor, database, file_location)

    # close cursor
    cursor.close()

    # end transaction
    cnx.close()

    # tarball
    tar = tarfile.open(file_location + database + '.tar.gz', 'w:gz')
    tar.add(file_location + database, arcname=database)
    tar.close()

    # remove dirs
    shutil.rmtree(file_location + database, ignore_errors=True)

def export_database(cursor, database, file_location):

    table_list = get_table_names(cursor)

    if table_list == []:
        print "No tables in database"
        quit(1)

    for table_name in table_list:

        try:
            # create a directory based on table name
            os.makedirs(file_location + database + '/' + table_name)

            create_schema_file(cursor, database, table_name, file_location)

            create_data_file(cursor, database, table_name, file_location)

            create_checksum_file(cursor, database, table_name, file_location)

        except mysql.connector.Error as err:
            print "ERROR: Failed to execute SQL command: {}".format(err)
            quit(1)

        # except:
        #     print "ERROR: Something went really wrong"
        #     quit(1)

def get_table_names(cursor):
    
    cursor.execute("SHOW TABLES")

    # build list of db tables
    table_list = []
    for table in cursor.fetchall():
        table_list += [table[0]]
    return table_list

def create_schema_file(cursor, database, table_name, file_location):
    # query for show table sql
    cursor.execute("SHOW CREATE TABLE " + table_name)
    # fetch result
    show_create_result = cursor.fetchall()
    # drill into result for raw query
    show_create = show_create_result[0][1]

    # create a file that contains the sql
    file_schema = open(file_location + database + '/' + table_name + '/schema.sql', 'w')
    file_schema.write(show_create)
    file_schema.close()

def create_data_file(cursor, database, table_name, file_location):
    cursor.execute("SELECT * FROM " + table_name + " INTO OUTFILE '" + file_location + database + '/' + table_name + "/data'")

# create a file containing the checksum of the table dump
def create_checksum_file(cursor, database, table_name, file_location):
    cursor.execute("CHECKSUM TABLE " + table_name)
    # fetch result
    checksum_result = cursor.fetchall()
    # drill into result for raw query
    checksum = checksum_result[0][1]
    # create a file that contains the sql
    file_checksum = open(file_location + database + '/' + table_name + '/checksum', 'w')
    file_checksum.write(str(checksum))
    file_checksum.close()