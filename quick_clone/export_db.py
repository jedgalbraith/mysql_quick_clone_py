# code: utf-8
# python2

import mysql.connector, os, tarfile, shutil, quick_clone.util as util

def execute(user, password, host, port, database, dir, tables):

    # ensure path contains forward slash
    if dir[-1:] != "/":
        dir += "/"

    if not os.path.isdir(dir):
        print "ERROR: File Location does not exist"
        quit(1)

    # connect to database
    cnx = util.connect_db(user, password, host, port, database)

    # cursor
    cursor = cnx.cursor()

    # export database to flat files
    export_database(cursor, database, dir, tables)

    # close cursor
    cursor.close()

    # end transaction
    cnx.close()

    # tarball
    print "Creating tarball..."
    tar = tarfile.open(dir + database + '.tar.gz', 'w:gz')
    tar.add(dir + database, arcname=database)
    tar.close()

    # remove dirs
    print "Removing temporary dirs..."
    shutil.rmtree(dir + database, ignore_errors=True)

    print "Quick Clone export successful!"

def export_database(cursor, database, dir, tables):

    print "Table export initiated..."
    tables = get_tables(cursor, tables)

    for table in tables:

        try:
            # create a directory based on table name
            os.makedirs(dir + database + '/' + table)

            create_schema_file(cursor, database, table, dir)

            create_data_file(cursor, database, table, dir)

            create_checksum_file(cursor, database, table, dir)

        except mysql.connector.Error as err:
            print "ERROR: Failed to execute SQL command: {}".format(err)
            quit(1)

        # except:
        #     print "ERROR: Something went really wrong"
        #     quit(1)

    print "Table export finished..."

def get_tables(cursor, tables):

    print "Preparing table list..."
    if tables == None:
        # build list of tables from db
        tables = []
        cursor.execute("SHOW TABLES")
        for table in cursor.fetchall():
            tables += [table[0]]

        # don't proceed if no tables to clone
        if tables == []:
            print "No tables in database"
            quit(1)
        else:
            print "All db tables added to table list..."
            return tables

    else:
        # split comma separated list of tables into list
        tables = tables.split(',')
        print "Requested tables added to table list..."
        return tables

def create_schema_file(cursor, database, table, dir):
    # query for show table sql
    cursor.execute("SHOW CREATE TABLE " + table)
    # fetch result
    show_create_result = cursor.fetchall()
    # drill into result for raw query
    show_create = show_create_result[0][1]

    # create a file that contains the sql
    file_schema = open(dir + database + '/' + table + '/schema.sql', 'w')
    file_schema.write(show_create)
    file_schema.close()

def create_data_file(cursor, database, table, dir):
    cursor.execute("SELECT * FROM " + table + " INTO OUTFILE '" + dir + database + '/' + table + "/data'")

# create a file containing the checksum of the table dump
def create_checksum_file(cursor, database, table, dir):
    cursor.execute("CHECKSUM TABLE " + table)
    # fetch result
    checksum_result = cursor.fetchall()
    # drill into result for raw query
    checksum = checksum_result[0][1]
    # create a file that contains the sql
    file_checksum = open(dir + database + '/' + table + '/checksum', 'w')
    file_checksum.write(str(checksum))
    file_checksum.close()