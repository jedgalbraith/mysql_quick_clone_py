# code: utf-8
# python2

import mysql.connector, os, tarfile, shutil

def execute(host, user, password, port, database, archive_location):
    
    db_name = "python-quick-clone-dst"

    # untar db dump
    tar = tarfile.open(archive_location)
    tar.extractall()
    extracted_dir = os.path.realpath(tar.getnames()[0])
    tar.close()

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

    import_database(cursor, db_name, extracted_dir)

    cursor.close()

    cnx.commit()

    cnx.close()

    # remove tarball
    os.remove(extracted_dir + '.tar.gz')

    shutil.rmtree(extracted_dir, ignore_errors=True)

def import_database(cursor, db_name, extracted_dir):

    for table_name in os.listdir(extracted_dir):

        try:
            # todo: check if table exists
            # change existing table names _old
            cursor.execute("RENAME TABLE " + table_name + " TO " + table_name + "_old")

            create_tmp_table(cursor, extracted_dir, table_name)   

            cursor.execute("LOAD DATA INFILE '" + extracted_dir + '/' + table_name + "/data' INTO TABLE " + table_name + "_tmp")

            if not verify_checksums(cursor, extracted_dir, table_name):
                print "checksums didn't match for table" + table_name
                quit(1)

            # delete old table
            cursor.execute("DROP TABLE " + table_name + "_old")

            # rename tmp table
            cursor.execute("RENAME TABLE " + table_name + "_tmp TO " + table_name)

        except mysql.connector.Error as err:
            print "ERROR: Failed to execute SQL command: " + err.msg
            quit(1)
        # except:
        #     print "ERROR: Something went really wrong"
        #     quit(1)

def create_tmp_table(cursor, extracted_dir, table_name):
    # read in table schema
    create_file = open(extracted_dir + '/' + table_name + "/file_schema.sql", "r")

    create_command = create_file.read()

    create_file.close()

     # rename table to include _tmp
    create_command = create_command.replace("`" + table_name + "`", "`" + table_name + "_tmp`")
    # create table with _tmp
    cursor.execute(create_command)

def verify_checksums(cursor, extracted_dir, table_name):
    # get checksum for source
    checksum_src_file = open(extracted_dir + '/' + table_name + "/checksum", "r")

    checksum_src = int(checksum_src_file.read())

    try:
        # get checksum on tmp table
        cursor.execute("CHECKSUM TABLE " + table_name + "_tmp")
        checksum_tmp = cursor.fetchall()[0][1]

    except mysql.connector.Error as err:
        print "ERROR: Failed to execute SQL command: {}".format(err)
        quit(1)
        
    except:
        print "ERROR: Something went really wrong"
        quit(1)

    if checksum_tmp == checksum_src:
        return True
    else:
        return False