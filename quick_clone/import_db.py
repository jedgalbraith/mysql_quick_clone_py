# code: utf-8
# python2

# todo consider running checksum on dest table to see if src table has update

import mysql.connector, os, tarfile, shutil, quick_clone.util as util

def execute(user, password, host, port, database, file, fk):
    
    # extract file
    tar = util.extract_tar(file)

    # get name of dir, which is name of database
    extracted_dir = os.path.realpath(tar.getnames()[0])
    tar.close()
    
    # get list of table names from file
    table_names = get_table_names(extracted_dir)

    # connect to database
    cnx = util.connect_db(user, password, host, port, database)

    # get cursor
    cursor = cnx.cursor()

    # disable checks
    util.mysql_checks(cursor, 0)
    
    # import clone tables as _clone
    # without fk constraints
    import_cloned_tables(cnx, cursor, extracted_dir, table_names)

    # commit transaction
    cnx.commit()

    # verify checksums
    verify_checksums(cursor, extracted_dir, table_names)

    # drop original tables
    drop_tables(cursor, table_names)

    if fk == True:
        # add foreign key constraints back in
        add_fk(cursor, extracted_dir, table_names)
    else:
        print "Skipping adding foreign key checks back in..."
    
    # rename _clone tables
    rename_tables(cursor, table_names)
    
    # enable checks
    util.mysql_checks(cursor, 1)

    cursor.close()
    cnx.commit()
    cnx.close()

    # remove tarball
    os.remove(extracted_dir + '.tar.gz')

    # remove extracted directory
    shutil.rmtree(extracted_dir, ignore_errors=True)
    print "Quick Clone import successful!"

def get_table_names(extracted_dir):
    table_names = os.listdir(extracted_dir)
    return table_names

def import_cloned_tables(cnx, cursor, extracted_dir, table_names):

    print "Importing cloned tables..."
    for table_name in table_names:
        try:
            # create empty table
            create_clone_table(cursor, extracted_dir, table_name)

            # load data into table
            # todo: dynamically use character set of table
            # cursor.execute("LOAD DATA INFILE '/Users/jedgalbraith/Downloads/test-quick-clone/demo-magento/admin_user/data' INTO TABLE admin_user_tmp")
            cursor.execute("LOAD DATA INFILE '" + extracted_dir + '/' + table_name + "/data' INTO TABLE " + table_name + "_clone CHARACTER SET utf8")

        except mysql.connector.Error as err:
            print "ERROR: Failed to execute SQL command: " + err.msg
            cnx.rollback()
            # drop_tables(cursor, "_clone")
            quit(1)
        # except:
        #     print "ERROR: Something went really wrong"
        #     quit(1)
    print "Import successful..."

def create_clone_table(cursor, extracted_dir, table_name):
    # remove foreign key constraints
    with open(extracted_dir + '/' + table_name + "/schema.sql", "r") as create_sql_f:
        lines = create_sql_f.readlines()

        # loop multiple times due to indicies changing after list items removed
        constr_found = True
        while constr_found:
            for line in lines:
                if line.startswith("  CONSTRAINT "):
                    # get index of previous line
                    line_prev_index = lines.index(line) - 1
                    # get previous line
                    line_prev = lines[line_prev_index]
                    # strip comma from previous line
                    lines[line_prev_index] = line_prev.replace(",\n", "\n")
                    # remove constraint line
                    del lines[lines.index(line)]
                    constr_found = True
                    # since a constraint was found, start over checking again
                    break

                constr_found = False

    # convert sql back into string
    create_sql_str = ""
    for line in lines:
        create_sql_str += line

    # rename table to include _clone
    create_sql = create_sql_str.replace("`" + table_name + "`", "`" + table_name + "_clone`")
    # create _clone table
    cursor.execute(create_sql)

def drop_tables(cursor, table_names):
    if isinstance(table_names, basestring):
        print "Dropping " + table_names + " tables..."
        cursor.execute("SHOW TABLES LIKE '%" + table_names +"'")
        remove_tables = cursor.fetchall()
        if remove_tables != []:
            for remove_table in remove_tables:
                cursor.execute("DROP TABLE " + remove_table[0])
            print "STATUS: " + table_names + " tables removed..."
    else:
        print "Removing original tables..."
        for table_name in table_names:
            cursor.execute("DROP TABLE IF EXISTS " + table_name)
        print "Remove successful..."

def verify_checksums(cursor, extracted_dir, table_names):
    print "Verifying checksums..."
    for table_name in table_names:
        if not verify_checksum(cursor, extracted_dir, table_name):
            print "checksums didn't match for table: " + table_name
            drop_tables(cursor, "_clone")
            quit(1)
    print "Verification successful..."
    
def verify_checksum(cursor, extracted_dir, table_name):
    # get checksum for source
    checksum_src_file = open(extracted_dir + '/' + table_name + "/checksum", "r")

    checksum_src = int(checksum_src_file.read())

    try:
        # get checksum on tmp table
        cursor.execute("CHECKSUM TABLE " + table_name + "_clone")
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

def rename_tables(cursor, table_names):

    print "Renaming _clone tables to original names..."
    for table_name in table_names:
        # ensure table exists
        cursor.execute("SHOW TABLES LIKE '" + table_name + "_clone'")
        if cursor.fetchall() != []:
            cursor.execute("RENAME TABLE " + table_name + "_clone TO " + table_name)

    print "Rename successful..."

def add_fk(cursor, extracted_dir, table_names):

    print "Adding FK constraints back in..."
    for table_name in table_names:
        # open schema file
        with open(extracted_dir + '/' + table_name + "/schema.sql", "r") as create_sql_f:
            # only collect fk constraints
            create_sql_list = []
            for line in create_sql_f:
                if line.startswith("  CONSTRAINT"):
                    line = line.lstrip()
                    line = line.rstrip("\n")
                    line = line.rstrip(",")
                    create_sql_list += [line]

        for create_sql in create_sql_list:
            # run alter table sql
            create_sql = "ALTER TABLE " + table_name + "_clone ADD " + create_sql
            cursor.execute(create_sql)

    print "Add FK constraints successful..."
