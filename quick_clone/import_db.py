# code: utf-8
# python2

# todo consider running checksum on dest table to see if src table has update

import mysql.connector, os, tarfile, shutil, quick_clone.util as util

def execute(user, password, host, port, database, archive):
    
    # extract archive
    tar = util.extract_tar(archive)
    
    # get list of table names from archive
    table_names = get_table_names(tar)

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

    # add foreign key constraints back in
    add_fk(cursor, extracted_dir, table_names)
    
    # rename _clone tables & FK to original name
    rename_tables(cursor, extracted_dir, table_name)
    
    # enable checks
    util.mysql_checks(1)

    cursor.close()
    cnx.commit()
    cnx.close()

    # remove tarball
    os.remove(extracted_dir + '.tar.gz')

    # remove extracted directory
    shutil.rmtree(extracted_dir, ignore_errors=True)

def get_table_names(tar):
    extracted_dir = os.path.realpath(tar.getnames()[0])
    table_names = os.listdir(extracted_dir)
    tar.close()
    return table_names

def import_cloned_tables(cnx, cursor, extracted_dir, table_names):

    print "STATUS: Importing cloned tables..."
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
            drop_tables(cursor, "_clone")
            quit(1)
        # except:
        #     print "ERROR: Something went really wrong"
        #     quit(1)
    print "STATUS: Import successful..."

def create_clone_table(cursor, extracted_dir, table_name):
    # remove foreign key constraints
    with open(extracted_dir + '/' + table_name + "/schema.sql", "r") as create_sql_f:
        for line in create_sql_f:
            if not line.startswith("  CONSTRAINT"):
                create_sql_str += line + "\r\n"

    # rename table to include _clone
    create_sql = create_sql_str.replace("`" + table_name + "`", "`" + table_name + "_clone`")
    # create table with _tmp
    cursor.execute(create_sql)

def drop_tables(cursor, tables):
    if isinstance(tables, basestring):
        print "STATUS: Dropping " + like + " tables..."
        cursor.execute("SHOW TABLES LIKE '%" + like +"'")
        remove_tables = cursor.fetchall()
        if remove_tables != []:
            for remove_table in remove_tables:
                cursor.execute("DROP TABLE " + remove_table[0])
            print "STATUS: " + like + " tables removed..."
    else:
        print "STATUS: Removing original tables..."
        for table_name in table_names:
            cursor.execute("DROP TABLE IF EXISTS " + table_name)
        print "STATUS: Remove successful..."

def verify_checksums(cursor, extracted_dir, table_names):
    print "STATUS: Verifying checksums..."
    for table_name in table_names:
        if not verify_checksum(cursor, extracted_dir, table_name):
            print "checksums didn't match for table: " + table_name
            drop_tables(cursor, "_clone")
            quit(1)
    print "STATUS: Verification successful..."
    
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

def rename_tables(cursor, extracted_dir, table_names):

    print "STATUS: Renaming _clone tables to original names..."
    for table_name in table_names:
        # ensure table exists
        cursor.execute("SHOW TABLES LIKE '" + table_name + "_clone'")
        if cursor.fetchall() != []:
            cursor.execute("RENAME TABLE " + table_name + "_clone TO " + table_name)

            fk_sql = get_fk_sql(extracted_dir, table_name)
            if fk_sql != "":
                # drop current FK
                cursor.execute("ALTER TABLE " + table_name + " DROP FOREIGN KEY FK_" + table_name)
                # add properly named FK
                cursor.execute("ALTER TABLE " + table_name + " ADD " + fk_sql)
    print "STATUS: Rename successful..."

def add_fk(cursor, extracted_dir, table_names):

    for table_name in table_names:
        # open schema file
        with open(extracted_dir + '/' + table_name + "/schema.sql", "r") as create_sql_f:
            # extract constraint sql
            create_sql_list = []
            for line in create_sql_f:
                if line.startswith("  CONSTRAINT"):
                    line = line.lstrip("  ")
                    line = line.rstrip("\n")
                    create_sql_list += [line]

        for create_sql in create_sql_list:
            # run alter table sql
            create_sql = "ALTER TABLE " + table_name + " ADD FOREIGN KEY " + create_sql
            cursor.execute(create_sql)
