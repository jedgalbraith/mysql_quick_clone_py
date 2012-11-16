import mysql.connector, os, tarfile, shutil

def connect_db(user, password, host, port, database):
    print "STATUS: Connecting database..."
    try:
        # cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='test-demo-magento')
        cnx = mysql.connector.connect(
            user        = user,
            password    = password,
            host        = host,
            database    = database,
            charset     = 'utf8')

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong your username or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exists")
        else:
            print(err)

    print "STATUS: Connection successful..."
    return cnx

def extract_tar(archive):
    print "STATUS: Extracting archive file..."
    try:
        tar = tarfile.open(archive)
    except:
        print "Couldn't find import file at :" + archive
        quit(1)
    
    tar.extractall()
    print "STATUS: Extract successful..."
    return

def mysql_checks(cursor, switch):
    if switch == 0:
        cursor.execute("SET foreign_key_checks = 0;")
        cursor.execute("SET UNIQUE_CHECKS = 0;")
        cursor.execute("SET sql_mode='NO_AUTO_VALUE_ON_ZERO'")
        cursor.execute("SET NAMES utf8")
    elif switch == 1:
        cursor.execute("SET foreign_key_checks = 1;")
        cursor.execute("SET UNIQUE_CHECKS = 1;")
        cursor.execute("SET sql_mode=''")
