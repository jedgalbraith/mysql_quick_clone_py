import mysql.connector, os, tarfile, shutil
from mysql.connector import errorcode

def connect_db(user, password, host, port, database):
    print "Connecting database..."
    try:
        cnx = mysql.connector.connect(
            user     = user,
            password = password,
            host     = host,
            port     = port,
            database = database,
            charset  = 'utf8')

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("ERROR: Something is wrong your username or password")
            quit(1)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("ERROR: Database does not exist")
            quit(1)
        else:
            print(err)
            quit(1)
    except:
        print "something else went wrong"

    print "Connection successful..."
    return cnx

def extract_tar(file):
    print "Extracting file..."
    try:
        tar = tarfile.open(file)
    except:
        print "Couldn't find import file at: " + file
        quit(1)
    
    tar.extractall()
    return tar
    print "Extract successful..."

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
