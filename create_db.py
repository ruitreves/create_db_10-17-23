import sqlite3
from sqlite3 import Error
from sqlite3 import OperationalError
import os
import pandas as pd

def create_connection(path_to_db_file: str) -> sqlite3.Connection:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(path_to_db_file)
        return conn
    except Error as e:
        print(e)

    return conn
'''utf-8, utf8, latin-1, latin1, iso-8859-1, iso8859-1, mbcs (Windows only), ascii, us-ascii, utf-16, utf16, utf-32, utf32'''


def load_table(conn, sql_file):
    df = pd.read_csv(sql_file, encoding='latin-1')
    #print(df.head)
    df.to_sql(name='spotify', con=conn)

def execute_sql(conn: sqlite3.Connection, sql_file: str) -> None:
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param sql_file: one or more sql commands
    :return:
    """

    sqlCommands = sql_file.split(';')

    for command in sqlCommands:
        try:
            cursor = conn.execute(command)
            conn.commit()
            print(cursor.fetchall())
        except OperationalError as msg:
            print("Command skipped: ", msg)
    
    return None

def pandas_sql(conn, query):
    df = pd.read_sql(query, conn)
    print(df.head())


def get_sql(file_path: str) -> str:
    """retrieve the SQL commands from a text file
    @param: file_path - the path to the text file
    @return: str - a string containing the contents of the file"""
    fd = open(file_path, 'r')
    sql = fd.read()
    fd.close()
    return sql


def validate_db(conn: sqlite3.Connection) -> None:
    """validate that the databse was set up correctly
    @param conn: a connection to the database"""
    try:
        result = conn.execute("select * from customers;")
        rows = result.fetchall()
        if len(rows) != 10:
            print('data did not load')
        else:
            print('setup complete')
    except Error as e:
        print(e)


def main() -> None:
    file_path = 'spotify-2023.csv'
    db_path = 'database'
    conn = create_connection(db_path)
    #load_table(conn, file_path)
    #query = get_sql('query.sql')
    #execute_sql(conn, query)
    query = '''
    SELECT MIN(released_year)
    FROM spotify;'''
    pandas_sql(conn, query)



if __name__ == "__main__":
    main()