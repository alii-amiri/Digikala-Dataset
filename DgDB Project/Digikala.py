import xlrd
import json
import re

import mysql.connector as sql



# connects to sql and creates a database
def start_db(host, user, passwd, db_name):
    conn = sql.connect(
        host="localhost",
        user="admin",
        passwd="obsidian9513406",
    )
    cur = conn.cursor()
    cur.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    cur.execute(f'USE {db_name}')
    return conn, cur


# shuts down the connection
def shutdown_db(conn, cursor):
    cur.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    conn, cur = start_db('localhost', 'admin', 'obsidian9513406', 'DG_Project')


    shutdown_db(conn, cur)