"""
This module handles writing timestamps to Postgres. The "table" is the table to write within the database. All tables must have a primary key named "key".
All values are passed as a dictionary where the key is the name of the column in the database.
This only handles string values in the columns as of right now. It can be changed to to check the types of the values and then insert the appropriate placeholder in the query.
"""
from cessoc.postgresql import Postgresql
import psycopg2.extras


class timestampsDB:
    def __init__(self, user: str, password: str = None, getRDSToken: bool = False):
        self.db_conn = Postgresql(database="data_store", user=user, password=password, getRDSToken=getRDSToken)
    
    def get(self, table: str, key: str):
        with self.db_conn.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            sql = "select * from " + psycopg2.extras.quote_ident(table, curs) + " where key = %s ;"
            curs.execute(sql, (key,))
            row = curs.fetchone()
            return row
        
    def put(self, table: str, key: str, values: dict):
        with self.db_conn.connection.cursor() as curs:
            sql = "delete from " + psycopg2.extras.quote_ident(table, curs) + " where key = %s; "
            curs.execute(sql, (key,))
            sql_values = [key]

            sql = "insert into " + psycopg2.extras.quote_ident(table, curs) + " ( " + psycopg2.extras.quote_ident("key", curs) + ","
            for value_key in values.keys():
                sql += psycopg2.extras.quote_ident(value_key, curs) + ","
                sql_values.append(values[value_key])
            sql = sql[:-1] # Trim the trailing comma
            sql += ") VALUES (%s,"
            for i in range(len(values)):
                sql += "%s,"
            sql = sql[:-1] # Trim trailing comma
            sql += ");"
            curs.execute(sql, sql_values)
            self.db_conn.connection.commit()

    def update(self, table: str, key: str, values: dict):
        with self.db_conn.connection.cursor() as curs:
            sql_values = []
            sql = "update " + psycopg2.extras.quote_ident(table, curs) + " set "
            for value_key in values.keys():
                sql_values.append(values[value_key])
                sql += psycopg2.extras.quote_ident(value_key, curs) + " = %s,"
            sql = sql[:-1] # Trim extra comma
            sql += " where key = %s ;"
            sql_values.append(key)
            curs.execute(sql, sql_values)
            self.db_conn.connection.commit()
    
    def close(self):
        self.db_conn.connection.close()
        self.db_conn = None
