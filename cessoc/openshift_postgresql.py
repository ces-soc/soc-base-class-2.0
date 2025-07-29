"""
The postgresql provides standard postgresql functionality for cessoc services.
"""
from typing import Optional
import psycopg2 as pg  # psycopg2-binary
import os


class OpenshiftPostgresql:
    def __init__(self, database, user, password: Optional[str] = None, host: Optional[str] = None, port: Optional[int] = 5432, region: Optional[str] = "us-west-2"):
        if host is None:
            host = os.environ['/ces/data_store/rds_host']
        self.connection = pg.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )

    def query(self, query):
        """
        Sends a query to the postgresql database and returns the results.
        :param query: The query to be sent to the database
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        self.connection.commit()
        cursor.close()
        return results
