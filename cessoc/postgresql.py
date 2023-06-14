"""
The postgresql provides standard postgresql functionality for cessoc services.
"""
from typing import Optional
import psycopg2 as pg  # psycopg2-binary
from cessoc.aws import ssm


class Postgresql:
    def __init__(self, database, user, password, host: Optional[str] = ssm.get_value('/ces/data_store/rds_host'), port: Optional[int] = 5432):
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
