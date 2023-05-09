import psycopg2 as pg

print("postgresql imported")


class postgresql:
    def __init__(self, host, database, user, password, port=5432):
        self.connection = pg.connect(
            host=host, database=database, user=user, password=password, port=port)

    def create(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
        
    def read(self, query) -> list:
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def update(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()

    def delete(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
        