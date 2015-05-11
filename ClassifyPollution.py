__author__ = 'santiago'
# from DBConnection import Connection

from MainProgram import Utils

class ClassifyPollution(object):
    def __init__(self, Connection):
        self.postgres_cursor, self.postgres_conn = Connection.connect_to_postgres_db()

    def set_gid_of_measurement_table(postgres_cursor):

        print("hello")

    def classify_measurement_data(self):
        # connection = Connection()
        # postgres_cursor, postgres_conn = connection.connect_to_postgres_db()
        # sql_cursor, sql_conn = connection.connect_to_pollution_db()
        print("hello")