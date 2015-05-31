__author__ = 'santiago'

import MySQLdb
import psycopg2


class Connection(object):

    @staticmethod
    def connect_to_pollution_db():
        sql_connection = MySQLdb.connect("localhost", "root", "", "aircasting_development")
        sql_cursor = sql_connection.cursor()
        return sql_cursor, sql_connection

    @staticmethod
    def connect_to_postgres_db():
        try:
            postgres_connection = psycopg2.connect("dbname='pgrouting_melbourne' "
                                                 "user='santiago' "
                                                 "host='localhost' "
                                                 "password='santy312postgres'")
        except:
            print ("I am unable to connect to the database")

        postgres_cursor = postgres_connection.cursor()
        return postgres_cursor, postgres_connection