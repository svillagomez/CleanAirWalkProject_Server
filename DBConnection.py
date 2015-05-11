__author__ = 'santiago'

import MySQLdb
import psycopg2

class Connection(object):

    def connect_to_pollution_db(self):
        sql_connection = MySQLdb.connect("localhost","root","","aircasting_development")
        sql_cursor = sql_connection.cursor()
        return (sql_cursor,sql_connection)

    def connect_to_postgres_db(self):
        try:
            psgres_connection = psycopg2.connect("dbname='pgrouting_melbourne' "
                                                 "user='santiago' "
                                                 "host='localhost' "
                                                 "password='santy312postgres'")
        except:
            print ("I am unable to connect to the database")

        psgres_cursor = psgres_connection.cursor()
        return (psgres_cursor,psgres_connection)