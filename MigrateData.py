__author__ = 'santiago'

from DBConnection import *
import Constants as CONST
import time
# from MainProgram import Utils

# function: get_lastest_pollution_id
# description: this function get the latest Id in our own db corresponding to the Id in the AirCasting DB
# return: latest_id => the searched Id
# parameters: postgres_cursor => cursor to operate in the postgresql DB
def get_lastest_pollution_id(postgres_cursor):
    # query the latest id since ID are unique
    query = "SELECT max(id_orig) FROM measurements"
    postgres_cursor.execute(query)
    latest_id = postgres_cursor.fetchone()
    return latest_id


# function: get_stream_ids_of_pollution
# description: this function get all the stream Ids corresponding to PM ( particle matter)
# return: tuples_session_id => the stream Ids searched for
# parameters: sql_cursor => cursor to operate in the postgresql DB
#               CONST => constant parameters (to know the particula matter)
def get_stream_ids_of_pollution(sql_cursor,CONST):
    # Query all id in streams that ONLY belong to pollution
    querySQL_session_id = "SELECT id FROM streams WHERE measurement_short_type = %s"

    sql_cursor.execute(querySQL_session_id,CONST.POLLUTION_TYPE)
    data = sql_cursor.fetchall()

    session_ids=[]
    for row in data:
        session_ids.append(row[0])

    tuples_session_id = tuple(session_ids)
    return tuples_session_id


# function: get_data_since_last_update
# description: migrate data (update) from the last Id inserted
# return: data => all records from SQL (AirCasting DB) not present in our own DB
# parameters: lastest_update => the last updated Id in our DB
#             stream_ids => The stream Ids corresponding to particulate matter sessions
#              sql_cursor => cursor to operate in the mysql DB
def get_data_since_last_update(lastest_update,stream_ids,sql_cursor):
    if lastest_update[0]== None:
        sql_query_lat_lon_value = "SELECT id,latitude, longitude, time,stream_id, measured_value " \
                                  "FROM measurements WHERE stream_id IN %s"
    else:
        query_from_last_known_id = "SELECT id,latitude,longitude,time,stream_id,measured_value " \
                                   "FROM measurements WHERE id > %d"%(lastest_update[0])
        sql_query_lat_lon_value = query_from_last_known_id+" AND stream_id IN %s"

    sql_cursor.execute(sql_query_lat_lon_value,[stream_ids])

    data = sql_cursor.fetchall()

    return data

# function: insert_data_into_measurements_table
# description: copy data (only needed fields) into our own DB
# return: None
# parameters: data => all records coming form AirCasting DB
#             postgres_cursor => cursor to operate in the postgresql DB
#              postgres_conn => cursor to operate in the postgresql DB (to perform commits)
def insert_data_into_measurements_table(data,postgres_cursor,postgres_conn):
    query_move_to_postgres = "INSERT INTO measurements(id_orig,latitude,longitude,time,stream_id,measured_value) " \
                             "VALUES(%s,%s,%s,%s,%s,%s)"
    postgres_cursor.executemany(query_move_to_postgres,data)
    postgres_conn.commit()

# function: update
# description: runs the logic to update our own DB
# return: None
# parameters: None
def update():
    start_time = time.time()
    connection = Connection()
    postgres_cursor, postgres_conn = connection.connect_to_postgres_db()
    sql_cursor, sql_conn = connection.connect_to_pollution_db()

    lastest_updated_id = get_lastest_pollution_id(postgres_cursor)

    stream_ids  = get_stream_ids_of_pollution(sql_cursor,CONST)

    data = get_data_since_last_update(lastest_updated_id,stream_ids,sql_cursor)

    insert_data_into_measurements_table(data,postgres_cursor,postgres_conn)

    # print("Updating time = %f sec"%(time.time()-start_time))

    # print("SUCCESFUL")


# Call of update used to do tests
update()