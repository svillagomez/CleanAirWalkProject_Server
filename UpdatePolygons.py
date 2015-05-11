__author__ = 'santiago'
import math
import psycopg2
import time

# in KM
EARTH_RADIUS = 6371
RECTANGLE_HALF_WIDTH = (20/float(1000))

end = [-37.801078, 144.953108]
start = [-37.800311, 144.954197]

def init_postgresDB_connection():
    try:
        conn = psycopg2.connect("dbname='pgrouting_melbourne' user='santiago' host='localhost' password='santy312postgres'")
        print("Connection established")
    except:
        print ("I am unable to connect to the database")

    cursor = conn.cursor()

    return cursor,conn

def close_connection(cursor,connection):
    cursor.close()
    connection.close()
    print("Connection closed")


def create_geom_polygon_column(cursor):
    """This functions does not affect the db and I do not why"""
    tableName = "ways"
    columnName = "geom_polygon"

    query_drop_column = "ALTER TABLE %s DROP COLUMN IF EXISTS %s"%(tableName,columnName)

    cursor.execute(query_drop_column)

    # try:
    #     cursor.execute(query_drop_column)
    # except:
    #     print("Error dropping %s column"%(columnName))

    # ALTER TABLE roads ADD COLUMN geom2 geometry(LINESTRINGZ,4326);
    query_add_column = "ALTER TABLE %s ADD COLUMN %s geometry(POLYGON,4326)"%(tableName,columnName)
    # print(query_add_column)
    cursor.execute(query_add_column)
    # try:
    #     cursor.execute(query_add_column)
    #     print("se anadio la columna")
    # except:
    #     print("Error creating column. Perhaps it exits already")



"""update polygon in postgres to enclose a rectangular (rotated area)"""
def update_bounding_polygon(start_loc,end_loc,cursor,connection):
    """Query all nodes from ways"""
    tableName = "ways"
    columnName = "geom_polygon"

    all_ids_query = "SELECT gid from %s"%(tableName)
    cursor.execute(all_ids_query)
    edge_id_list = cursor.fetchall()
    # print(edge_id_list)

    # print(len(edge_id_list))

    """QUERY lat, lon of every edge"""
    base_query_lat_lon = "SELECT y1,x1,y2,x2 from %s WHERE gid = %s"
    # cursor.execute(base_query_lat_lon)

    # print(cursor.fetchall())

    """Create and update geom_poly column"""
    update_polygon = "UPDATE %s SET %s = " \
                         "ST_Polygon(ST_GeomFromText('LINESTRING(%f %f,%f %f,%f %f,%f %f,%f %f)'),4326) WHERE gid = %d"


    counter = 0;

    for gid in edge_id_list:
        cursor.execute(base_query_lat_lon%(tableName,gid[0]))
        coordinates = cursor.fetchone()

        start_lat = coordinates[0]
        start_lon = coordinates[1]
        end_lat = coordinates[2]
        end_lon = coordinates[3]

        v1,v2,v3,v4 = find_four_vertices_given_start_end_of_edge(start_lat,start_lon,end_lat,end_lon)

        que = update_polygon%(tableName,columnName,v1[0],v1[1],v2[0],v2[1],v3[0],v3[1],v4[0],v4[1],v1[0],v1[1],gid[0])

        cursor.execute(que)

        counter += 1

        if counter % 1000 == 0:
            print(counter)
        # print(cursor.fetchone())
        # print(que)

        connection.commit()



    # current_id = edge_id[0]
    #
    # print(current_id)
    """ Create a polygon geometry. actually a rotated rectangle and update the postgres DB"""


    ver_1,ver_2,ver_3,ver_4 = find_four_vertices_given_start_end_of_edge(start_loc[0],start_loc[1],end_loc[0],end_loc[1])

    # print(ver_1)
    # print(ver_2)
    # print(ver_3)
    # print(ver_4)

    # query_edges_gid = "SELECT gid from ways LIMIT 10"


    # base_query = "UPDATE ways SET geom_poly = ST_MakePolygon(ST_GeomFromText('LINESTRING(75.15 29.53,77 29,77.6 29.5, 75.15 29.53)')) where gid in (37,3407,7888,8870,9509)
    # cursor.execute("SELECT version()")
    # print(cursor.fetchall())


def mid_point_given_latitude(lat_1,lat_2):
    dif_degrees = (lat_1-lat_2)/2
    diff_km = dif_degrees*EARTH_RADIUS/(180/math.pi)
    # print("dif lat",diff_km)
    mid_point = lat_1 - dif_degrees
    return  diff_km,mid_point


def mid_point_given_longitude(lon_1,lon_2,lat_ref):
    diff_degrees = (lon_1-lon_2)/2
    diff_km = (diff_degrees*EARTH_RADIUS)/(180/math.pi)*math.cos(lat_ref*math.pi/180)
    # print("dif_lon",diff_km)
    # (distance/EARTH_RADIUS)*(180/math.pi)/math.cos(lat1*math.pi/180)
    mid_point = lon_1 - diff_degrees
    return  diff_km,mid_point


def find_four_vertices_given_start_end_of_edge(lat_1, lon_1, lat_2, lon_2):
    diff_lat = mid_point_given_latitude(lat_1,lat_2)[0]
    diff_lon = mid_point_given_longitude(lon_1,lon_2,lat_2)[0]
    tan_angle = 100000

    if diff_lon>0:
        tan_angle = (diff_lat)/(diff_lon)
        abs_tang_angle = math.fabs(tan_angle)
        angle = math.atan(abs_tang_angle)
    else:
        angle = math.pi/2

    # http://stackoverflow.com/questions/1253499/simple-calculations-for-working-with-lat-lon-km-distance
    add_lon_km = RECTANGLE_HALF_WIDTH*math.sin(angle)
    add_lat_km = RECTANGLE_HALF_WIDTH*math.cos(angle)
    add_lat_degrees = (add_lat_km/110.54)
    add_lon_degrees = (add_lon_km/(111.320*math.cos(lat_2*math.pi/180)))

# """FALTA VALIDACION PARA LA SUMAS DE LONGITUD PARA EVITAR MALOS RECTANGULOS"""
    if tan_angle >=0:
        vertex_1 = [lat_1+add_lat_degrees,lon_1-add_lon_degrees]
        vertex_2 = [lat_1-add_lat_degrees,lon_1+add_lon_degrees]
        vertex_3 = [lat_2-add_lat_degrees,lon_2+add_lon_degrees]
        vertex_4 = [lat_2+add_lat_degrees,lon_2-add_lon_degrees]
    else:
        vertex_1 = [lat_1+add_lat_degrees,lon_1+add_lon_degrees]
        vertex_2 = [lat_1-add_lat_degrees,lon_1-add_lon_degrees]
        vertex_3 = [lat_2-add_lat_degrees,lon_2-add_lon_degrees]
        vertex_4 = [lat_2+add_lat_degrees,lon_2+add_lon_degrees]

    return (vertex_1,vertex_2,vertex_3,vertex_4)



start_time = time.time()


cursor, connection = init_postgresDB_connection()

create_geom_polygon_column(cursor)

# run only to create all geom_polygon values ( it takes a lot of time!!!!!!)
# update_bounding_polygon(start,end,cursor,connection)

close_connection(cursor,connection)


end_time = time.time()

print("Running time=%3.6f seconds"%(end_time-start_time))

# update_bounding_polygon(start,end)