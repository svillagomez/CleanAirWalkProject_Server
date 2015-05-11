__author__ = 'santiago'
import math

from DBConnection import *
import time
from Constants import EARTH_RADIUS

# connection = Connection()
# postgres_cursor, postgres_conn = connection.connect_to_postgres_db()
# slq_cursor, sql_conn = connection.connect_to_pollution_db()
#
# # postgres_cursor.execute("SELECT y1,x1,y2,x2,gid,length from ways where length > 1")
# # data = postgres_cursor.fetchall()
# #
# # for row in data:
# #     print("%f,%f"%(row[0],row[1]))
# #     print("%f,%f"%(row[2],row[3]))
#
#
#
#
# def expand_box(lat1, long1, lat2, long2,distance):
#
#     degrees_to_radians = math.pi/180.0
#
#     if(lat1>lat2):
#         new_up = lat1 + (distance/EARTH_RADIUS)*(180/math.pi)
#         new_down = lat2 - (distance/EARTH_RADIUS)*(180/math.pi)
#     else:
#         new_up = lat2 + (distance/EARTH_RADIUS)*(180/math.pi)
#         new_down = lat1 - (distance/EARTH_RADIUS)*(180/math.pi)
#
#     if(long1 > long2):
#         new_right = long1 + (distance/EARTH_RADIUS)*\
#                         (180/math.pi)/math.cos(lat1*math.pi/180)
#         new_left = long2 - (distance/EARTH_RADIUS)*\
#                         (180/math.pi)/math.cos(lat2*math.pi/180)
#     else:
#         new_right = long2 + (distance/EARTH_RADIUS)*\
#                         (180/math.pi)/math.cos(lat2*math.pi/180)
#         new_left = long1 - (distance/EARTH_RADIUS)*\
#                         (180/math.pi)/math.cos(lat1*math.pi/180)
#
#     # print("%s,%s,%s,%s"%(new_left,new_right,new_up,new_down))
#
#     return (new_left,new_right,new_up,new_down)
#
#
#
# start = [-37.8, 144.8]
# start_lat =start[0]
# start_lon = start[1]
#
# lon_left,lon_right,lat_up,lat_down = expand_box(start_lat,start_lon,start_lat,start_lon,0.25)
# print(lat_down,lon_left,lat_up,lon_right)
#
#
# print ("LOOPING")
# st_time = time.time()
# static_point = "SELECT ST_SetSRID(ST_MakePoint(%f, %f),4326)"%(start_lon,start_lat)
# postgres_cursor.execute(static_point)
# result = postgres_cursor.fetchall()
#
# print(result[0])
#
#
# for i in range(0,1):
#     query_nearest = "SELECT gid,ST_Distance(w.the_geom,ST_SetSRID(ST_MakePoint(%s, %s),4326)) as distance FROM ways as w WHERE w.y1> %s and w.y1 < %s and w.x1>%s and w.x1<%s ORDER BY distance ASC LIMIT 1"
#     # query_nearest = "SELECT gid,ST_Distance(w.the_geom,%s) as distance FROM ways as w WHERE w.x1>%s and w.x1<%s ORDER BY distance ASC LIMIT 1"
#
#     postgres_cursor.execute(query_nearest,(start_lon,start_lat,lat_down,lat_up,lon_left,lon_right))
#     # postgres_cursor.execute(query_nearest,(result[0],lon_left,lon_right))
#     data = postgres_cursor.fetchone()
#
#     gid = data[0]
#     print(i)
#     print(time.time()-st_time)
# end_time = time.time()
#
# print("Total=%f"%(end_time-st_time))
#
#
# query_segment = "SELECT y1,x1,y2,x2 FROM ways WHERE gid = %s"
# postgres_cursor.execute(query_segment,(gid,))
# ans = postgres_cursor.fetchall()
# #
# lon_left,lon_right,lat_up,lat_down = expand_box(start_lat,start_lon,start_lat,start_lon,0.25)
# print(lat_down,lon_left,lat_up,lon_right)
#
#
#
# print(ans)


# query_nearest = "SELECT gid,ST_Distance(w.the_geom,ST_SetSRID(ST_MakePoint(%s, %s),4326)) as distance " \
#                 "FROM ways as w ORDER BY distance ASC LIMIT 1"
# self.postgres_cursor.execute(query_nearest,(lon,lat))
# data = self.postgres_cursor.fetchone()
#
# edge_id = data[0]
# NEW.gid = 'SELECT id::integer FROM ways_vertices_pgr ORDER BY the_geom <-> ST_SetSRID(ST_Point(NEW.longitude, NEW.latitude),4326) LIMIT 1';

# query_test = "SELECT gid,ST_Distance(w.the_geom,ST_SetSRID(ST_MakePoint(%s, %s),4326)) as distance INTO local_gid, local_distance  FROM ways as w ORDER BY 2 ASC LIMIT 1"
# todos = "(161377, 161379, 161380, 161386, 161396, 409337, 409362, 409439, 409440, 409463, 490137, 514141, 514159, 514160, 514161, 575798, 578309, 578318, 619924, 647078, 647149, 647153)"
#
# qqq = "SELECT y1,x1,y2,x2 FROM ways where gid in %s"%todos
#
# postgres_cursor.execute(qqq)
# lll = postgres_cursor.fetchall()
#
# for rr in lll:
#     print("%f,%f"%(rr[0],rr[1]))
#     print("%f,%f"%(rr[2],rr[3]))
import urlparse
path = "/q?start=-37.7999168567061&start=144.93830289691687&end=-37.80298775026495&end=144.94625229388475"

# /q?start=-37.79611250165&start144.9613407254219&end=-37.78720726649882&end=144.9812987074256

if '?' in path:
    path,tmp = path.split('?',1)
    qs = urlparse.parse_qs(tmp)
    print(tmp)

print(path,qs)

start_str = qs['start']
end_str = qs['end']

start = [float(i) for i in start_str]
end = [float(i) for i in end_str]

print(type(start))
print(type(end))

verch = float(start[0])
print(type(float(start[0])))
print(type(verch))
print(verch)

print(start)
print(end)