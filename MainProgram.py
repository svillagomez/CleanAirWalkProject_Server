__author__ = 'santiago'

import time
import Distance
import Constants as CONST
from DBConnection import *
from itertools import groupby
from operator import itemgetter
from collections import OrderedDict

class Utils(object):
    def __init__(self, connection):
        self.postgres_cursor, self.postgres_conn = connection.connect_to_postgres_db()


    def find_nearest_vertex_id(self,lat,lon):
        query_start = "SELECT id::integer FROM ways_vertices_pgr " \
                      "ORDER BY the_geom <-> ST_GeometryFromText('POINT(%s %s)') LIMIT 1"
        self.postgres_cursor.execute(query_start,(lon,lat))
        ans = self.postgres_cursor.fetchall()

        for row in ans:
            edge_id =row[0]

        return edge_id


""" QUERY TO create an aid bounding box"""
def create_bigger_bounding_box_with_astar(source,target,CONST,psgres_cursor):
    saved_partial_result = "partial_result"
    coordinates = [0.0]*4


    query_route_astar = \
    "SELECT DISTINCT id2 AS edge into %s " \
    "FROM pgr_astar('SELECT gid AS id,source::integer,target::integer,length::double precision AS cost,x1, y1, x2, y2 " \
    "FROM ways',%d,%d, false, false)"\
    %(saved_partial_result,source,target)

    psgres_cursor.execute(query_route_astar)

    lat_long_query = "SELECT max(x1),max(x2), min(x1),min(x2),max(y1),max(y2),min(y1),min(y2) FROM ways WHERE gid in " \
                     "((SELECT * FROM %s))"%(saved_partial_result)
    psgres_cursor.execute(lat_long_query)
    aux = psgres_cursor.fetchall()

    for each in aux:
        coordinates[CONST.LONG_MAX_INDEX] = max(each[0],each[1])
        coordinates[CONST.LONG_MIN_INDEX] = min(each[2],each[3])
        coordinates[CONST.LAT_MAX_INDEX] = max(each[4],each[5])
        coordinates[CONST.LAT_MIN_INDEX] = min(each[6],each[7])

    #------ EXPAND THE BOUNDING BOX
    coordinates[CONST.LONG_MIN_INDEX], coordinates[CONST.LONG_MAX_INDEX],\
    coordinates[CONST.LAT_MAX_INDEX],coordinates[CONST.LAT_MIN_INDEX] =\
    Distance.expand_box(coordinates[CONST.LAT_MIN_INDEX],coordinates[CONST.LONG_MIN_INDEX],
                     coordinates[CONST.LAT_MAX_INDEX],coordinates[CONST.LONG_MAX_INDEX])
    #---------------------------------------------------------------------------------------

    query_drop = "DROP TABLE IF EXISTS partial_result"
    psgres_cursor.execute(query_drop)

    return coordinates

def calculate_bigger_bounding_box_given_route_gids(edges_tuples,CONST,psgres_cursor):

    coordinates = [0.0]*4

    lat_long_query = "SELECT max(x1),max(x2), min(x1),min(x2),max(y1),max(y2),min(y1),min(y2) FROM ways WHERE gid in %s"
    psgres_cursor.execute(lat_long_query,[edges_tuples])
    aux = psgres_cursor.fetchall()

    for each in aux:
        coordinates[CONST.LONG_MAX_INDEX] = max(each[0],each[1])
        coordinates[CONST.LONG_MIN_INDEX] = min(each[2],each[3])
        coordinates[CONST.LAT_MAX_INDEX] = max(each[4],each[5])
        coordinates[CONST.LAT_MIN_INDEX] = min(each[6],each[7])

    #-------------------------- EXPAND THE BOUNDING BOX-------------------------------------
    coordinates[CONST.LONG_MIN_INDEX], coordinates[CONST.LONG_MAX_INDEX],\
    coordinates[CONST.LAT_MAX_INDEX],coordinates[CONST.LAT_MIN_INDEX] =\
    Distance.expand_box(coordinates[CONST.LAT_MIN_INDEX],coordinates[CONST.LONG_MIN_INDEX],
                     coordinates[CONST.LAT_MAX_INDEX],coordinates[CONST.LONG_MAX_INDEX])
    # --------------------------------------------------------------------------------------

    return coordinates

"""QUERY ALL NODES WITHIN A BOUNDING BOX"""
def get_edges_id_inside_bounding_box(coordinates,CONST,psgres_cursor):

    query_nodes_inside_bounding = "SELECT gid,source,target,length,to_cost,x1,y1,x2,y2,the_geom INTO bounding_box_table " \
                                  "FROM ways where the_geom && ST_MakeEnvelope(%f,%f,%f,%f,4326)" \
                                  %(coordinates[CONST.LONG_MIN_INDEX],coordinates[CONST.LAT_MIN_INDEX],
                                    coordinates[CONST.LONG_MAX_INDEX],coordinates[CONST.LAT_MAX_INDEX])

    psgres_cursor.execute(query_nodes_inside_bounding)

    query_dos = "SELECT gid FROM bounding_box_table"
    psgres_cursor.execute(query_dos)

    edges = psgres_cursor.fetchall()

    edge_ids=[]

    for row in edges:
        edge_ids.append(row[0])

    tuples_edge_ids = tuple(edge_ids)

    return tuples_edge_ids


def diff_y1_x1(p1,p2):
    ans = pow(((pow((p1[0] - p2[0]),2))+(pow((p1[1] - p2[1]),2))),0.5)
    return ans

def calc_ends_diffs(t1,t2):

    dict_ans = {}
    counter = 0
    for i in range(0,3,2):
        for j in range(0,3,2):
            p1 = []
            p1.append(t1[i])
            p1.append(t1[i+1])
            p2 = []
            p2.append(t2[j])
            p2.append(t2[j+1])
            dict_ans[counter]=diff_y1_x1(p1,p2)
            counter+=1
    return dict_ans

def invert_ends(t1):
    # make a copy because t1 might have more tha 4 elements inside
    partial = t1
    partial[0]=(t1[2])
    partial[1]=(t1[3])
    partial[2]=(t1[0])
    partial[3]=(t1[1])
    return partial

def correct_pair(first_pair,t1,t2):
    dict_values = calc_ends_diffs(t1,t2)
    which = min(dict_values, key=dict_values.get)
    if first_pair == True:
        partial = []
        if which == 0 or which == 1:
            # invert values
            t1 = invert_ends(t1)
        elif which == 3:
            t2 = invert_ends(t2)
    else:
        #----- Assume the first point is in the correct order
        if which == 3:
            t2 = invert_ends(t2)
    return t1,t2

def display_lat_lon_given_edges_gid(tuples_edge_ids,order,psgres_cursor):

    if order == True:
        query = "SELECT DISTINCT y1,x1,y2,x2,gid FROM ways WHERE gid in %s ORDER BY y1,x1"
        psgres_cursor.execute(query,[tuples_edge_ids])
    else:
        query = "SELECT DISTINCT y1,x1,y2,x2,gid FROM ways WHERE gid in %s"
        psgres_cursor.execute(query,[tuples_edge_ids])

    data = psgres_cursor.fetchall()

    dict_lat_lon_id = {}
    # keep the same order as the imput gid_tuples
    for item in data:
        dict_lat_lon_id[item[4]]=[item[0],item[1],item[2],item[3]]

    # for each in tuples_edge_ids:
    #     print("(%f,%f),"%(dict_lat_lon_id[int(each)][0],dict_lat_lon_id[int(each)][1]))
    #     print("(%f,%f),"%(dict_lat_lon_id[int(each)][2],dict_lat_lon_id[int(each)][3]))

    for each in tuples_edge_ids:
        print("%f,%f"%(dict_lat_lon_id[int(each)][0],dict_lat_lon_id[int(each)][1]))
        print("%f,%f"%(dict_lat_lon_id[int(each)][2],dict_lat_lon_id[int(each)][3]))


def get_lat_lon_given_id(tuples_edge_ids,psgres_cursor):
    # if do_order == True:
    query = "SELECT y1,x1,y2,x2,gid FROM ways WHERE gid in %s"
    psgres_cursor.execute(query,[tuples_edge_ids])
    data = psgres_cursor.fetchall()

    dict_lat_lon_id ={}
    # keep the same order as the imput gid_tuples
    for item in data:
        dict_lat_lon_id[item[4]]=[item[0],item[1],item[2],item[3]]

    # keep the same order
    startAndEnd_coords_dict_gid = OrderedDict()

    # route_coord = []
    for each in tuples_edge_ids:
        startAndEnd_coords_dict_gid[each]=dict_lat_lon_id[int(each)]

    ret_val = []
    edges = []
    counter = 0
    for item in startAndEnd_coords_dict_gid:
        if counter < 2:
            edges.append(startAndEnd_coords_dict_gid[item])
        elif counter == 2:
            first_edge = edges[0]
            second_edge = edges[1]
            first_edge,second_edge = correct_pair(True,first_edge,second_edge)
            edge = second_edge
            ret_val.append(first_edge)
            ret_val.append(second_edge)
        elif counter >2:
            next_edge = startAndEnd_coords_dict_gid[item]
            edge,next_edge = correct_pair(False,edge,next_edge)
            ret_val.append(next_edge)
            edge = next_edge
        counter += 1
    return ret_val


def get_lat_lon_pollution_given_id(tuples_edge_ids,gid_with_pollutionAverage_tuples,psgres_cursor):
    # if do_order == True:
    query = "SELECT y1,x1,y2,x2,gid FROM ways WHERE gid in %s"
    psgres_cursor.execute(query,[tuples_edge_ids])
    data = psgres_cursor.fetchall()

    gid_with_pollutionAverage_dict = dict(gid_with_pollutionAverage_tuples)

    dict_lat_lon_id ={}

    # keep the same order as the iput gid_tuples
    for item in data:
        # the gid must be the key for the dictionary. is located at position 4
        key = item[4]
        dict_lat_lon_id[key]=[item[0],item[1],item[2],item[3],gid_with_pollutionAverage_dict[key]]

    # keep the same order
    startAndEnd_coords_dict_gid = OrderedDict()
    for each in tuples_edge_ids:
        startAndEnd_coords_dict_gid[each]=dict_lat_lon_id[int(each)]

    ret_val = []
    edges = []
    counter = 0
    for item in startAndEnd_coords_dict_gid:
        if counter < 2:
            edges.append(startAndEnd_coords_dict_gid[item])
        elif counter == 2:
            first_edge = edges[0]
            second_edge = edges[1]
            first_edge,second_edge = correct_pair(True,first_edge,second_edge)
            edge = second_edge
            ret_val.append(first_edge)
            ret_val.append(second_edge)
        elif counter >2:
            next_edge = startAndEnd_coords_dict_gid[item]
            edge,next_edge = correct_pair(False,edge,next_edge)
            ret_val.append(next_edge)
            edge = next_edge
        counter += 1
    return ret_val

def get_lat_lon_pollution_given_id(tuples_edge_ids,gid_with_pollutionAverage_tuples,psgres_cursor):
    query = "SELECT y1,x1,y2,x2,gid FROM ways WHERE gid in %s"
    psgres_cursor.execute(query,[tuples_edge_ids])
    data = psgres_cursor.fetchall()

    gid_with_pollutionAverage_dict = dict(gid_with_pollutionAverage_tuples)

    dict_lat_lon_id ={}
    # keep the same order as the iput gid_tuples
    for item in data:
        # the gid must be the key for the dictionary. is located at position 4
        key = item[4]
        dict_lat_lon_id[key]=[item[0],item[1],item[2],item[3],gid_with_pollutionAverage_dict[key]]

    # keep the same order
    startAndEnd_coords_dict_gid = OrderedDict()
    for each in tuples_edge_ids:
        startAndEnd_coords_dict_gid[each]=dict_lat_lon_id[int(each)]
        # print(dict_lat_lon_id[int(each)])

    ret_val = []
    edges = []
    counter = 0
    for item in startAndEnd_coords_dict_gid:
        if counter < 2:
            edges.append(startAndEnd_coords_dict_gid[item])
        elif counter == 2:
            first_edge = edges[0]
            second_edge = edges[1]
            first_edge,second_edge = correct_pair(True,first_edge,second_edge)
            edge = second_edge
            ret_val.append(first_edge)
            ret_val.append(second_edge)
        elif counter >2:
            next_edge = startAndEnd_coords_dict_gid[item]
            edge,next_edge = correct_pair(False,edge,next_edge)
            ret_val.append(next_edge)
            edge = next_edge
        counter += 1
    return ret_val

def get_lat_lon_pollution_length_given_id(tuples_edge_ids,gid_with_pollutionAverage_tuples,psgres_cursor):
    query = "SELECT y1,x1,y2,x2,gid,length FROM ways WHERE gid in %s"
    psgres_cursor.execute(query,[tuples_edge_ids])
    data = psgres_cursor.fetchall()

    gid_with_pollutionAverage_dict = dict(gid_with_pollutionAverage_tuples)

    dict_lat_lon_id ={}
    # keep the same order as the iput gid_tuples
    for item in data:
        # the gid must be the key for the dictionary. is located at position 4
        key = item[4]
        dict_lat_lon_id[key]=[item[0],item[1],item[2],item[3],gid_with_pollutionAverage_dict[key],item[5]]

    # keep the same order
    startAndEnd_coords_dict_gid = OrderedDict()
    for each in tuples_edge_ids:
        startAndEnd_coords_dict_gid[each]=dict_lat_lon_id[int(each)]

    ret_val = []
    edges = []
    counter = 0
    for item in startAndEnd_coords_dict_gid:
        if counter < 2:
            edges.append(startAndEnd_coords_dict_gid[item])
        elif counter == 2:
            first_edge = edges[0]
            second_edge = edges[1]
            first_edge,second_edge = correct_pair(True,first_edge,second_edge)
            edge = second_edge
            ret_val.append(first_edge)
            ret_val.append(second_edge)
        elif counter >2:
            next_edge = startAndEnd_coords_dict_gid[item]
            edge,next_edge = correct_pair(False,edge,next_edge)
            ret_val.append(next_edge)
            edge = next_edge
        counter += 1
    return ret_val


def construct_buffer_following_linestring(tuples_edge_ids,seq_and_geom_dict,psgres_cursor):
    seq_and_geom_dict.pop(len(seq_and_geom_dict)-1)

    query = "SELECT gid,source,target,length,to_cost,y1,x1,y2,x2 FROM ways WHERE gid IN %s"
    psgres_cursor.execute(query,[tuples_edge_ids])
    answer = psgres_cursor.fetchall()

    data= []

    dict_aid = {}
    for item in answer:
        dict_aid[item[0]] = item

    for key in seq_and_geom_dict:
        data.append(tuple(dict_aid[seq_and_geom_dict[key]]))

    total = len(data)

    Y1_INDEX = 5
    X1_INDEX = 6
    Y2_INDEX = 7
    X2_INDEX = 8
    res = ""
    aux_counter = 0

    if total>100:
        divisor = 5
    elif total>50:
        divisor = 3
    else:
        divisor = 2;

    for item in data:
        if (aux_counter % (total/divisor)) == 0:
            res += str(item[X1_INDEX])+" "+str(item[Y1_INDEX])+","
        aux_counter += 1

    # always include the last element just in case
    res += str(item[X1_INDEX])+" "+str(item[Y1_INDEX])+","
    res += str(item[X2_INDEX])+" "+str(item[Y2_INDEX])+","

    res = res[:-1]

    query_nodes_inside_bounding = "SELECT gid, ST_Contains(ST_Buffer(ST_GeomFromText('LINESTRING(" + res +\
                                  ")',4326),0.008,'endcap=round join=round'),the_geom::geometry) FROM bounding_box_table"

    psgres_cursor.execute(query_nodes_inside_bounding)
    edges = psgres_cursor.fetchall()

    edge_ids=[]

    tuples_edge_ids_str_list = list(tuples_edge_ids)

    # http://stackoverflow.com/questions/7368789/convert-all-strings-in-a-list-to-int
    tuples_edge_ids_int_list = map(int,tuples_edge_ids_str_list)

    delete_this_counter = 0
    for row in edges:
        if row[0] in tuples_edge_ids_int_list:
            edge_ids.append(row[0])
        elif row[1]:
            edge_ids.append(row[0])
        delete_this_counter +=1

    tuples_edge_ids = tuple(edge_ids)

    query = "SELECT gid,source,target,length,to_cost,y1,x1,y2,x2 INTO buffer_geometry_table FROM ways WHERE gid IN %s ORDER BY y1"
    psgres_cursor.execute(query,[tuples_edge_ids])

    return tuples_edge_ids

"""QUERY ALL NODES WITHIN A BOUNDING BOX"""
def get_route_edges_given_start_end(source,target,psgres_cursor):
    query_route_astar = "SELECT DISTINCT ON (seq) seq, id1 AS node, id2 AS edge, cost, b.the_geom FROM pgr_astar(" \
                        "'SELECT gid AS id,source::integer,target::integer,length::double precision AS cost," \
                        "x1, y1, x2, y2 FROM ways',%s,%s, false, false) a LEFT JOIN ways b ON (a.id2 = b.gid) ORDER BY seq"

    psgres_cursor.execute(query_route_astar,(source,target))

    rows = psgres_cursor.fetchall()

    edges = []

    seq_and_geom_dict = OrderedDict()

    REAL_GID_INDEX = 2
    # THE_GEOM_INDEX = 4
    SEQ_INDEX = 0
    for row in rows:
        edges.append(str(row[REAL_GID_INDEX]))
        seq_and_geom_dict[row[SEQ_INDEX]]=row[REAL_GID_INDEX]

    # get rid of last element
    edges.pop()

    edges_tuples = tuple(edges)

    return edges_tuples,seq_and_geom_dict

def get_route_edges_given_start_end_within_bounding_box_cost(source,target,psgres_cursor,coordinates):
    query_route_astar = "SELECT DISTINCT ON (seq) seq, id1 AS node, id2 AS edge, cost, b.the_geom FROM pgr_astar(" \
                        "'SELECT gid AS id,source::integer,target::integer,(length::double precision*to_cost::double precision) AS cost," \
                        "x1, y1, x2, y2 FROM ways WHERE y1>%s AND y1<%s AND x1>%s AND x1<%s',%s,%s, false, false) a LEFT JOIN ways b ON (a.id2 = b.gid)"
    psgres_cursor.execute(query_route_astar,(coordinates[CONST.LAT_MIN_INDEX],coordinates[CONST.LAT_MAX_INDEX],
                                            coordinates[CONST.LONG_MIN_INDEX],coordinates[CONST.LONG_MAX_INDEX],
                                            source,target))

    rows = psgres_cursor.fetchall()

    edges = []

    REAL_GID_INDEX = 2
    for row in rows:
        edges.append(str(row[REAL_GID_INDEX]))

    # get rid of last element
    edges.pop()

    edges_tuples = tuple(edges)

    return edges_tuples

def get_pollution_data_within_bounding_box(coordinates,sql_cursor,psgres_cursor,psgres_connection):

    st_time = time.time()

    query_delete_pollution_table = "DELETE FROM pollution"

    psgres_cursor.execute(query_delete_pollution_table)

    sql_query_lat_lon_value = "SELECT id,latitude, longitude, measured_value from measurements WHERE " \
                 "latitude>=%s and latitude<=%s and longitude >=%s and longitude <=%s "
    sql_cursor.execute(sql_query_lat_lon_value,(coordinates[CONST.LAT_MIN_INDEX],coordinates[CONST.LAT_MAX_INDEX],
                                                coordinates[CONST.LONG_MIN_INDEX],coordinates[CONST.LONG_MAX_INDEX]))

    data = sql_cursor.fetchall()

    # a trigger in the db COULD calculate the point geometries. NOW is not please check all triggers with
    # \dS <table_name>
    query_move_to_postgres = "INSERT INTO pollution(id,latitude,longitude,measured_value) VALUES(%s,%s,%s,%s)"
    psgres_cursor.executemany(query_move_to_postgres,data)

    psgres_connection.commit()

def update_ways_sample_data(edge_ids,posgres_cursor,posgres_connection):

    query_delete_pollution_table = "DELETE FROM ways_sample"

    posgres_cursor.execute(query_delete_pollution_table)

    query_get_all_given_gid = "SELECT gid,source,target,length,to_cost,x1,y1,x2,y2 FROM ways WHERE gid in %s"

    posgres_cursor.execute(query_get_all_given_gid,[edge_ids])
    data = posgres_cursor.fetchall()

    # a trigger in the db COULD calculate the point geometries. NOW is not please check all triggers with
    # \dS <table_name>
    query_insert_into_ways_sample = "INSERT INTO ways_sample(gid,source,target,length,to_cost,x1,y1,x2,y2) " \
                                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    posgres_cursor.executemany(query_insert_into_ways_sample,data)

    posgres_connection.commit()

def accumulate(tuples_list):
    it = groupby(tuples_list, str(itemgetter(0)))
    for key, subiter in it:
       yield key, sum(float(item[1]) for item in subiter)

def getKey(item):
    return item[0]

def sort_tuples(tuples):
    return sorted(tuples,key=getKey)
    # sorted_tuples = sorted(gidAndValue,key=getKey)
def average_pollution_of_given_ids(edge_ids,posgres_cursor):
    query_value = "SELECT gid, AVG(measured_value) as measured_value FROM measurements WHERE gid IN %s GROUP BY gid"
    posgres_cursor.execute(query_value,[edge_ids])

    gidAndValue = posgres_cursor.fetchall()

    gids_of_valued_adges = [ x[0] for x in gidAndValue ]
    all_gids = [x for x in edge_ids]
    gid_without_pollution = [item for item in all_gids if item not in gids_of_valued_adges]


    NO_POLLUTION = float(0.0)
    for item in gid_without_pollution:
        gidAndValue.append((item,NO_POLLUTION))

    return gidAndValue

def classify_measurements():
    # print("hello")
    pass
def update_cost_column_in_ways(dict_ret_val,posgres_cursor,postgres_connection):

    what = [(v,k) for k,v in dict_ret_val.iteritems()]

    query_route_astar = "SELECT DISTINCT ON (seq) seq, id1 AS node, id2 AS edge, cost, b.the_geom FROM pgr_astar(" \
                        "'SELECT gid AS id,source::integer,target::integer,length::double precision AS cost," \
                        "x1, y1, x2, y2 FROM ways',%s,%s, false, false) a LEFT JOIN ways b ON (a.id2 = b.gid)"

    query_set_cost = "UPDATE ways SET to_cost = %s WHERE gid = %s"
    posgres_cursor.executemany(query_set_cost,(what))
    postgres_connection.commit()

def get_max_pollution_value():
    db_connection = Connection()
    psgres_cursor,psgres_connection = db_connection.connect_to_postgres_db()

    query_max = "SELECT max(measured_value) FROM measurements"
    psgres_cursor.execute(query_max)
    data = psgres_cursor.fetchall()

    ret_val = list(data[0])

    return ret_val


def classify_ids_into_ranks(sorted_tuples):

    max_value = get_max_pollution_value()[0]

    THRESHOLD_UP = (3.0*max_value/4.0)
    THRESHOLD_MIDDLE = (2.0*max_value/4.0)
    THRESHOLD_DOWN = (1.0*max_value/4.0)

    group_highest = []
    group_mid_high = []
    group_mid_low = []
    group_lowest = []

    for item in sorted_tuples:
        if item[1]>THRESHOLD_UP:
            group_highest.append(item[0])
        elif item[1]>THRESHOLD_MIDDLE:
            group_mid_high.append(item[0])
        elif item[1]>THRESHOLD_DOWN:
            group_mid_low.append(item[0])
        else:
            group_lowest.append(item[0])

    return tuple(group_lowest),tuple(group_mid_low),tuple(group_mid_high),tuple(group_highest)



def update_cost_column_in_buffer(gidAndPollution_tuples,posgres_cursor,postgres_connection):
    sorted_values = sort_tuples(gidAndPollution_tuples)

    g_low,g_mid_low,g_mid_high,g_high = classify_ids_into_ranks(sorted_values)

    # http://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query

    # http://initd.org/psycopg/docs/usage.html

    stt = time.time()

    POLLUTION_AVE_INDEX = 4
    test_query = "SELECT gid,to_cost FROM buffer_geometry_table as alias ORDER BY alias.gid ASC"
    posgres_cursor.execute(test_query)
    todos = posgres_cursor.fetchall()

    HIGH_COST = 1.6
    MIDDLE_COST = 1.4
    LOW_COST = 1.2
    NO_COST = 1.0


    if len(g_high) > 0:
        query_set_cost = "UPDATE buffer_geometry_table SET to_cost = 8.0 WHERE gid IN %s"
        posgres_cursor.execute(query_set_cost,[g_high])

    if len(g_mid_high) > 0:
        query_set_cost = "UPDATE buffer_geometry_table SET to_cost = 6.0 WHERE gid IN %s"
        posgres_cursor.execute(query_set_cost,[g_mid_high])

    if len(g_mid_low) > 0:
        query_set_cost = "UPDATE buffer_geometry_table SET to_cost = 3.0 WHERE gid IN %s"
        posgres_cursor.execute(query_set_cost,[g_mid_low])

    if len(g_low) > 0:
        query_set_cost = "UPDATE buffer_geometry_table SET to_cost = 1.0 WHERE gid IN %s"
        posgres_cursor.execute(query_set_cost,[g_low])

def get_route_edges_given_start_end_on_pollution(source,target,psgres_cursor):

    ch = "SELECT source,target FROM buffer_geometry_table"
    psgres_cursor.execute(ch)
    ora = psgres_cursor.fetchall()

    all_source = []
    all_target = []
    for item in ora:
        all_source.append(item[0])
        all_target.append(item[1])

    dos_query = "SELECT * FROM buffer_geometry_table"
    psgres_cursor.execute(dos_query)
    datos = psgres_cursor.fetchall()

    query_route_astar = "SELECT DISTINCT ON (seq) seq, id1 AS node, id2 AS edge, cost FROM pgr_astar(" \
                        "'SELECT gid AS id,source::integer,target::integer,(length::double precision)*(to_cost::double precision) AS cost," \
                        "x1, y1, x2, y2 FROM buffer_geometry_table',%s,%s, false, false) ORDER BY seq"
    psgres_cursor.execute(query_route_astar,(source,target))

    rows = psgres_cursor.fetchall()

    edges = []

    REAL_GID_INDEX = 2
    for row in rows:
        edges.append(str(row[REAL_GID_INDEX]))

    # get rid of last element
    edges.pop()

    edges_tuples = tuple(edges)

    return edges_tuples


def main_program(start,end):
    start_lat = start[0]
    start_lon = start[1]

    end_lon = end[1]
    end_lat = end[0]

    db_connection = Connection()

    sql_cursor,sql_connection = db_connection.connect_to_pollution_db()
    psgres_cursor,psgres_connection = db_connection.connect_to_postgres_db()

    utils = Utils(db_connection)
    source = utils.find_nearest_vertex_id(start_lat,start_lon)
    target = utils.find_nearest_vertex_id(end_lat,end_lon)

    get_max_pollution_value()

    route_edge_ids,seq_and_geom_dict = get_route_edges_given_start_end(source,target,psgres_cursor)

    coordinates = calculate_bigger_bounding_box_given_route_gids(route_edge_ids,CONST,psgres_cursor)
    edge_ids = get_edges_id_inside_bounding_box(coordinates,CONST,psgres_cursor)

    all_ids = construct_buffer_following_linestring(route_edge_ids,seq_and_geom_dict,psgres_cursor)

    gid_with_pollutionAverage_tuples = average_pollution_of_given_ids(all_ids,psgres_cursor)

    update_cost_column_in_buffer(gid_with_pollutionAverage_tuples,psgres_cursor,psgres_connection)

    new_route = get_route_edges_given_start_end_on_pollution(source,target,psgres_cursor)

    coords_route = get_lat_lon_given_id(new_route,psgres_cursor)
    coordsWithpollution_route = get_lat_lon_pollution_given_id(new_route,gid_with_pollutionAverage_tuples,psgres_cursor)
    coord_pollution_length = get_lat_lon_pollution_length_given_id(new_route,gid_with_pollutionAverage_tuples,psgres_cursor)


    psgres_cursor.close()
    psgres_connection.close()
    sql_connection.close()

    return coord_pollution_length

if __name__=="__main__":
    start = [-37.805598, 144.963075]
    end = [-37.797205, 144.964470]
    main_program(start,end)