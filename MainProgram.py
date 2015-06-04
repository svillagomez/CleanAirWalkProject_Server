__author__ = 'santiago'

import time
import Distance
import Constants as CONST
from DBConnection import *
from itertools import groupby
from operator import itemgetter
from collections import OrderedDict

# Location: class
# Usage: It is intended for retrieving information regarding a tweet location
class Utils(object):
    @staticmethod
    def diff_y1_x1(p1, p2):
        ans = pow(((pow((p1[0] - p2[0]),2))+(pow((p1[1] - p2[1]),2))), 0.5)
        return ans

    @staticmethod
    def calc_ends_diffs(t1, t2):
        dict_ans = {}
        counter = 0
        for i in range(0, 3, 2):
            for j in range(0, 3, 2):
                p1 = []
                p1.append(t1[i])
                p1.append(t1[i+1])
                p2 = []
                p2.append(t2[j])
                p2.append(t2[j+1])
                dict_ans[counter] = Utils.diff_y1_x1(p1,p2)
                counter += 1
        return dict_ans

    @staticmethod
    def invert_ends(t1):
        # make a copy because t1 might have more tha 4 elements inside
        partial = t1
        partial[0] = (t1[2])
        partial[1] = (t1[3])
        partial[2] = (t1[0])
        partial[3] = (t1[1])
        return partial

    @staticmethod
    def correct_pair(is_first_pair, t1, t2):
        dict_values = Utils.calc_ends_diffs(t1,t2)
        which = min(dict_values, key=dict_values.get)
        if is_first_pair == True:
            partial = []
            if which == 0 or which == 1:
                # invert values
                t1 = Utils.invert_ends(t1)
            elif which == 3:
                t2 = Utils.invert_ends(t2)
        else:
            #----- Assume the first point is in the correct order
            if which == 3:
                t2 = Utils.invert_ends(t2)
        return t1, t2

    @staticmethod
    def getKey(item):
        return item[0]

    @staticmethod
    def sort_tuples(tuples):
        return sorted(tuples,key=Utils.getKey)

    @staticmethod
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

    @staticmethod
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

class RouteClass():
    def __init__(self, posgres_connection, postgres_cursor):
        self.posgres_cursor = postgres_cursor
        # self.posgres_connection = posgres_connection

    def get_route_edges_given_source_target(self, source, target):
        query_route_astar = "SELECT DISTINCT ON (seq) seq, id1 AS node, id2 AS edge, cost, b.the_geom FROM pgr_astar(" \
                        "'SELECT gid AS id,source::integer,target::integer,length::double precision AS cost," \
                        "x1, y1, x2, y2 FROM ways',%s,%s, false, false) a LEFT JOIN ways b ON (a.id2 = b.gid) ORDER BY seq"

        self.posgres_cursor.execute(query_route_astar,(source,target))

        rows = self.posgres_cursor.fetchall()

        edges = []

        seq_and_geom_dict = OrderedDict()

        REAL_GID_INDEX = 2
        # THE_GEOM_INDEX = 4
        SEQ_INDEX = 0
        for row in rows:
            edges.append(str(row[REAL_GID_INDEX]))
            seq_and_geom_dict[row[SEQ_INDEX]] = row[REAL_GID_INDEX]

        # get rid of last element
        edges.pop()

        edges_tuples = tuple(edges)

        return edges_tuples, seq_and_geom_dict

    def get_route_edges_given_start_end_within_bounding_box_cost(self, source, target, coordinates):
        query_route_astar = "SELECT DISTINCT ON (seq) seq, id1 AS node, id2 AS edge, cost, b.the_geom FROM pgr_astar(" \
                            "'SELECT gid AS id,source::integer,target::integer,(length::double precision*to_cost::double precision) AS cost," \
                            "x1, y1, x2, y2 FROM ways WHERE y1>%s AND y1<%s AND x1>%s AND x1<%s',%s,%s, false, false) a LEFT JOIN ways b ON (a.id2 = b.gid)"
        self.posgres_cursor.execute(query_route_astar,(coordinates[CONST.LAT_MIN_INDEX],coordinates[CONST.LAT_MAX_INDEX],
                                                coordinates[CONST.LONG_MIN_INDEX],coordinates[CONST.LONG_MAX_INDEX],
                                                source,target))

        rows = self.posgres_cursor.fetchall()

        edges = []

        REAL_GID_INDEX = 2
        for row in rows:
            edges.append(str(row[REAL_GID_INDEX]))

        # get rid of last element
        edges.pop()

        edges_tuples = tuple(edges)

        return edges_tuples

    def get_route_edges_given_start_end_on_pollution(self, source, target):

        ch = "SELECT source,target FROM buffer_geometry_table"
        self.posgres_cursor.execute(ch)
        ora = self.posgres_cursor.fetchall()

        all_source = []
        all_target = []
        for item in ora:
            all_source.append(item[0])
            all_target.append(item[1])

        dos_query = "SELECT * FROM buffer_geometry_table"
        self.posgres_cursor.execute(dos_query)
        datos = self.posgres_cursor.fetchall()

        query_route_astar = "SELECT DISTINCT ON (seq) seq, id1 AS node, id2 AS edge, cost FROM pgr_astar(" \
                            "'SELECT gid AS id,source::integer,target::integer,(length::double precision)*(to_cost::double precision) AS cost," \
                            "x1, y1, x2, y2 FROM buffer_geometry_table',%s,%s, false, false) ORDER BY seq"
        self.posgres_cursor.execute(query_route_astar,(source,target))

        rows = self.posgres_cursor.fetchall()

        edges = []

        REAL_GID_INDEX = 2
        for row in rows:
            edges.append(str(row[REAL_GID_INDEX]))

        # get rid of last element
        edges.pop()

        edges_tuples = tuple(edges)

        return edges_tuples

    def calculate_bigger_bounding_box_given_route_gids(self, edges_tuples, CONST):

        coordinates = [0.0]*4

        lat_long_query = "SELECT max(x1),max(x2), min(x1),min(x2),max(y1),max(y2),min(y1),min(y2) FROM ways WHERE gid in %s"
        self.posgres_cursor.execute(lat_long_query,[edges_tuples])
        aux = self.posgres_cursor.fetchall()

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

    def get_edges_id_inside_bounding_box(self, coordinates,CONST):

        query_nodes_inside_bounding = "SELECT gid,source,target,length,to_cost,x1,y1,x2,y2,the_geom INTO bounding_box_table " \
                                      "FROM ways where the_geom && ST_MakeEnvelope(%f,%f,%f,%f,4326)" \
                                      %(coordinates[CONST.LONG_MIN_INDEX],coordinates[CONST.LAT_MIN_INDEX],
                                        coordinates[CONST.LONG_MAX_INDEX],coordinates[CONST.LAT_MAX_INDEX])

        self.posgres_cursor.execute(query_nodes_inside_bounding)

        query_dos = "SELECT gid FROM bounding_box_table"
        self.posgres_cursor.execute(query_dos)

        edges = self.posgres_cursor.fetchall()

        edge_ids=[]

        for row in edges:
            edge_ids.append(row[0])

        tuples_edge_ids = tuple(edge_ids)

        return tuples_edge_ids

    def construct_buffer_following_linestring(self, tuples_edge_ids,seq_and_geom_dict):
        seq_and_geom_dict.pop(len(seq_and_geom_dict)-1)

        query = "SELECT gid,source,target,length,to_cost,y1,x1,y2,x2 FROM ways WHERE gid IN %s"
        self.posgres_cursor.execute(query,[tuples_edge_ids])
        answer = self.posgres_cursor.fetchall()

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

        self.posgres_cursor.execute(query_nodes_inside_bounding)
        edges = self.posgres_cursor.fetchall()

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
        self.posgres_cursor.execute(query,[tuples_edge_ids])

        return tuples_edge_ids

    def average_pollution_of_given_ids(self, edge_ids):
        query_value = "SELECT gid, AVG(measured_value) as measured_value FROM measurements WHERE gid IN %s GROUP BY gid"
        self.posgres_cursor.execute(query_value,[edge_ids])

        gidAndValue = self.posgres_cursor.fetchall()

        gids_of_valued_adges = [ x[0] for x in gidAndValue ]
        all_gids = [x for x in edge_ids]
        gid_without_pollution = [item for item in all_gids if item not in gids_of_valued_adges]

        NO_POLLUTION = float(0.0)
        for item in gid_without_pollution:
            gidAndValue.append((item,NO_POLLUTION))

        return gidAndValue

    def find_nearest_vertex_id(self,lat,lon):
        query_start = "SELECT id::integer FROM ways_vertices_pgr " \
                      "ORDER BY the_geom <-> ST_GeometryFromText('POINT(%s %s)') LIMIT 1"
        self.posgres_cursor.execute(query_start,(lon,lat))
        ans = self.posgres_cursor.fetchall()

        for row in ans:
            edge_id =row[0]

        return edge_id

    # Leave this method 'cause be useful
    def get_lat_lon_given_id(self, tuples_edge_ids):
        # if do_order == True:
        query = "SELECT y1,x1,y2,x2,gid FROM ways WHERE gid in %s"
        self.posgres_cursor.execute(query,[tuples_edge_ids])
        data = self.posgres_cursor.fetchall()

        dict_lat_lon_id ={}
        # keep the same order as the imput gid_tuples
        for item in data:
            dict_lat_lon_id[item[4]] = [item[0], item[1], item[2], item[3]]

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
                first_edge,second_edge = Utils.correct_pair(True, first_edge, second_edge)
                edge = second_edge
                ret_val.append(first_edge)
                ret_val.append(second_edge)
            elif counter >2:
                next_edge = startAndEnd_coords_dict_gid[item]
                edge,next_edge = Utils.correct_pair(False, edge, next_edge)
                ret_val.append(next_edge)
                edge = next_edge
            counter += 1
        return ret_val


     # Leave this method 'cause be useful
    def get_lat_lon_pollution_given_id(self, tuples_edge_ids, gid_with_pollutionAverage_tuples):

        query = "SELECT y1,x1,y2,x2,gid FROM ways WHERE gid in %s"
        self.posgres_cursor.execute(query,[tuples_edge_ids])
        data = self.posgres_cursor.fetchall()

        gid_with_pollutionAverage_dict = dict(gid_with_pollutionAverage_tuples)

        dict_lat_lon_id ={}

        # keep the same order as the iput gid_tuples
        for item in data:
            # the gid must be the key for the dictionary. is located at position 4
            key = item[4]
            dict_lat_lon_id[key]=[item[0], item[1], item[2], item[3], gid_with_pollutionAverage_dict[key]]

        # keep the same order
        startAndEnd_coords_dict_gid = OrderedDict()
        for each in tuples_edge_ids:
            startAndEnd_coords_dict_gid[each] = dict_lat_lon_id[int(each)]

        ret_val = []
        edges = []
        counter = 0
        for item in startAndEnd_coords_dict_gid:
            if counter < 2:
                edges.append(startAndEnd_coords_dict_gid[item])
            elif counter == 2:
                first_edge = edges[0]
                second_edge = edges[1]
                first_edge,second_edge = Utils.correct_pair(True,first_edge,second_edge)
                edge = second_edge
                ret_val.append(first_edge)
                ret_val.append(second_edge)
            elif counter >2:
                next_edge = startAndEnd_coords_dict_gid[item]
                edge, next_edge = Utils.correct_pair(False, edge, next_edge)
                ret_val.append(next_edge)
                edge = next_edge
            counter += 1
        return ret_val

    def get_lat_lon_pollution_length_given_id(self, tuples_edge_ids, gid_with_pollutionAverage_tuples):
        query = "SELECT y1,x1,y2,x2,gid,length FROM ways WHERE gid in %s"
        self.posgres_cursor.execute(query,[tuples_edge_ids])
        data = self.posgres_cursor.fetchall()

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
                first_edge,second_edge = Utils.correct_pair(True,first_edge,second_edge)
                edge = second_edge
                ret_val.append(first_edge)
                ret_val.append(second_edge)
            elif counter >2:
                next_edge = startAndEnd_coords_dict_gid[item]
                edge,next_edge = Utils.correct_pair(False, edge, next_edge)
                ret_val.append(next_edge)
                edge = next_edge
            counter += 1
        return ret_val

    def update_cost_column_in_buffer(self, gidAndPollution_tuples):
        sorted_values = Utils.sort_tuples(gidAndPollution_tuples)

        g_low,g_mid_low,g_mid_high,g_high = Utils.classify_ids_into_ranks(sorted_values)

        # http://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query

        # http://initd.org/psycopg/docs/usage.html

        POLLUTION_AVE_INDEX = 4
        test_query = "SELECT gid,to_cost FROM buffer_geometry_table as alias ORDER BY alias.gid ASC"
        self.posgres_cursor.execute(test_query)
        todos = self.posgres_cursor.fetchall()

        HIGH_COST = 4.0
        MIDDLE_COST = 3.0
        LOW_COST = 2.0
        NO_COST = 1.0


        if len(g_high) > 0:
            query_set_cost = "UPDATE buffer_geometry_table SET to_cost = 4.0 WHERE gid IN %s"
            self.posgres_cursor.execute(query_set_cost,[g_high])

        if len(g_mid_high) > 0:
            query_set_cost = "UPDATE buffer_geometry_table SET to_cost = 3.0 WHERE gid IN %s"
            self.posgres_cursor.execute(query_set_cost,[g_mid_high])

        if len(g_mid_low) > 0:
            query_set_cost = "UPDATE buffer_geometry_table SET to_cost = 2.0 WHERE gid IN %s"
            self.posgres_cursor.execute(query_set_cost,[g_mid_low])

        if len(g_low) > 0:
            query_set_cost = "UPDATE buffer_geometry_table SET to_cost = 1.0 WHERE gid IN %s"
            self.posgres_cursor.execute(query_set_cost,[g_low])

def get_max_pollution_value():
    db_connection = Connection()
    psgres_cursor,psgres_connection = db_connection.connect_to_postgres_db()

    query_max = "SELECT max(measured_value) FROM measurements"
    psgres_cursor.execute(query_max)
    data = psgres_cursor.fetchall()

    ret_val = list(data[0])

    return ret_val


# function: add_region_field
    # description: add the suburb information taking into account the location values inside a twee
    # return: a tweet with suburb field
    # parameters: single tweet

def main_program(start, end):
    start_lat = start[0]
    start_lon = start[1]

    end_lon = end[1]
    end_lat = end[0]

    db_connection = Connection()

    postgres_cursor, postgres_connection = db_connection.connect_to_postgres_db()

    route_util = RouteClass(postgres_connection,postgres_cursor)

    source = route_util.find_nearest_vertex_id(start_lat, start_lon)

    target = route_util.find_nearest_vertex_id(end_lat, end_lon)

    route_edge_ids, seq_and_geom_dict = route_util.get_route_edges_given_source_target(source, target)

    coordinates = route_util.calculate_bigger_bounding_box_given_route_gids(route_edge_ids, CONST)

    route_util.get_edges_id_inside_bounding_box(coordinates, CONST)

    all_ids = route_util.construct_buffer_following_linestring(route_edge_ids, seq_and_geom_dict)

    gid_with_pollutionAverage_tuples = route_util.average_pollution_of_given_ids(all_ids)

    route_util.update_cost_column_in_buffer(gid_with_pollutionAverage_tuples)

    new_route = route_util.get_route_edges_given_start_end_on_pollution(source, target)

    coord_pollution_length = route_util.get_lat_lon_pollution_length_given_id(new_route, gid_with_pollutionAverage_tuples)

    postgres_cursor.close()
    postgres_connection.close()

    return coord_pollution_length

if __name__=="__main__":
    # start = [-37.805598, 144.963075]
    start = [-37.514217, 144.803413]
    end = [-37.797205, 144.964470]
    main_program(start, end)