__author__ = 'santiago'
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from MainProgram import main_program
from MainProgram import get_max_pollution_value
from MigrateData import update
import urlparse

import json

# suponiendo = "/q?lat-37.7999168567061lon144.93830289691687lat-37.80298775026495lon144.94625229388475"


# Name = HttpRequestHandler: class
# Usage: define method to encode routes into JSON
# Costructor parameters: BaseHTTPRequestHandler => HTTP handler
class HttpRequestHandler(BaseHTTPRequestHandler):

    # function: do_GET
    # description: processes get requests (update the DB after every call
    # return: None
    # parameters: None
    def do_GET(self):
        my_parser = ParamParser()
        if 'start=' in self.path:
            start,end = my_parser.parse_url_params(self.path)
            route = main_program(start,end)
            my_encoder = Route_json_encoder()
            coord_json = my_encoder._encode(route)
            self.send_response(200,coord_json)
        elif 'maxValue=' in self.path:
            ret_val = get_max_pollution_value()
            my_encoder = Route_json_encoder()
            max_val_json = my_encoder._encode(ret_val)
            self.send_response(200,max_val_json)

        update()

    def log_message(self, format, *args):
        return


# Name = Route_json_encoder: class
# Usage: define method to encode routes into JSON
class Route_json_encoder():
    def __init__(self):
        pass

    # function: _encode
    # description: encode a route into json format
    # return: data_string =>  json encoding of a route
    # parameters: route
    def _encode(self, route):
        route_dict = {}

        for point_seq in range(0,len(route)):
            route_dict[point_seq]=route[point_seq]

        data_string = json.dumps(route_dict)
        return data_string

# Name = ParamParser: class
# Usage: define method to parse parse parameters
class ParamParser():
    def __init__(self):
        pass

    # function: parse_url_params
    # description: parse parameters ( get start and end coordinates)
    # return: start =>  start coordinates (lat , lon)
    #           end => start coordinates (lat , lon)
    # parameters: the incoming request to this server
    def parse_url_params(self, path):
        if '?' in path:
            path,tmp = path.split('?', 1)
            qs = urlparse.parse_qs(tmp)
            start_str = qs['start']
            end_str = qs['end']
            start = [float(i) for i in start_str]
            end = [float(i) for i in end_str]
        return (start,end)


# function: run
# description: runs the main logic of server (run server)
# return: None
# parameters: None
def run():
    HOST_NAME = ""
    PORT = 5678
    server_add = (HOST_NAME, PORT)
    httpserver = HTTPServer(server_add, HttpRequestHandler)

    try:
        httpserver.serve_forever()
    except KeyboardInterrupt:
        httpserver.shutdown()
        httpserver.server_close()
    httpserver.shutdown()
    httpserver.server_close()

if __name__ == '__main__':
    run()
