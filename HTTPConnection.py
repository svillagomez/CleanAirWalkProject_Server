__author__ = 'santiago'
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from MainProgram import main_program
from MainProgram import get_max_pollution_value
from MigrateData import update
import urlparse

import json

# suponiendo = "/q?lat-37.7999168567061lon144.93830289691687lat-37.80298775026495lon144.94625229388475"

class HttpRequestHandler(BaseHTTPRequestHandler):
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

class Route_json_encoder():
    def __init__(self):
        pass

    def _encode(self, route):
        route_dict = {}

        for point_seq in range(0,len(route)):
            route_dict[point_seq]=route[point_seq]

        data_string = json.dumps(route_dict)
        return data_string

class ParamParser():
    def __init__(self):
        pass

    def parse_url_params(self, path):
        if '?' in path:
            path,tmp = path.split('?', 1)
            qs = urlparse.parse_qs(tmp)
            start_str = qs['start']
            end_str = qs['end']
            start = [float(i) for i in start_str]
            end = [float(i) for i in end_str]
        return (start,end)


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
