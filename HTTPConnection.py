__author__ = 'santiago'
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import os
from MainProgram import main_program
import time
import urlparse
# from urlparse import urlparse

import json

# suponiendo = "/q?lat-37.7999168567061lon144.93830289691687lat-37.80298775026495lon144.94625229388475"



class HttpRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # print(self.path)
        my_parser = ParamParser()
        start,end = my_parser.parse_url_params(self.path)
        route = main_program(start,end)
        my_encoder = route_json_encoder()
        coord_json = my_encoder.encode(route)

        # print(myurlparse.parse_qs(os.environ['']))
        # print("CAlled from:",self.path())
        # route = main_program()
        # my_encoder = route_json_encoder()
        # coord_json = my_encoder.encode(route)
        # self.send_header()
        self.send_response(200,coord_json)
        # self.send_header('Content type','text-html')
        # self.end_headers()
        # self.wfile.write(coord_json)
        # print("HELLO")
        return

class route_json_encoder():
    def __init__(self):
        # print("HELLO")
        pass

    def encode(self, route):
        route_dict = {}

        for point_seq in range(0,len(route)):
            route_dict[point_seq]=route[point_seq]

        data_string = json.dumps(route_dict)
        return data_string
        # print(data_string)

class ParamParser():
    def parse_url_params(self,path):
        if '?' in path:
            path,tmp = path.split('?',1)
            qs = urlparse.parse_qs(tmp)
            start_str = qs['start']
            end_str = qs['end']
            start = [float(i) for i in start_str]
            end = [float(i) for i in end_str]
        return (start,end)


def run():
    HOST_NAME = ""
    PORT = 5667
    server_add = (HOST_NAME, PORT)
    httpserv = HTTPServer(server_add, HttpRequestHandler)
    # print(time.asctime(),"Server started - %s-%s"%(HOST_NAME,PORT))

    try:
        httpserv.serve_forever()
    except KeyboardInterrupt:
        pass
    httpserv.server_close()

    # print(time.asctime(),"Server stopped - %s-%s"%(HOST_NAME,PORT))

if __name__ == '__main__':
    run()

# import SimpleHTTPServer
# import SocketServer
#
#
# PORT = 5658
#
# Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
# httpd = SocketServer.TCPServer(("", PORT), Handler)
# print("serving at port",PORT)
# httpd.serve_forever()

# import BaseHTTPServer
# import time
#
# HOST_NAME = '127.0.0.1' # !!!REMEMBER TO CHANGE THIS!!!
# PORT_NUMBER = 9999 # Maybe set this to 9000.
#
# class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
#     def do_HEAD(s):
#         print("Que sera")
#         s.send_response(200)
#         s.send_header("Content-type", "text/html")
#         s.end_headers()
#     def do_GET(s):
#         """Respond to a GET request."""
#         print("No funcia")
#         s.send_response(200)
#         s.send_header("Content-type", "text/html")
#         s.end_headers()
#         s.wfile.write("<html><head><title>Title goes here.</title></head>")
#         s.wfile.write("<body><p>This is a test.</p>")
#         # If someone went to "http://something.somewhere.net/foo/bar/",
#         # then s.path equals "/foo/bar/".
#         s.wfile.write("<p>You accessed path: %s</p>" % s.path)
#         s.wfile.write("</body></html>")
#
# if __name__ == '__main__':
#     server_class = BaseHTTPServer.HTTPServer
#     httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
#     print time.asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER)
#     try:
#         httpd.serve_forever()
#     except KeyboardInterrupt:
#         pass
#     httpd.server_close()
#     print time.asctime(), "Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER)
#
#
# from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
#
# class MyHandler(BaseHTTPRequestHandler):
#     def do_GET(self):
#         print("Just received a GET request")
#         self.send_response(200)
#         self.send_header("Content-type", "text/html")
#         self.end_headers()
#
#         self.wfile.write('Hello world')
#
#         return
#
#     def log_request(self, code=None, size=None):
#         print('Request')
#
#     def log_message(self, format, *args):
#         print('Message')
#
# if __name__ == "__main__":
#     try:
#         server = HTTPServer(('localhost', 5566), MyHandler)
#         print('Started http server')
#         server.serve_forever()
#     except KeyboardInterrupt:
#         print('^C received, shutting down server')
#         server.socket.close()