__author__ = 'santiago'


from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.request
import time
import queue
import threading

def get_route(q, url):
    q.put(urllib.request.urlopen(url).read())

start_time = time.time()

all_urls = ["https://maps.googleapis.com/maps/api/directions/json?origin=-37.818563,144.959880&destination=-37.800720,144.966958&waypoints=-37.816313,144.964192|-37.812230,144.962234|-37.813641,144.957277|-37.809099,144.955351|-37.808870,144.957958|-37.806505,144.961499|-37.803351,144.964997|-37.802114,144.966767&mode=bicycling",
            "https://maps.googleapis.com/maps/api/directions/json?origin=-37.818563,144.959880&destination=-37.800720,144.966958&waypoints=-37.816313,144.964192|-37.812230,144.962234|-37.813641,144.957277|-37.809099,144.955351|-37.808870,144.957958|-37.806505,144.961499|-37.803351,144.964997|-37.802114,144.966767&mode=walking",
            "https://maps.googleapis.com/maps/api/directions/json?origin=-37.818563,144.959880&destination=-37.800720,144.966958&waypoints=-37.816313,144.964192|-37.812230,144.962234|-37.813641,144.957277|-37.809099,144.955351|-37.808870,144.957958|-37.806505,144.961499|-37.803351,144.964997|-37.802114,144.966767&mode=driving"]

q = queue.Queue()


for each in all_urls:
    t = threading.Thread(target=get_route, args=(q,each))
    t.daemon = True
    t.start()

fetch_all_ans = q.get()

    # print(q.get())

print(fetch_all_ans)


# q.get()
# print(s)

print("total=",(time.time()-start_time))


# start_time = time.time()
# x = urllib.request.urlopen("https://maps.googleapis.com/maps/api/directions/json?origin=-37.818563,144.959880&destination=-37.800720,144.966958&waypoints=-37.816313,144.964192|-37.812230,144.962234|-37.813641,144.957277|-37.809099,144.955351|-37.808870,144.957958|-37.806505,144.961499|-37.803351,144.964997|-37.802114,144.966767&mode=bicycling")
# print(x.read())
# print("total=",(time.time()-start_time))
# x = urllib.request.urlopen("https://maps.googleapis.com/maps/api/directions/json?origin=-37.818563,144.959880&destination=-37.800720,144.966958&waypoints=-37.816313,144.964192|-37.812230,144.962234|-37.813641,144.957277|-37.809099,144.955351|-37.808870,144.957958|-37.806505,144.961499|-37.803351,144.964997|-37.802114,144.966767&mode=bicycling")
# print(x.read())
# print("total=",(time.time()-start_time))
# x = urllib.request.urlopen("https://maps.googleapis.com/maps/api/directions/json?origin=-37.818563,144.959880&destination=-37.800720,144.966958&waypoints=-37.816313,144.964192|-37.812230,144.962234|-37.813641,144.957277|-37.809099,144.955351|-37.808870,144.957958|-37.806505,144.961499|-37.803351,144.964997|-37.802114,144.966767&mode=bicycling")
# print(x.read())
# print("total=",(time.time()-start_time))
# x = urllib.request.urlopen("https://maps.googleapis.com/maps/api/directions/json?origin=-37.818563,144.959880&destination=-37.800720,144.966958&waypoints=-37.816313,144.964192|-37.812230,144.962234|-37.813641,144.957277|-37.809099,144.955351|-37.808870,144.957958|-37.806505,144.961499|-37.803351,144.964997|-37.802114,144.966767&mode=bicycling")
# print(x.read())
# print("total=",(time.time()-start_time))
# x = urllib.request.urlopen("https://maps.googleapis.com/maps/api/directions/json?origin=-37.818563,144.959880&destination=-37.800720,144.966958&waypoints=-37.816313,144.964192|-37.812230,144.962234|-37.813641,144.957277|-37.809099,144.955351|-37.808870,144.957958|-37.806505,144.961499|-37.803351,144.964997|-37.802114,144.966767&mode=bicycling")
# print(x.read())
# print("total=",(time.time()-start_time))

def run(server_class=HTTPServer, handler_class=BaseHTTPRequestHandler):
    server_address = ('', 8000)
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()

