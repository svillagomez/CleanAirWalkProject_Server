__author__ = 'santiago'
from Constants import EARTH_RADIUS

# http://stackoverflow.com/questions/7477003/calculating-new-longtitude-latitude-from-old-n-meters

import math
"""calcu;ate vertices of bounding box or envelope adding some distance"""

# function: expand_box
# description: create a bigger bounding box given extrem parameters (smaller bounding box)
# return: None
# parameters: lat1, long1, lat2, long2 => coordinate values
def expand_box(lat1, long1, lat2, long2):

    degrees_to_radians = math.pi/180.0

    # km
    distance = 0.8

    if(lat1>lat2):
        new_up = lat1 + (distance/EARTH_RADIUS)*(180/math.pi)
        new_down = lat2 - (distance/EARTH_RADIUS)*(180/math.pi)
    else:
        new_up = lat2 + (distance/EARTH_RADIUS)*(180/math.pi)
        new_down = lat1 - (distance/EARTH_RADIUS)*(180/math.pi)

    if(long1 > long2):
        new_right = long1 + (distance/EARTH_RADIUS)*\
                        (180/math.pi)/math.cos(lat1*math.pi/180)
        new_left = long2 - (distance/EARTH_RADIUS)*\
                        (180/math.pi)/math.cos(lat2*math.pi/180)
    else:
        new_right = long2 + (distance/EARTH_RADIUS)*\
                        (180/math.pi)/math.cos(lat2*math.pi/180)
        new_left = long1 - (distance/EARTH_RADIUS)*\
                        (180/math.pi)/math.cos(lat1*math.pi/180)

    return (new_left,new_right,new_up,new_down)