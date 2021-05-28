from functools import partial

import pyproj
from shapely import geometry
from shapely.geometry import Point
from shapely.ops import transform

def get_radius_meters(lon, lat, radius):
    '''
    Params: 
        - lon (Longitude in WGS84 projection)
        - lat (Latitude in WGS84 projection)
        - radius in meters
    '''

    local_azimuthal_projection = "+proj=aeqd +R=6371000 +units=m +lat_0={} +lon_0={}".format(
        lat, lon
    )
    wgs84_to_aeqd = partial(
        pyproj.transform,
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
        pyproj.Proj(local_azimuthal_projection),
    )
    aeqd_to_wgs84 = partial(
        pyproj.transform,
        pyproj.Proj(local_azimuthal_projection),
        pyproj.Proj("+proj=longlat +datum=WGS84 +no_defs"),
    )

    center = Point(float(lon), float(lat))
    point_transformed = transform(wgs84_to_aeqd, center)
    buffer = point_transformed.buffer(radius)

    # Get the polygon with lat lon coordinates
    circle_poly = transform(aeqd_to_wgs84, buffer)

    return circle_poly