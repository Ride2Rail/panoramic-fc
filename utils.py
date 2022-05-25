import numpy as np
from shapely.geometry import Polygon

# belgium
belgium_shape = np.loadtxt('polygons/belgium.txt')
belgium_polygon = Polygon(belgium_shape)
# czech republic
czech_republic_shape = np.loadtxt('polygons/czech-republic.txt')
czech_republic_polygon = Polygon(czech_republic_shape)
# finland
finland_shape = np.loadtxt('polygons/finland.txt')
finland_polygon = Polygon(finland_shape)
# france
france_shape = np.loadtxt('polygons/france.txt')
france_polygon = Polygon(france_shape)
# greece
greece_shape = np.loadtxt('polygons/greece.txt')
greece_polygon = Polygon(greece_shape)
# italy
italy_shape = np.loadtxt('polygons/italy.txt')
italy_polygon = Polygon(italy_shape)
# norway
norway_shape = np.loadtxt('polygons/norway.txt')
norway_polygon = Polygon(norway_shape)
# portugal
portugal_shape = np.loadtxt('polygons/portugal.txt')
portugal_polygon = Polygon(portugal_shape)
# slovakia
slovakia_shape = np.loadtxt('polygons/slovakia.txt')
slovakia_polygon = Polygon(slovakia_shape)
# spain
spain_shape = np.loadtxt('polygons/spain.txt')
spain_polygon = Polygon(spain_shape)
# switzerland
switzerland_shape = np.loadtxt('polygons/switzerland.txt')
switzerland_polygon = Polygon(switzerland_shape)


def check_country(intial_coordinates):
    """This function compute in which country the given coordinate belongs to"""
    if belgium_polygon.contains(intial_coordinates):
        return 'belgium'
    elif czech_republic_polygon.contains(intial_coordinates):
        return 'czech-republic'
    elif finland_polygon.contains(intial_coordinates):
        return 'finland'
    elif france_polygon.contains(intial_coordinates):
        return 'france'
    elif greece_polygon.contains(intial_coordinates):
        return 'greece'
    elif italy_polygon.contains(intial_coordinates):
        return 'italy'
    elif norway_polygon.contains(intial_coordinates):
        return 'norway'
    elif portugal_polygon.contains(intial_coordinates):
        return 'portugal'
    elif slovakia_polygon.contains(intial_coordinates):
        return 'slovakia'
    elif spain_polygon.contains(intial_coordinates):
        return 'spain'
    elif switzerland_polygon.contains(intial_coordinates):
        return 'switzerland'
    else:
        return 'other'


def osm_query(lat_ini, long_ini, lat_end, long_end, last_leg=False):
    if last_leg:
        query = """
        [out:json];
        (
        node["historic"="monument"](around:100, {latini},{lonini});
        node["historic"="monument"](around:100, {latend},{longend});
        node["tourism"="attraction"](around:100, {latini},{lonini});
        node["tourism"="attraction"](around:100, {latend},{longend});
        node["tourism"="artwork"](around:100, {latini},{lonini});
        node["tourism"="artwork"](around:100, {latend},{longend});
        node["historic"="archaeological_site"](around:100, {latini},{lonini});
        node["historic"="archaeological_site"](around:100, {latend},{longend});
        node["historic"="wayside_cross"](around:100, {latini},{lonini});
        node["historic"="wayside_cross"](around:100, {latend},{longend});
        node["historic"="memorial"](around:100, {latini},{lonini});
        node["historic"="memorial"](around:100, {latend},{longend});
        node["historic"="yes"](around:100, {latini},{lonini});
        node["historic"="yes"](around:100, {latend},{longend});
        node["historic"="wayside_shrine"](around:100, {latini},{lonini});
        node["historic"="wayside_shrine"](around:100, {latend},{longend});
        node["historic"="ruins"](around:100, {latini},{lonini});
        node["historic"="ruins"](around:100, {latend},{longend});
        node["natural"="coastline"](around:100, {latini},{lonini});
        node["natural"="coastline"](around:100, {latend},{longend});
        node["natural"="water"](around:100, {latini},{lonini});
        node["natural"="water"](around:100, {latend},{longend});
        node["natural"="peak"](around:100, {latini},{lonini});
        node["natural"="peak"](around:100, {latend},{longend});
        node["natural"="cliff"](around:100, {latini},{lonini});
        node["natural"="cliff"](around:100, {latend},{longend});
        node["tourism"="viewpoint"](around:100, {latini},{lonini});
        node["tourism"="viewpoint"](around:100, {latend},{longend});
        );
        out center;
        """.format(latini=lat_ini, lonini=long_ini, latend=lat_end, longend=long_end) 

    else:
        query = """
        [out:json];
        (
        node["historic"="monument"](around:100, {latini},{lonini});
        node["tourism"="attraction"](around:100, {latini},{lonini});
        node["tourism"="artwork"](around:100, {latini},{lonini});
        node["historic"="archaeological_site"](around:100, {latini},{lonini});
        node["historic"="wayside_cross"](around:100, {latini},{lonini});
        node["historic"="memorial"](around:100, {latini},{lonini});
        node["historic"="yes"](around:100, {latini},{lonini});
        node["historic"="wayside_shrine"](around:100, {latini},{lonini});
        node["historic"="ruins"](around:100, {latini},{lonini});
        node["natural"="coastline"](around:100, {latini},{lonini});
        node["natural"="water"](around:100, {latini},{lonini});
        node["natural"="peak"](around:100, {latini},{lonini});
        node["natural"="cliff"](around:100, {latini},{lonini});
        node["tourism"="viewpoint"](around:100, {latini},{lonini});
        );
        out center;
        """.format(latini=lat_ini, lonini=long_ini) 

    return query
