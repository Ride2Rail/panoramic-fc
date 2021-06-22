#!/usr/bin/env python3

import os
import configparser as cp

import redis
from flask import Flask, request

from r2r_offer_utils.logging import setup_logger
from r2r_offer_utils.cache_operations import read_data_from_cache_wrapper, store_simple_data_to_cache_wrapper
from r2r_offer_utils.normalization import zscore, minmaxscore
from utils import check_country, osm_query

import numpy as np
from shapely.geometry import Point
import geojson
import requests

service_name = os.path.splitext(os.path.basename(__file__))[0]
app = Flask(service_name)

# config
config = cp.ConfigParser()
config.read(f'{service_name}.conf')

# logging
logger, ch = setup_logger()

# score
score = config.get('running', 'scores')

# data from all europe
all_europe = config.get('data', 'all_europe')

# cache
cache = redis.Redis(host=config.get('cache', 'host'),
                    port=config.get('cache', 'port'),
                    decode_responses=True)


@app.route('/compute', methods=['POST'])
def extract():
    data = request.get_json()
    request_id = data['request_id']

    # ask for the entire list of offer ids
    offer_data = cache.lrange('{}:offers'.format(request_id), 0, -1)
    # print(offer_data)
    # print(all_europe)

    response = app.response_class(
        response=f'{{"request_id": "{request_id}"}}',
        status=200,
        mimetype='application/json'
    )

    output_offer_level, output_tripleg_level = read_data_from_cache_wrapper(pa_cache=cache, pa_request_id=request_id,
                                                                            pa_offer_level_items=[],
                                                                            pa_tripleg_level_items=['leg_stops'])

    # load the shapes of each country and convert them into a polygon
    countries = ['belgium', 'czech-republic', 'finland', 'france', 'greece', 'italy', 'norway', 'portugal',
                 'slovakia', 'spain', 'switzerland']

    offer_points_of_interest = dict()
    if 'offer_ids' in output_offer_level.keys():
        for offer_id in output_offer_level['offer_ids']:
            leg_points_of_interest = list()
            if 'triplegs' in output_tripleg_level[offer_id].keys():
                for leg_id in output_tripleg_level[offer_id]['triplegs']:
                    # coordinates
                    track = geojson.loads(output_tripleg_level[offer_id][leg_id]['leg_stops'])
                    if all_europe:
                        lat_ini, long_ini = str(track['coordinates'][0][0]), str(track['coordinates'][0][1])
                        lat_end, long_end = str(track['coordinates'][-1][0]), str(track['coordinates'][-1][1])
                        overpass_url = "http://172.20.61.117:8080/api/interpreter"
                        overpass_query = osm_query(lat_ini, long_ini, lat_end, long_end)
                        response_query = requests.get(overpass_url,
                                                      params={'data': overpass_query}, timeout=5)
                        data = response_query.json()
                        leg_points_of_interest.append(len(data['elements']), )
                    else:
                        # first the longitude, then the latitude
                        leg_start_coordinates = Point([track['coordinates'][0][1], track['coordinates'][0][0]])
                        # print(leg_start_coordinates)
                        country = check_country(leg_start_coordinates)
                        print(country)
                        # country = 'spain'
                        if country in countries:
                            lat_ini, long_ini = str(track['coordinates'][0][0]), str(track['coordinates'][0][1])
                            lat_end, long_end = str(track['coordinates'][-1][0]), str(track['coordinates'][-1][1])
                            # lat_ini, long_ini = str(39.8581), str(-4.02263)
                            # lat_end, long_end = str(40.4165), str(-3.70256)
                            overpass_url = "http://172.20.48.31/{}/api/interpreter".format(country)
                            overpass_query = osm_query(lat_ini, long_ini, lat_end, long_end)
                            response_query = requests.get(overpass_url,
                                                          params={'data': overpass_query}, timeout=5)
                            data = response_query.json()
                            leg_points_of_interest.append(len(data['elements']), )

                        else:
                            leg_points_of_interest.append(np.random.randint(0, 3))

            offer_points_of_interest.setdefault(offer_id, sum(leg_points_of_interest))
        print(offer_points_of_interest)
        if score == 'z_score':
            normalized_points_of_interest = zscore(offer_points_of_interest)
        else:
            normalized_points_of_interest = minmaxscore(offer_points_of_interest)
        print(normalized_points_of_interest)
        # store data to the cache
        try:
            store_simple_data_to_cache_wrapper(cache, request_id, normalized_points_of_interest, 'panoramic')
        except redis.exceptions.ConnectionError as exc:
            logging.debug("Writing outputs to cache by panoramic feature collector failed.")

        return response
    return response


if __name__ == '__main__':
    import argparse
    import logging
    from r2r_offer_utils.cli_utils import IntRange

    FLASK_PORT = 5000
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379

    parser = argparse.ArgumentParser()
    parser.add_argument('--redis-host',
                        default=REDIS_HOST,
                        help=f'Redis hostname [default: {REDIS_HOST}].')
    parser.add_argument('--redis-port',
                        default=REDIS_PORT,
                        type=IntRange(1, 65536),
                        help=f'Redis port [default: {REDIS_PORT}].')
    parser.add_argument('--flask-port',
                        default=FLASK_PORT,
                        type=IntRange(1, 65536),
                        help=f'Flask port [default: {FLASK_PORT}].')

    # remove default logger
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    # create file handler which logs debug messages
    fh = logging.FileHandler(f"{service_name}.log", mode='a+')
    fh.setLevel(logging.DEBUG)

    # set logging level to debug
    ch.setLevel(logging.DEBUG)

    os.environ["FLASK_ENV"] = "development"

    cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

    app.run(port=FLASK_PORT, debug=True)

    exit(0)
