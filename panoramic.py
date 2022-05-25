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
from datetime import datetime as dt
from datetime import timezone
import random


service_name = os.path.splitext(os.path.basename(__file__))[0]
app = Flask(service_name)

# config
config = cp.ConfigParser()
config.read(f'{service_name}.conf')

# logging
logger, ch = setup_logger()

# score
score = config.get('running', 'scores')
execution_mode = config.get('running', 'mode')
minimum_waiting = float(config.get('running', 'minimum_waiting'))

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

    response = app.response_class(
        response=f'{{"request_id": "{request_id}"}}',
        status=200,
        mimetype='application/json'
    )

    output_offer_level, output_tripleg_level = read_data_from_cache_wrapper(pa_cache=cache, pa_request_id=request_id,
                                                                            pa_offer_level_items=['start_time'],
                                                                            pa_tripleg_level_items=['leg_stops',
                                                                                                    'start_time',
                                                                                                    'end_time'])

    # load the shapes of each country and convert them into a polygon
    countries = ['belgium', 'czech-republic', 'finland', 'france', 'greece', 'italy', 'norway', 'portugal',
                 'slovakia', 'spain', 'switzerland']

    offer_start_time_string = output_offer_level[output_offer_level['offer_ids'][0]]['start_time']
    try:
        offer_start_time = dt.fromisoformat(offer_start_time_string)
    except ValueError:
        # this is to handle an error in the formatting of the time string in some TRIAS files
        offer_start_time_string = offer_start_time_string[:offer_start_time_string.index('+')] + '0' + offer_start_time_string[offer_start_time_string.index('+'):]
        offer_start_time = dt.fromisoformat(offer_start_time_string)
    # get the time zone from one of the leg_times, or else default it to UTC
    try:
        time_zone = offer_start_time.tzinfo
    except:
        time_zone = timezone.utc
    current_time = dt.now(tz=time_zone)

    offer_points_of_interest = dict()
    if 'offer_ids' in output_offer_level.keys():
        for offer_id in output_offer_level['offer_ids']:
            leg_points_of_interest = list()
            if 'triplegs' in output_tripleg_level[offer_id].keys():
                # 'reversed' sorts legs chronologically by start time, from first to last 
                leg_ids = list(reversed(output_tripleg_level[offer_id]['triplegs']))

                for i in range(len(leg_ids)):
                    next_start_time_string = output_tripleg_level[offer_id][leg_ids[i]]['start_time']
                    try:
                        next_start_time = dt.fromisoformat(next_start_time_string)
                    except ValueError:
                        # this is to handle an error in the formatting of the time string in some TRIAS files
                        next_start_time_string = next_start_time_string[:next_start_time_string.index('+')] + '0' + next_start_time_string[next_start_time_string.index('+'):]
                        next_start_time = dt.fromisoformat(next_start_time_string)
                    if i == 0:
                        waiting_time = (next_start_time - current_time).total_seconds()/60
                    else:
                        previous_end_time_string = output_tripleg_level[offer_id][leg_ids[i-1]]['end_time']
                        try:
                            previous_end_time = dt.fromisoformat(previous_end_time_string)
                        except ValueError:
                            # this is to handle an error in the formatting of the time string in some TRIAS files
                            previous_end_time_string = previous_end_time_string[:previous_end_time_string.index('+')] + '0' + previous_end_time_string[previous_end_time_string.index('+'):]
                            previous_end_time = dt.fromisoformat(previous_end_time_string)
                        waiting_time = (next_start_time - previous_end_time).total_seconds()/60

                    if execution_mode == 'TEST' and waiting_time == 0 and minimum_waiting > 0:
                        waiting_time = random.randint(0, 2*minimum_waiting)
                        logger.info('The waiting time has been manually changed for testing.')
                    
                    logger.info(f'Waiting time: {waiting_time}')
                    if waiting_time > minimum_waiting:

                        if i == len(output_tripleg_level[offer_id]['triplegs']) - 1:
                            last_leg = True
                        else:
                            last_leg = False

                        # coordinates
                        track = geojson.loads(output_tripleg_level[offer_id][leg_ids[i]]['leg_stops'])
                        if all_europe:
                            lat_ini, long_ini = str(track['coordinates'][0][0]), str(track['coordinates'][0][1])
                            lat_end, long_end = str(track['coordinates'][-1][0]), str(track['coordinates'][-1][1])
                            #overpass_url = "http://172.20.61.117:8080/api/interpreter"
                            overpass_url = "http://192.168.2.7:8080/api/interpreter"
                            overpass_query = osm_query(lat_ini, long_ini, lat_end, long_end, last_leg=last_leg)
                            response_query = requests.get(overpass_url,
                                                          params={'data': overpass_query})#, timeout=5)
                            data = response_query.json()
                            leg_points_of_interest.append(len(data['elements']), )
                        else:
                            # first the longitude, then the latitude
                            leg_start_coordinates = Point([track['coordinates'][0][1], track['coordinates'][0][0]])
                            country = check_country(leg_start_coordinates)
                            if country in countries:
                                lat_ini, long_ini = str(track['coordinates'][0][0]), str(track['coordinates'][0][1])
                                lat_end, long_end = str(track['coordinates'][-1][0]), str(track['coordinates'][-1][1])

                                overpass_url = "http://172.20.48.31/{}/api/interpreter".format(country)
                                overpass_query = osm_query(lat_ini, long_ini, lat_end, long_end)
                                response_query = requests.get(overpass_url,
                                                                params={'data': overpass_query}, timeout=5)
                                data = response_query.json()
                                leg_points_of_interest.append(len(data['elements']), )

                            else:
                                leg_points_of_interest.append(np.random.randint(0, 3))

            offer_points_of_interest.setdefault(offer_id, sum(leg_points_of_interest))
        if score == 'z_score':
            normalized_points_of_interest = zscore(offer_points_of_interest)
        else:
            normalized_points_of_interest = minmaxscore(offer_points_of_interest)
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
