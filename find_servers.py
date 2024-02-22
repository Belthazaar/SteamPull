import datetime
import json
import logging
import os
import requests
import random
import time
from dotenv import load_dotenv
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from threading import Thread

load_dotenv()
API_KEY = os.environ.get('STEAM_API_KEY')
MONGO_URI = os.environ.get('MONGO_URI')
DB_NAME = os.environ.get('DB_NAME')
LOG_DB_NAME = os.environ.get('LOG_DB_NAME', None)
LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'info').upper())

base_url = 'http://api.steampowered.com/'
chin_url = 'http://api.steamchina.com/'

params = {
    'format': 'json',
    'key': API_KEY,
}

now = datetime.datetime.now(datetime.UTC)
minut = (now.minute // 10) * 10
rounded_ts = now.replace(minute=minut, second=0, microsecond=0)
time_stamp = rounded_ts

cm_cache_detail = []

cell_id_to_region = {}

seen_caches = []
seen_cms = []


class MongoDBHandler(logging.Handler):

    def __init__(self, collection: Collection):
        super().__init__()
        self.collection = collection

    def emit(self, record):
        log_entry = self.format_record_for_mongodb(record)
        self.collection.insert_one(log_entry)

    def format_record_for_mongodb(self, record):
        """Formats the log record into a dictionary for MongoDB."""
        return {
            "timestamp": datetime.datetime.fromtimestamp(record.created, datetime.UTC),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
        }


def get(interface, method, version, params, china=False):

    if china:
        url = chin_url + interface + '/' + method + '/v' + str(version) + '/'
    else:
        url = base_url + interface + '/' + method + '/v' + str(version) + '/'
    response = requests.get(url, params=params)
    if not response.ok:
        raise Exception(f'Error getting {response.url}: {response.status_code}')
    logger.debug(f'Got {url}: {response.json()}')
    return response.json()


def get_cache_details(cell_id, t_id, attempt=0):
    if attempt > 0:
        sleep_timer = random.randint(2, 30)
        logger.info(f"{t_id:>2} Gonna sleep for {sleep_timer:>3} seconds")
        time.sleep(sleep_timer)
    try:
        db = get_database()
        cache_params = params.copy()

        cache_params['cell_id'] = cell_id
        resp = get('IContentServerDirectoryService', 'GetServersForSteamPipe', 1, cache_params)
        servers = resp['response']['servers']
        matching_ids = [s for s in servers if s.get('cell_id') == cell_id]
        if len(matching_ids) == 0:
            return
        unique_caches = []
        for server in servers:
            server['time_stamp'] = time_stamp
            server['query_id'] = cell_id
            uq = {
                "last_seen": time_stamp,
                "host": server['host'],
                "vhost": server['vhost'],
                "load": server['load'],
            }
            if 'cell_id' in server:
                uq['cell_id'] = server['cell_id']
                if server['cell_id'] not in [c['cell_id'] for c in cm_cache_detail if c['cache']]:
                    uq['first_seen'] = time_stamp
                    logger.info(f"Completely new unseen cache ID found: {server['cell_id']}")
                else:
                    cell_details = cell_id_to_region[server['cell_id']]
                    uq['region'] = cell_details['region']
                    uq['code'] = cell_details['code']
                    uq['city'] = cell_details['city']

            if server['host'] not in seen_caches:
                uq['first_seen'] = time_stamp
                logger.info(f"Completely new unseen cache host found: {server['host']}")
            unique_caches.append(uq)
        if not unique_caches:
            return
        for c in unique_caches:
            db["cache_hosts"].find_one_and_update({"host": c['host']}, {"$set": c}, upsert=True)
    except KeyError as e:
        logger.warning(f"{t_id:>2} encountered an error: \n{e}")
        return
    except Exception as e:
        logger.warning(f"{t_id:>2} encountered an error: \n{e}")
        if attempt > 5:
            return
        attempt += 1
        get_cache_details(cell_id, t_id, attempt)


def get_china_cache_details(cell_id, t_id, attempt=0):
    if attempt > 0:
        sleep_timer = random.randint(2, 30)
        logger.info(f"{t_id:>2} Gonna sleep for {sleep_timer:>3} seconds")
        time.sleep(sleep_timer)
    try:

        db = get_database()
        cache_params = params.copy()
        cache_params['cell_id'] = cell_id
        resp = get('IContentServerDirectoryService', 'GetServersForSteamPipe', 1, cache_params, china=True)
        servers = resp['response']['servers']
        matching_ids = [s for s in servers if s.get('cell_id') == cell_id]
        if len(matching_ids) == 0:
            return
        unique_caches = []
        for server in servers:
            server['time_stamp'] = time_stamp
            server['query_id'] = cell_id
            uq = {
                "last_seen": time_stamp,
                "host": server['host'],
                "vhost": server['vhost'],
                "load": server['load'],
            }
            if 'cell_id' in server:
                if server['cell_id'] not in [c['cell_id'] for c in cm_cache_detail if c['cache']]:
                    uq['first_seen'] = time_stamp
                    logger.info(f"Completely new unseen cache ID found: {server['cell_id']}")
                else:
                    cell_details = cell_id_to_region[server['cell_id']]
                    uq['cell_id'] = server['cell_id']
                    uq['region'] = cell_details['region']
                    uq['code'] = cell_details['code']
                    uq['city'] = cell_details['city']

            if server['host'] not in seen_caches_china:
                server['first_seen'] = time_stamp
                logger.info(f"Completely new unseen china cache host found: {server['host']}")
            unique_caches.append(uq)
        if not unique_caches:
            return
        for c in unique_caches:
            db["cache_hosts_china"].find_one_and_update({"host": c['host']}, {"$set": c}, upsert=True)
    except Exception as e:
        logger.warn(f"{t_id:>2} encountered an error: \n{e}")
        if attempt > 5:
            return
        attempt += 1
        get_china_cache_details(cell_id, t_id, attempt)


def get_cm_details(cell_id, t_id, attempt=0):
    if attempt > 0:
        sleep_timer = random.randint(2, 30)
        logger.info(f"{t_id:>2} Gonna sleep for {sleep_timer:>3} seconds")
        time.sleep(sleep_timer)
    try:
        db = get_database()
        cm_params = params.copy()
        cm_params['cellid'] = cell_id
        resp = get('ISteamDirectory', 'GetCMListForConnect', 1, cm_params)
        servers = resp['response']['serverlist']
        unique_endpoints = []
        for server in servers:

            uq = {
                "last_seen": time_stamp,
                "endpoint": server['endpoint'],
                "dc": server['dc'],
                "realm": server['realm'],
                "type": server['type'],
                "load": server['load'],
            }
            for region in cm_cache_detail:
                if region['cm'] != '' and region['cm'] in server['dc']:
                    uq['region'] = region['region']
                    uq['code'] = region['code']
                    uq['city'] = region['city']
                    break
            if server['endpoint'] not in seen_cms:
                uq['first_seen'] = time_stamp
                logger.info(f"Completely new unseen cm endpoint found: {server['endpoint']}")
            unique_endpoints.append(uq)
        if not unique_endpoints:
            return
        for e in unique_endpoints:
            db["cm_endpoints"].find_one_and_update({"endpoint": e['endpoint']}, {"$set": e}, upsert=True)
    except Exception as e:
        logger.warning(f"{t_id:>2} encountered an error: \n{e}")
        if attempt > 5:
            return
        attempt += 1
        get_cm_details(cell_id, t_id)


def get_cm_china_details(cell_id, t_id, attempt=0):
    if attempt > 0:
        sleep_timer = random.randint(2, 30)
        logger.info(f"{t_id:>2} Gonna sleep for {sleep_timer:>3} seconds")
        time.sleep(sleep_timer)
    try:
        db = get_database()
        cm_params = params.copy()
        cm_params['cellid'] = cell_id
        resp = get('ISteamDirectory', 'GetCMListForConnect', 1, cm_params, china=True)
        servers = resp['response']['serverlist']
        unique_endpoints = []
        for server in servers:

            uq = {
                "last_seen": time_stamp,
                "endpoint": server['endpoint'],
                "dc": server['dc'],
                "realm": server['realm'],
                "type": server['type'],
                "load": server['load'],
            }
            for region in cm_cache_detail:
                if region['cm'] != '' and region['cm'] in server['dc']:
                    uq['region'] = region['region']
                    uq['code'] = region['code']
                    uq['city'] = region['city']
                    break
            if server['endpoint'] not in seen_cms_china:
                uq['first_seen'] = time_stamp
                logger.info(f"Completely new unseen cm endpoint found: {server['endpoint']}")
            unique_endpoints.append(uq)
        if not unique_endpoints:
            return
        for e in unique_endpoints:
            db["cm_endpoints_china"].find_one_and_update({"endpoint": e['endpoint']}, {"$set": e}, upsert=True)
    except Exception as e:
        logger.info(f"{t_id:>2} encountered an error: \n{e}")
        if attempt > 5:
            return
        attempt += 1
        get_cm_china_details(cell_id, t_id, attempt)


def get_database():
    connection_string = MONGO_URI

    client = MongoClient(connection_string)
    return client[DB_NAME]


def get_all_cache_details(cell_ids, t_id):
    for i in cell_ids:
        threads = []
        t1 = Thread(target=get_cache_details, args=(i, t_id))
        threads.append(t1)
        t2 = Thread(target=get_cm_details, args=(i, t_id))
        threads.append(t2)
        t3 = Thread(target=get_china_cache_details, args=(i, t_id))
        threads.append(t3)
        t4 = Thread(target=get_cm_china_details, args=(i, t_id))
        threads.append(t4)
        for t in threads:
            t.start()
        for t in threads:
            t.join()



def get_log_db():
    connection_string = MONGO_URI

    client = MongoClient(connection_string)
    return client[LOG_DB_NAME]


def main():
    start_time = time.time()
    logger.info("Let's start")
    cell_ids = range(1, 500)

    num_threads = 5
    threads = []
    for i in range(num_threads):
        threads.append(
            Thread(target=get_all_cache_details, args=(cell_ids[i::num_threads], i)))
        threads[i].start()
    for t in threads:
        t.join()
    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info('All Done!')
    logger.info(f'Elapsed time: {elapsed_time}')


if __name__ == "__main__":
    load_dotenv()
    logger = logging.getLogger('SteamFindIDs')
    stream_handler = logging.StreamHandler()
    FORMAT = logging.Formatter('[%(asctime)s %(filename)s:%(lineno)s - %(funcName)10s()] %(message)s',
                               datefmt='%Y-%m-%d %H:%M:%S')
    stream_handler.setFormatter(FORMAT)
    logger.addHandler(stream_handler)
    try:
        get_database()
    except (errors.ConnectionFailure, errors.ServerSelectionTimeoutError) as e:
        logger.critical(f'Error connecting to database: {e}')
        os._exit(1)
    if LOG_DB_NAME:
        try:
            log_db = get_log_db()
            log_collection = log_db['logs']
            log_handler = MongoDBHandler(log_collection)
            log_handler.setLevel(LOG_LEVEL)
            logger.addHandler(log_handler)
        except Exception as e:
            logger.error(f'Error getting log database: {e}')
    logger.setLevel(LOG_LEVEL)
    try:
        with open('CellMap.json', 'r') as f:
            cell_id_to_region = json.load(f)
            cell_id_to_region = {int(k): v for k, v in cell_id_to_region.items()}
    except Exception as e:
        logger.critical(f'Error opening cell map: {e}')
        os._exit(1)
    cm_cache_detail = [{
        'cell_id': int(k),
        'cm': v['cm'],
        'cache': v['cache'],
        'code': v['code'],
        'region': v['region'],
        'city': v['city'],
    } for k, v in cell_id_to_region.items()]
    cache_details = {}
    cm_details = {}
    for c in cm_cache_detail:
        if c['cache'] != '':
            cache_details[c['cache']] = c
        if c['cm'] != '':
            cm_details[c['cm']] = c
    db = get_database()
    seen_caches = db["cache_hosts"].distinct("host")
    seen_caches_china = db["cache_hosts_china"].distinct("host")
    seen_cms = db["cm_endpoints"].distinct("endpoint")
    seen_cms_china = db["cm_endpoints_china"].distinct("endpoint")
    main()
