import datetime
import json
import logging
import os
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

load_dotenv()
API_KEY = os.environ.get('STEAM_API_KEY')
MONGO_URI = os.environ.get('MONGO_URI')
DB_NAME = os.environ.get('DB_NAME')
LOG_DB_NAME = os.environ.get('LOG_DB', None)
LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'info').upper())

base_url = 'http://api.steampowered.com/'

params = {
    'format': 'json',
    'key': API_KEY,
}

pub_stats_url = "https://cdn.cloudflare.steamstatic.com/steam/publicstats/"

now = datetime.datetime.now(datetime.UTC)
minut = (now.minute // 10) * 10
rounded_ts = now.replace(minute=minut, second=0, microsecond=0)
time_stamp = rounded_ts

cm_cache_detail = []

cell_id_to_region = {}


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
            "timestamp": record.created,
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
        }


def process_bandwidth_per_region(d):
    try:
        db = get_database()
    except Exception as e:
        logger.error(f'Error getting database: {e}')
        return
    try:
        rname = d.get('label').replace(' ', '_') + ''
        name = d.get('label').replace(' ', '_') + '_bandwidth'
        existing_ts = db[name].distinct('timestamp')
        data = [
            entry for entry in d.get('data')
            if datetime.datetime.fromtimestamp(entry[0] / 1000, tz=datetime.timezone.utc) not in existing_ts
        ]
        entries = []
        for i in data:
            ts = datetime.datetime.fromtimestamp(i[0] / 1000, tz=datetime.timezone.utc)
            bw = i[1]
            entries.append({'timestamp': ts, 'bandwidth': int(bw)})

        db[name].insert_many(entries)
    except Exception as e:
        logger.error(f'Error inserting bandwidth per region for {d.get("label")}: {e}')
        return

    region_ts_global = list(
        db.global_bandwidth.find({
            rname: {
                '$exists': True
            }
        }, {
            'timestamp': 1
        }).distinct('timestamp'))

    for i in d.get('data'):
        try:
            row = {
                'timestamp': datetime.datetime.fromtimestamp(i[0] / 1000, tz=datetime.timezone.utc),
                'bandwidth': int(i[1])
            }

            if row['timestamp'] in region_ts_global:
                continue
            elif db.global_bandwidth.find_one({'timestamp': row['timestamp']}):
                db.global_bandwidth.update_one({'timestamp': row['timestamp']}, {'$set': {rname: row['bandwidth']}})
            else:
                db.global_bandwidth.insert_one({'timestamp': row['timestamp'], rname: row['bandwidth']})
        except Exception as e:
            logger.error(f'Error inserting/updating global bandwidth for {d.get("label")}: {e}')
            continue


def get_contentserver_bandwidth_stacked(date: str, db):
    url = pub_stats_url + "contentserver_bandwidth_stacked.jsonp?v=" + date
    try:
        r = requests.get(url)
        logger.debug(f'Got contentserver bandwidth stacked: {r.text}')
    except Exception as e:
        logger.error(f'Error getting contentserver bandwidth stacked: {e}')
        return
    data = r.text
    form = data.split('(')[1].split(')')[0]
    jj = json.loads(form)

    summary_data = {'Global': {'cur': int(jj.get('current')), 'max': int(jj.get('peak'))}, 'timestamp': time_stamp}

    for a in jj.get('legend'):
        summary_data[a.get('name')] = {'cur': int(a.get('cur')), 'max': int(a.get('max'))}
    logger.debug(f'Got contentserver bandwidth stacked: {summary_data}')
    for d in json.loads(jj.get('json')):
        process_bandwidth_per_region(d)
    try:
        db['bandwidth_summary'].insert_one(summary_data)
    except Exception as e:
        logger.error(f'Error inserting bandwidth summary: {e}')
        return


def get_download_traffic_per_country(date: str, db):
    url = pub_stats_url + "download_traffic_per_country.jsonp?v=" + date
    try:
        r = requests.get(url)
        logger.debug(f'Got download traffic per country: {r.text}')
    except Exception as e:
        logger.error(f'Error getting download traffic per country: {e}')
        return
    try:

        data = r.text
        form = data.split('(')[1].split(')')[0]
        formated_file = json.loads(form)
        countries = []
        for c in formated_file:
            country = {
                'country': c,
                'timestamp': time_stamp,
                'totalbytes': int(formated_file.get(c).get('totalbytes')),
                'avgmbps': float(formated_file.get(c).get('avgmbps'))
            }
            countries.append(country)
    except Exception as e:
        logger.error(f'Error formatting download traffic per country: {e}')
        return
    logger.debug(f'Got download traffic per country: {countries}')
    try:
        db['download_per_country'].insert_many(countries)
        logger.debug(f'Inserted download traffic per country')
    except Exception as e:
        logger.error(f'Error inserting download traffic per country: {e}')
        return


def get_top_asns_per_country(date, db):
    url = pub_stats_url + "top_asns_per_country.jsonp?v=" + date
    try:
        r = requests.get(url)
        logger.debug(f'Got top asns per country: {r.text}')
    except Exception as e:
        logger.error(f'Error getting top asns per country: {e}')
        return

    try:
        raw_text = r.text
        formated_file = json.loads(raw_text.split('onCountryASNData(')[1].split(');')[0])
    except Exception as e:
        logger.error(f'Error formatting top asns per country: {e}')
        return
    countries = []

    for c in formated_file:
        try:
            country = {'name': c, 'timestamp': time_stamp, 'asns': []}
            for asn in formated_file.get(c):
                country['asns'].append({
                    'asname': asn.get('asname'),
                    'totalbytes': int(asn.get('totalbytes')),
                    'avgmbps': float(asn.get('avgmbps'))
                })
            countries.append(country)
        except Exception as e:
            logger.error(f'Error formattinf top asn for country: {e}')
            continue
    logger.debug(f'Formatted asns per country: {countries}')
    try:
        db['top_asns_per_country'].insert_many(countries)
        logger.debug(f'Inserted asns per country')
    except Exception as e:
        logger.error(f'Error inserting top asns per country: {e}')
        return


def get(interface, method, version, params):
    url = base_url + interface + '/' + method + '/v' + str(version) + '/'
    response = requests.get(url, params=params)
    if not response.ok:
        raise Exception(f'Error getting {url}: {response.status_code}')
    logger.debug(f'Got {url}: {response.json()}')
    return response.json()


def get_cache_details(cell_id, db):

    cache_params = params.copy()
    cache_params['cell_id'] = cell_id
    try:
        resp = get('IContentServerDirectoryService', 'GetServersForSteamPipe', 1, cache_params)
    except Exception as e:
        logger.error(f'Error getting cache details for cell {cell_id}: {e}')
        return
    try:
        servers = resp['response']['servers']
    except Exception as e:
        logger.error(f'Error getting servers list for cell {cell_id}: {e}')
        return
    for server in servers:
        try:
            server['time_stamp'] = time_stamp
            server['timestamp'] = time_stamp
            server['query_id'] = cell_id
            if 'cell_id' in server:
                cell_details = cell_id_to_region[server['cell_id']]
                server['region'] = cell_details['region']
                server['code'] = cell_details['code']
                server['city'] = cell_details['city']
        except Exception as e:
            logger.error(f'Error formatting cache details for cell {cell_id}: {e}')
            continue
    try:
        db["cache"].insert_many(servers)
    except Exception as e:
        logger.error(f'Error inserting cache details for cell {cell_id}: {e}')
        return


def get_cm_details(cell_id, db):

    cm_params = params.copy()
    cm_params['cellid'] = cell_id
    try:
        resp = get('ISteamDirectory', 'GetCMListForConnect', 1, cm_params)
    except Exception as e:
        logger.error(f'Error getting cm details for cell {cell_id}: {e}')
        return
    try:
        servers = resp['response']['serverlist']
    except Exception as e:
        logger.error(f'Error getting cm list for cell {cell_id}: {e}')
        return
    entries = []
    for server in servers:
        try:
            entry = {
                "timestamp": time_stamp,
                "metadata": {
                    "type": server["type"],
                    "realm": server["realm"],
                    "dc": server["dc"],
                    "endpoint": server["endpoint"],
                    "legacy_endpoint": server["legacy_endpoint"],
                    "query_id": cell_id
                },
                "load": int(server["load"]),
                "wtd_load": int(server["wtd_load"])
            }

            for region in cm_cache_detail:
                if region['cm'] == entry['metadata']['dc']:
                    entry['metadata']['region'] = region['region']
                    entry['metadata']['code'] = region['code']
                    entry['metadata']['city'] = region['city']
                    break

            entries.append(entry)
        except Exception as e:
            logger.error(f'Error formatting cm details for cell {cell_id}: {e}')
            continue

    try:
        db["cm"].insert_many(entries)
        logger.debug(f'Inserted cm details for cell {cell_id}')
    except Exception as e:
        logger.error(f'Error inserting cm details for cell {cell_id}: {e}')
        return


def get_database():
    connection_string = MONGO_URI

    client = MongoClient(connection_string)
    return client[DB_NAME]


def process_global(db):
    traffic_query = list(db.global_bandwidth.find({'Global': {'$exists': False}}))
    regions = [
        'Africa', 'Asia', 'Central_America', 'Europe', 'Oceania', 'North_America', 'Russia', 'South_America',
        'Middle_East'
    ]
    for row in traffic_query:
        try:
            glob_traffic = 0
            for r in regions:
                glob_traffic += row[r]
            db.global_bandwidth.update_one({'_id': row['_id']}, {'$set': {'Global': glob_traffic}})
            logger.debug(f'Updated global bandwidth for {row["timestamp"]}\tGlobal: {glob_traffic}\n{row}')
        except Exception as e:
            logger.error(f'Error processing global bandwidth: {e}')
            continue


def get_all_cache_details(cell_ids, cache_ids, hour: str, utc_date: str, date: str, db):

    for i in cell_ids:
        get_cm_details(i, db)
    for i in cache_ids:
        get_cache_details(i, db)

    get_top_asns_per_country(utc_date, db)
    get_download_traffic_per_country(utc_date, db)
    get_contentserver_bandwidth_stacked(utc_date, db)
    process_global(db)


def get_log_db():
    connection_string = MONGO_URI

    client = MongoClient(connection_string)
    return client[LOG_DB_NAME]


def main():
    logger.info('Starting cache pull')

    utc_now = datetime.datetime.now(datetime.UTC)
    utc_str = utc_now.strftime("%m-%d-%Y-%H")
    now = datetime.datetime.now()
    db = get_database()
    logger.info(f'Pulling steam data')

    cell_ids = [1, 14, 15, 25, 26, 31, 32, 33, 35, 38, 4, 40, 5, 50, 52, 63, 64, 65, 66, 92, 116, 118, 117, 8]
    cache_ids = [52, 92, 25, 14, 5, 33, 32, 15, 38, 35, 26, 40, 66, 4, 50, 65, 63, 64, 1, 31]

    get_all_cache_details(cell_ids, cache_ids, now.strftime('%H%M'), utc_str, now.strftime('%Y%m%d'), db)

    logger.info(f'Cache pull done')


if __name__ == "__main__":
    load_dotenv()
    logger = logging.getLogger('SteamCachePuller')
    stream_handler = logging.StreamHandler()
    FORMAT = logging.Formatter('[%(asctime)s %(filename)s:%(lineno)s - %(funcName)10s()] %(message)s',
                               datefmt='%Y-%m-%d %H:%M:%S')
    stream_handler.setFormatter(FORMAT)
    logger.addHandler(stream_handler)
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
        'region': v['region']
    } for k, v in cell_id_to_region.items() if v['cm']]
    main()
