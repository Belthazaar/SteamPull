'''
Pull data from Steam API and store it in MongoDB
'''
import datetime
import json
import logging
import os
from threading import Thread
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, errors
from pymongo.collection import Collection

load_dotenv()
API_KEY = os.environ.get('STEAM_API_KEY')
MONGO_URI = os.environ.get('MONGO_URI')
DB_NAME = os.environ.get('DB_NAME')
LOG_DB_NAME = os.environ.get('LOG_DB_NAME', None)
LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'info').upper())

BASE_URL = 'http://api.steampowered.com/'

params = {
    'format': 'json',
    'key': API_KEY,
}

STATS_URL = "https://cdn.cloudflare.steamstatic.com/steam/publicstats/"

start = datetime.datetime.now(datetime.UTC)
minut = (start.minute // 10) * 10
rounded_ts = start.replace(minute=minut, second=0, microsecond=0)
time_stamp = rounded_ts

cm_cache_detail = []

cell_id_to_region = {}


class MongoDBHandler(logging.Handler):
    """
    A custom logging handler that sends log records to MongoDB.

    Args:
        collection (pymongo.collection.Collection): The MongoDB collection to insert log records into.
    """

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


def process_bandwidth_per_region(d, logger):
    """
    Process bandwidth data per region and store it in the database.

    Args:
        d (dict): The raw bandwidth data for a region.
        logger: The logger object for logging errors.

    Returns:
        None
    """
    try:
        db = get_database()
    except errors.PyMongoError as ex:
        logger.error(f'Error getting database: {ex}')
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
            entries.append({
                'timestamp': datetime.datetime.fromtimestamp(i[0] / 1000, tz=datetime.timezone.utc),
                'bandwidth': int(i[1])
            })

        db[name].insert_many(entries, ordered=False, bypass_document_validation=True)
    except errors.BulkWriteError as err:
        logger.debug(f'Dups found inserting bandwidth per region for {d.get("label")}')
        panic_list = list(filter(lambda x: x['code'] != 11000, err.details['writeErrors']))
        if len(panic_list) > 0:
            logger.error(f"these are not duplicate errors {panic_list}")
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
        except errors.DuplicateKeyError:
            db.global_bandwidth.update_one({'timestamp': row['timestamp']}, {'$set': {rname: row['bandwidth']}})
            continue


def get_contentserver_bandwidth_stacked(date: str, db, logger):
    """
    Retrieves content server bandwidth data for a specific date and stores it in the database.

    Args:
        date (str): The date for which to retrieve the bandwidth data.
        db: The database object to store the data.
        logger: The logger object for logging debug and error messages.

    Returns:
        None
    """
    url = STATS_URL + "contentserver_bandwidth_stacked.jsonp?v=" + date
    try:
        r = requests.get(url, timeout=60) 
        logger.debug(f'Got contentserver bandwidth stacked: {r.text}')
    except requests.exceptions.RequestException as e:
        logger.error(f'Error getting contentserver bandwidth stacked: {e}')
        return
    data = r.text
    form = data.split('(')[1].split(')')[0]
    jj = json.loads(form)

    summary_data = {'Global': {'cur': int(jj.get('current')), 'max': int(jj.get('peak'))}, 'timestamp': time_stamp}

    for a in jj.get('legend'):
        summary_data[a.get('name')] = {'cur': int(a.get('cur')), 'max': int(a.get('max'))}
    logger.debug(f'Got contentserver bandwidth stacked: {summary_data}')
    threads = []
    for d in json.loads(jj.get('json')):
        t = Thread(target=process_bandwidth_per_region, args=(d, logger))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    try:
        db['bandwidth_summary'].insert_one(summary_data)
    except errors.DuplicateKeyError:
        logger.debug('Dups found inserting bandwidth summary')


def get_download_traffic_per_country(date: str, db, logger):
    """
    Retrieves the download traffic per country for a given date and stores it in the database.

    Args:
        date (str): The date for which to retrieve the download traffic per country.
        db: The database connection object.
        logger: The logger object for logging.

    Returns:
        None
    """
    url = STATS_URL + "download_traffic_per_country.jsonp?v=" + date
    try:
        r = requests.get(url, timeout=60)
        logger.debug(f'Got download traffic per country: {r.text}')
    except requests.exceptions.RequestException as e:
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
    except ValueError as e:
        logger.error(f'Error formatting download traffic per country: {e}')
        return
    except TypeError as e:
        logger.error(f'Error formatting download traffic per country: {e}')
        return
    logger.debug(f'Got download traffic per country: {countries}')
    try:
        db['download_per_country'].insert_many(countries, ordered=False, bypass_document_validation=True)
        logger.debug('Inserted download traffic per country')
    except errors.BulkWriteError as e:
        logger.debug('Dups found inserting download traffic per country')
        panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
        if len(panic_list) > 0:
            logger.error(f"these are not duplicate errors {panic_list}")
            return


def get_top_asns_per_country(date, db, logger):
    """
    Retrieves the top ASNs (Autonomous System Numbers) per country for a given date.

    Args:
        date (str): The date for which to retrieve the top ASNs per country.
        db: The database object used for inserting the retrieved data.
        logger: The logger object used for logging errors and debug information.

    Returns:
        None
    """
    url = STATS_URL + "top_asns_per_country.jsonp?v=" + date
    try:
        r = requests.get(url, timeout=60)
        logger.debug(f'Got top asns per country: {r.text}')
    except requests.exceptions.RequestException as e:
        logger.error(f'Error getting top asns per country: {e}')
        return
    except json.JSONDecodeError as e:
        logger.error(f'Error formatting top asns per country: {e}')
        return
    try:
        raw_text = r.text
        formated_file = json.loads(raw_text.split('onCountryASNData(')[1].split(');')[0])
    except json.JSONDecodeError as e:
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
        except ValueError as e:
            logger.error(f'Error formatting top asn for country: {e}')
            continue
    logger.debug(f'Formatted asns per country: {countries}')
    try:
        db['top_asns_per_country'].insert_many(countries, ordered=False, bypass_document_validation=True)
        logger.debug('Inserted asns per country')
    except errors.BulkWriteError as e:
        logger.debug('Dups found inserting top asns per country')
        panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
        if len(panic_list) > 0:
            logger.error(f"these are not duplicate errors {panic_list}")
            return


def get(interface, method, version, param, logger):
    """
    Sends a GET request to the specified interface, method, and version with the given parameters.

    Args:
        interface (str): The interface to send the request to.
        method (str): The method to call on the interface.
        version (int): The version of the method to call.
        param (dict): The parameters to include in the request.
        logger: The logger object to use for logging.

    Returns:
        dict: The JSON response from the request.

    Raises:
        ConnectionError: If the request fails with a non-OK status code.
    """
    url = BASE_URL + interface + '/' + method + '/v' + str(version) + '/'
    response = requests.get(url, params=param, timeout=60)
    if not response.ok:
        raise ConnectionError(f'Error getting {url}: {response.status_code}')
    logger.debug(f'Got {url}: {response.json()}')
    return response.json()


def get_cache_details(cell_id, db, logger):
    """
    Retrieves cache details for a given cell ID and inserts them into the database.

    Args:
        cell_id (int): The ID of the cell.
        db: The database object.
        logger: The logger object.

    Returns:
        None
    """
    cache_params = params.copy()
    cache_params['cell_id'] = cell_id
    try:
        resp = get('IContentServerDirectoryService', 'GetServersForSteamPipe', 1, cache_params, logger)
    except ConnectionError as e:
        logger.error(f'Error getting cache details for cell {cell_id}: {e}')
        return
    try:
        servers = resp['response']['servers']
    except (ValueError, KeyError) as e:
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
        except (ValueError, KeyError) as e:
            logger.error(f'Error formatting cache details for cell {cell_id}: {e}')
            continue
    try:
        db["cache"].insert_many(servers, ordered=False, bypass_document_validation=True)
    except errors.BulkWriteError as e:
        logger.debug(f'Dups found inserting cache details for cell {cell_id}')
        panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
        if len(panic_list) > 0:
            logger.error(f"these are not duplicate errors {panic_list}")
            return


def get_cm_details(cell_id, db, logger):
    """
    Retrieves CM details for a given cell ID and inserts them into the database.

    Args:
        cell_id (str): The ID of the cell.
        db (Database): The database object.
        logger (Logger): The logger object.

    Returns:
        None
    """
    cm_params = params.copy()
    cm_params['cellid'] = cell_id
    try:
        resp = get('ISteamDirectory', 'GetCMListForConnect', 1, cm_params, logger)
    except ConnectionError as e:
        logger.error(f'Error getting cm details for cell {cell_id}: {e}')
        return
    try:
        servers = resp['response']['serverlist']
    except (ValueError, KeyError) as e:
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
                if region['cm'] != '' and region['cm'] in ['dc']:
                    entry['metadata']['region'] = region['region']
                    entry['metadata']['code'] = region['code']
                    entry['metadata']['city'] = region['city']
                    break

            entries.append(entry)
        except (ValueError, KeyError) as e:
            logger.error(f'Error formatting cm details for cell {cell_id}: {e}')
            continue

    try:
        db["cm"].insert_many(entries, ordered=False, bypass_document_validation=True)
        logger.debug(f'Inserted cm details for cell {cell_id}')
    except errors.BulkWriteError as e:
        logger.debug(f'Dups found inserting cm details for cell {cell_id}')
        panic_list = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
        if len(panic_list) > 0:
            logger.error(f"these are not duplicate errors {panic_list}")
            return


def get_database():
    """
    Returns the MongoDB database object.

    :return: MongoDB database object.
    """
    connection_string = MONGO_URI

    client = MongoClient(connection_string)
    return client[DB_NAME]


def process_global(db, logger):
    """
    Process global bandwidth data for each row in the database.

    Args:
        db: The database object.
        logger: The logger object.

    Returns:
        None
    """
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
        except (errors.WriteError, KeyError, ValueError) as e:
            logger.error(f'Error processing global bandwidth: {e}')
            continue


def get_all_cache_details(cell_ids, cache_ids, utc_date: str, db, logger):
    """
    Retrieves cache details for all specified cell IDs and cache IDs.

    Args:
        cell_ids (list): List of cell IDs.
        cache_ids (list): List of cache IDs.
        utc_date (str): UTC date in string format.
        db: Database connection object.
        logger: Logger object.

    Returns:
        None
    """

    threads = []
    for i in cell_ids:
        t = Thread(target=get_cm_details, args=(i, db, logger))
        t.start()
        threads.append(t)

    for i in cache_ids:
        t = Thread(target=get_cache_details, args=(i, db, logger))
        t.start()

    asn_t = Thread(target=get_top_asns_per_country, args=(utc_date, db, logger))
    asn_t.start()
    get_download_traffic_per_country(utc_date, db, logger)
    content_t = Thread(target=get_download_traffic_per_country, args=(utc_date, db, logger))
    content_t.start()
    get_contentserver_bandwidth_stacked(utc_date, db, logger)
    for t in threads:
        t.join()
    process_global(db, logger)


def get_log_db():
    """
    Returns the MongoDB database object for logging.

    :return: MongoDB database object
    """
    connection_string = MONGO_URI

    client = MongoClient(connection_string)
    return client[LOG_DB_NAME]


def main():
    """
    Entry point of the application.
    Loads environment variables, initializes logging, and performs cache pull.
    """
    load_dotenv()
    logger = logging.getLogger('SteamCachePuller')
    stream_handler = logging.StreamHandler()
    fmt = logging.Formatter('[%(asctime)s %(filename)s:%(lineno)s - %(funcName)10s()] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)
    if LOG_DB_NAME:
        try:
            log_db = get_log_db()
            log_collection = log_db['logs']
            log_handler = MongoDBHandler(log_collection)
            log_handler.setLevel(LOG_LEVEL)
            logger.addHandler(log_handler)
        except errors.ConnectionFailure as e:
            logger.error(f'Error connecting to log database: {e}')
        except errors.OperationFailure as e:
            logger.error(f'Error performing operation on log database: {e}')
    logger.setLevel(LOG_LEVEL)
    try:
        with open('CellMap.json', 'r', encoding='utf-8') as f:
            cid_to_region = json.load(f)
            global cell_id_to_region #pylint: disable=global-statement
            cell_id_to_region = {int(k): v for k, v in cid_to_region.items()}  #pylint: disable=redefined-outer-name
    except json.JSONDecodeError as e:
        logger.critical(f'Error opening cell map: {e}')
        os._exit(1)
    #pylint: disable=unused-variable, redefined-outer-name, global-statement
    global cm_cache_detail
    cm_cache_detail = [{
        'cell_id': int(k),
        'cm': v['cm'],
        'cache': v['cache'],
        'code': v['code'],
        'region': v['region'],
        'city': v['city']
    } for k, v in cell_id_to_region.items()]
    logger.info('Starting cache pull')

    utc_now = datetime.datetime.now(datetime.UTC)
    utc_str = utc_now.strftime("%m-%d-%Y-%H")
    db = get_database()
    logger.info('Pulling steam data')

    cell_ids = [1, 14, 15, 25, 26, 31, 32, 33, 35, 38, 4, 40, 5, 50, 52, 63, 64, 65, 66, 92, 116, 118, 117, 8]
    cache_ids = [52, 92, 25, 14, 5, 33, 32, 15, 38, 35, 26, 40, 66, 4, 50, 65, 63, 64, 1, 31]

    get_all_cache_details(cell_ids, cache_ids, utc_str, db, logger)

    logger.info('Cache pull done')


if __name__ == "__main__":

    main()
