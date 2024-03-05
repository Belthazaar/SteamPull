"""Pull data from Steam API and store it in MongoDB."""
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
    'max_servers': 200
}

STATS_URL = "https://cdn.cloudflare.steamstatic.com/steam/publicstats/"

start = datetime.datetime.now(datetime.UTC)
minut = (start.minute // 10) * 10
rounded_ts = start.replace(minute=minut, second=0, microsecond=0)
time_stamp = rounded_ts

cm_cache_detail: list[dict] = []

cell_id_to_region: dict[int, dict] = {}


class MongoDBHandler(logging.Handler):
    """
    A custom logging handler that sends log records to MongoDB.

    Args:
    ----
        collection (pymongo.collection.Collection): The MongoDB collection to insert log records

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


def process_bandwidth_per_region(data, logger):
    """
    Process bandwidth data per region and store it in the database.

    Args:
    ----
        d (dict): The raw bandwidth data for a region.
        logger: The logger object for logging errors.

    Returns
    -------
        None

    """
    try:
        db = get_database()
        rname = data.get('label').replace(' ', '_') + ''
        name = data.get('label').replace(' ', '_') + '_bandwidth'
        existing_ts = db[name].distinct('timestamp')
        data_arr = [
            entry for entry in data.get('data')
            if datetime.datetime.fromtimestamp(entry[0] / 1000, tz=datetime.timezone.utc) not in existing_ts
        ]
        entries = []
        for i in data_arr:
            entries.append({
                'timestamp': datetime.datetime.fromtimestamp(i[0] / 1000, tz=datetime.timezone.utc),
                'bandwidth': int(i[1])
            })

    except errors.PyMongoError as err:
        logger.error(f'Error getting database: {err}')
        return
    except (ValueError, KeyError) as err:
        logger.error(f'Error processing bandwidth per region: {err}')
        return
    insert_many(db, name, entries, logger, f'bandwidth per region for {rname}')
    process_global_bandwidth(data, rname, logger, db)


def process_global_bandwidth(data, rname, logger, db):
    """
    Process the global bandwidth data and update the database with the new values.

    Args:
    ----
        data (dict): The data containing the global bandwidth information.
        rname (str): The name of the region.
        logger: The logger object for logging messages.
        db: The database object for accessing the database.

    Returns
    -------
        None

    """
    region_ts_global = list(
        db.global_bandwidth.find({
            rname: {
                '$exists': True
            }
        }, {
            'timestamp': 1
        }).distinct('timestamp'))

    for i in data.get('data'):
        try:
            row = {
                'timestamp': datetime.datetime.fromtimestamp(i[0] / 1000, tz=datetime.timezone.utc),
                'bandwidth': int(i[1])
            }

            if row['timestamp'] in region_ts_global:
                continue
            if db.global_bandwidth.find_one({'timestamp': row['timestamp']}):
                db.global_bandwidth.update_one({'timestamp': row['timestamp']}, {'$set': {rname: row['bandwidth']}})
            else:
                db.global_bandwidth.insert_one({'timestamp': row['timestamp'], rname: row['bandwidth']})
        except errors.DuplicateKeyError:
            logger.debug('Race condition found inserting global bandwidth for %s at %s', rname, row['timestamp'])
            db.global_bandwidth.update_one({'timestamp': row['timestamp']}, {'$set': {rname: row['bandwidth']}})
            continue


def get_contentserver_bandwidth_stacked(date: str, db, logger):
    """
    Retrieves content server bandwidth data for a specific date and stores it in the database.

    Args:
    ----
        date (str): The date for which to retrieve the bandwidth data.
        db: The database object to store the data.
        logger: The logger object for logging debug and error messages.

    Returns
    -------
        None

    """
    url = STATS_URL + "contentserver_bandwidth_stacked.jsonp?v=" + date
    try:
        response = requests.get(url, timeout=60)
        logger.debug(f'Got contentserver bandwidth stacked: {response.text}')
    except requests.exceptions.RequestException as err:
        logger.error('Error getting contentserver bandwidth stacked: %s', err)
        return
    data = response.text
    form = data.split('(')[1].split(')')[0]
    json_data = json.loads(form)

    summary_data = {
        'Global': {
            'cur': int(json_data.get('current')),
            'max': int(json_data.get('peak'))
        },
        'timestamp': time_stamp
    }

    for region in json_data.get('legend'):
        summary_data[region.get('name')] = {'cur': int(region.get('cur')), 'max': int(region.get('max'))}
    logger.debug(f'Got contentserver bandwidth stacked: {summary_data}')
    threads = []
    for json_data in json.loads(json_data.get('json')):
        thread = Thread(target=process_bandwidth_per_region, args=(json_data, logger))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    try:
        db['bandwidth_summary'].insert_one(summary_data)
    except errors.DuplicateKeyError:
        logger.debug('Dups found inserting bandwidth summary')


def get_download_traffic_per_country(date: str, db, logger):
    """
    Retrieves the download traffic per country for a given date and stores it in the database.

    Args:
    ----
        date (str): The date for which to retrieve the download traffic per country.
        db: The database connection object.
        logger: The logger object for logging.

    Returns
    -------
        None

    """
    url = STATS_URL + "download_traffic_per_country.jsonp?v=" + date
    try:
        response = requests.get(url, timeout=60)
        logger.debug(f'Got download traffic per country: {response.text}')
    except requests.exceptions.RequestException as err:
        logger.error(f'Error getting download traffic per country: {err}')
        return
    try:
        data = response.text
        form = data.split('(')[1].split(')')[0]
        formated_file = json.loads(form)
        countries = []
        for country_data in formated_file:
            country = {
                'country': country_data,
                'timestamp': time_stamp,
                'totalbytes': int(formated_file.get(country_data).get('totalbytes')),
                'avgmbps': float(formated_file.get(country_data).get('avgmbps'))
            }
            countries.append(country)
    except ValueError as err:
        logger.error(f'Error formatting download traffic per country: {err}')
        return
    except TypeError as err:
        logger.error(f'Error formatting download traffic per country: {err}')
        return
    logger.debug(f'Got download traffic per country: {countries}')
    insert_many(db, 'download_per_country', countries, logger, 'download traffic per country')


def get_top_asns_per_country(date, db, logger):
    """
    Retrieves the top ASNs (Autonomous System Numbers) per country for a given date.

    Args:
    ----
        date (str): The date for which to retrieve the top ASNs per country.
        db: The database object used for inserting the retrieved data.
        logger: The logger object used for logging errors and debug information.

    Returns
    -------
        None

    """
    url = STATS_URL + "top_asns_per_country.jsonp?v=" + date
    try:
        response = requests.get(url, timeout=60)
        logger.debug(f'Got top asns per country: {response.text}')
        raw_text = response.text
        formated_file = json.loads(raw_text.split('onCountryASNData(')[1].split(');')[0])
    except requests.exceptions.RequestException as err:
        logger.error(f'Error getting top asns per country: {err}')
        return
    except json.JSONDecodeError as err:
        logger.error(f'Error formatting top asns per country: {err}')
        return
    countries = []

    for c_data in formated_file:
        try:
            country = {'name': c_data, 'timestamp': time_stamp, 'asns': []}
            for asn in formated_file.get(c_data):
                country['asns'].append({
                    'asname': asn.get('asname'),
                    'totalbytes': int(asn.get('totalbytes')),
                    'avgmbps': float(asn.get('avgmbps'))
                })
            countries.append(country)
        except ValueError as err:
            logger.error(f'Error formatting top asn for country: {err}')
            continue
    logger.debug(f'Formatted asns per country: {countries}')
    insert_many(db, 'top_asns_per_country', countries, logger, 'top asns per country')


def get(interface, method, version, param, logger):
    """
    Sends a GET request to the specified interface, method, and version with the given parameters.

    Args:
    ----
        interface (str): The interface to send the request to.
        method (str): The method to call on the interface.
        version (int): The version of the method to call.
        param (dict): The parameters to include in the request.
        logger: The logger object to use for logging.

    Returns
    -------
        dict: The JSON response from the request.

    Raises
    ------
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
    ----
        cell_id (int): The ID of the cell.
        db: The database object.
        logger: The logger object.

    Returns
    -------
        None

    """
    cache_params = params.copy()
    cache_params['cell_id'] = cell_id
    try:
        resp = get('IContentServerDirectoryService', 'GetServersForSteamPipe', 1, cache_params, logger)
        servers = resp['response']['servers']
    except ConnectionError as err:
        logger.error(f'Error getting cache details for cell {cell_id}: {err}')
        return
    except (ValueError, KeyError) as err:
        logger.error(f'Error getting servers list for cell {cell_id}: {err}')
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
        except (ValueError, KeyError) as err:
            logger.error(f'Error formatting cache details for cell {cell_id}: {err}')
            continue
    insert_many(db, "cache", servers, logger, f'cell {cell_id}')


def get_cm_details(cell_id, db, logger):
    """
    Retrieves CM details for a given cell ID and inserts them into the database.

    Args:
    ----
        cell_id (str): The ID of the cell.
        db (Database): The database object.
        logger (Logger): The logger object.

    Returns
    -------
        None

    """
    cm_params = params.copy()
    cm_params['cellid'] = cell_id
    try:
        resp = get('ISteamDirectory', 'GetCMListForConnect', 1, cm_params, logger)
        servers = resp['response']['serverlist']
    except ConnectionError as err:
        logger.error(f'Error getting cm details for cell {cell_id}: {err}')
        return
    except (ValueError, KeyError) as err:
        logger.error(f'Error getting cm list for cell {cell_id}: {err}')
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
        except (ValueError, KeyError) as err:
            logger.error(f'Error formatting cm details for cell {cell_id}: {err}')
            continue
    insert_many(db, "cm", entries, logger, f'cell {cell_id}')


def insert_many(db, collection, data, logger, err_msg):
    """
    Insert multiple documents into a collection in the database.

    Args:
    ----
        db (pymongo.database.Database): The database object.
        collection (str): The name of the collection to insert the documents into.
        data (list): A list of documents to insert.
        logger (logging.Logger): The logger object for logging messages.
        err_msg (str): The error message to be logged.

    Returns
    -------
        None

    """
    try:
        db[collection].insert_many(data, ordered=False, bypass_document_validation=True)
        logger.debug('Inserted data for %s', err_msg)
    except errors.BulkWriteError as err:
        logger.debug('Dups found inserting data for %s', err_msg)
        panic_list = list(filter(lambda x: x['code'] != 11000, err.details['writeErrors']))
        if len(panic_list) > 0:
            logger.error(f"these are not duplicate errors {panic_list}")
            return


def get_database():
    """
    Returns the MongoDB database object.

    Returns
    -------
        MongoDB database object.

    """
    connection_string = MONGO_URI

    client = MongoClient(connection_string)
    return client[DB_NAME]


def process_global(db, logger):
    """
    Process global bandwidth data for each row in the database.

    Args:
    ----
        db: The database object.
        logger: The logger object.

    Returns
    -------
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
            for region in regions:
                glob_traffic += row[region]
            db.global_bandwidth.update_one({'_id': row['_id']}, {'$set': {'Global': glob_traffic}})
            logger.debug(f'Updated global bandwidth for {row["timestamp"]}\tGlobal: {glob_traffic}\n{row}')
        except (errors.WriteError, KeyError, ValueError) as err:
            logger.error(f'Error processing global bandwidth: {err}')
            continue


def get_all_cache_details(cell_ids, cache_ids, utc_date: str, db, logger):
    """
    Retrieves cache details for all specified cell IDs and cache IDs.

    Args:
    ----
        cell_ids (list): List of cell IDs.
        cache_ids (list): List of cache IDs.
        utc_date (str): UTC date in string format.
        db: Database connection object.
        logger: Logger object.

    Returns
    -------
        None

    """
    threads = []
    for i in cell_ids:
        thread = Thread(target=get_cm_details, args=(i, db, logger))
        thread.start()
        threads.append(thread)

    for i in cache_ids:
        thread = Thread(target=get_cache_details, args=(i, db, logger))
        thread.start()

    asn_t = Thread(target=get_top_asns_per_country, args=(utc_date, db, logger))
    asn_t.start()
    get_download_traffic_per_country(utc_date, db, logger)
    content_t = Thread(target=get_download_traffic_per_country, args=(utc_date, db, logger))
    content_t.start()
    get_contentserver_bandwidth_stacked(utc_date, db, logger)
    for thread in threads:
        thread.join()
    process_global(db, logger)


def get_log_db():
    """
    Returns the MongoDB database object for logging.

    Returns
    -------
        MongoDB database object

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
        except errors.ConnectionFailure as err:
            logger.error('Error connecting to log database: %s', err)
        except errors.OperationFailure as err:
            logger.error('Error performing operation on log database: %s', err)
    logger.setLevel(LOG_LEVEL)
    try:
        with open('CellMap.json', 'r', encoding='utf-8') as file:
            cid_to_region = json.load(file)
            global cell_id_to_region  # pylint: disable=global-statement
            cell_id_to_region = {int(k): v for k, v in cid_to_region.items()}
    except json.JSONDecodeError as err:
        logger.critical('Error opening cell map: %s', err)
        os._exit(1)
    # pylint: disable=global-statement
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

    cell_ids = [k for k, v in cell_id_to_region.items() if v['cm'] != '']

    cache_ids = [k for k, v in cell_id_to_region.items() if v['cache'] != '']

    get_all_cache_details(cell_ids, cache_ids, utc_str, db, logger)

    logger.info('Cache pull done')


if __name__ == "__main__":

    main()
