import requests as rq
import json
import re
from mysql.connector import MySQLConnection, Error
import datetime as dt
import logging
import sys
from configparser import ConfigParser



def read_db_config(filename='dbconfig.ini', section='mysql'):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to mysql
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return db


def fetch_data():
    """
    Method that calls the USGS API and stores the result in designated MYSQL db
    :return: None
    """

    db_config = read_db_config()
    conn = MySQLConnection(**db_config)
    mycursor = conn.cursor()

    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    fmt = '%Y-%m-%d'

    # Query per week as the request limit is at 20.000 -- we can reasonably assume that the
    # #earthquakes per week will not exceed this number. Checked through experimentation.
    # If we exceed limit, we could reduce request size
    for i in range((365 // 7) + 1):
        start = dt.datetime(2017, 1, 1) + dt.timedelta(days=i*7)
        end = min(start + dt.timedelta(days=7), dt.datetime(2018, 1, 1))

        params = {
            "starttime": start.strftime(fmt),
            "endtime": end.strftime(fmt),
            "format": 'geojson'
        }

        logging.info(f"------------------------- Querying for {start.strftime(fmt)} ------------------------------------")

        # Incrementally fetch data and ingest into db
        with rq.get(url, params=params, stream=True) as r:
            if r.status_code == 200:
                header = True

                rowcount = 0
                succount = 0

                # Iterate over lines so that not all data is read into memory
                for chunk in r.iter_lines(chunk_size=1, decode_unicode=True):
                    rows = []

                    # First row has some non-necessary fields, handle differently
                    if header:
                        data = json.loads(chunk.rstrip(",") + "]}")
                        meta = data["metadata"]
                        logging.info(f"Data Title: {meta['title']}, number of fetched rows = {meta['count']}")
                        rows = data["features"]

                        header = False
                    else:
                        # Process for body of json
                        try:
                            row = json.loads(chunk.rstrip(","))
                            rows.append(row)

                        # Last row of json, cut off bbox data
                        except json.JSONDecodeError as e:
                            if 'Extra data' in e.msg:
                                row = json.loads(re.split(r"(\],\"bbox)", chunk)[0])
                                rows.append(row)

                    # Usually rows only contains one row, but allow for deviations
                    for row in rows:
                        try:
                            # Ingest row in db
                            ingest_row(row, mycursor, rowcount)
                            succount += 1

                        except Error as e:
                            logging.debug(f"Row {rowcount} - " + e.msg)

                        rowcount += 1

                    # Push changes to db
                    conn.commit()
                logging.info(f"Successfully ingested {succount} rows")

            else:
                logging.debug(f"Failed request, status_code = {r.status_code}, reason = {r.reason}")


def ingest_row(row: dict, cursor: MySQLConnection, counter: int = 0) -> None :
    """
    Method that unpacks a json object with earthquake event data and ingests info into MySQL db
    :param row: json object (dict) with earthquake data, following USGS geojson format (see https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)
    :param cursor: mysql db cursor. Needed to execute queries.
    :param counter: rowcount. Used for logging
    :return: None
    """

    dat = row['properties']

    # Translate Unix time from data to string format
    ts = dt.datetime.fromtimestamp(row['properties'].get('time', 0) / 1000).strftime(format='%Y-%m-%d %H:%m:%d ')
    update_time = dt.datetime.fromtimestamp(row['properties'].get('updated', 0) / 1000).strftime(format='%Y-%m-%d %H:%m:%d ')

    # Get coordinates if sent (not always the case)
    coords = row['geometry'].get('coordinates', ['NULL', 'NULL', 'NULL'])

    if not 'id' in row:
        logging.debug(f"Row {counter} has no id, moving on...")
        return

    id = row['id']

    # # Insert row in event table
    sql = "INSERT INTO event(id, source_code, title, timestamp, update_time, lat, lon, place_description, depth, magnitude, " \
          "magnitude_type, nr_stations, max_gap, dist_to_epi, rms_arrival_time, status, report_net,url) " \
          "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    fields = (id,dat['code'],dat['title'],ts,update_time,coords[0],coords[1],dat['place'],
          coords[2],dat['mag'],dat['magType'],dat['nst'],dat['gap'],dat['dmin'],dat['rms'],
          dat['status'],dat['net'],dat['url'])
    cursor.execute(sql, fields)

    # Insert rows in event association table
    associations = {x for x in dat.get('ids', id).split(",") if len(x) > 0 and x != id}
    for ass_id in associations:
        sql = f"INSERT INTO associated_events(source_id, target_id) VALUES (%s, %s)"
        cursor.execute(sql, (id, ass_id))

    # Insert rows in contributor table
    cont = {x for x in dat.get('sources', '').split(",") if len(x) > 0}
    for cont_id in cont:
        sql = f"INSERT INTO contributed(event_id, contributor_name) VALUES (%s, %s)"
        cursor.execute(sql, (id, cont_id))

    logging.debug(f"Row {counter} - Successfully added event with id {id}")



if __name__ == '__main__':
    # Setup logging
    levels = [logging.INFO, logging.DEBUG, logging.ERROR]

    level = 0
    if len(sys.argv) > 1:
        level = int(sys.argv[1])

    logging.basicConfig(
        # filename='debug.log',
                        level=levels[level],
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        handlers=[
                            logging.FileHandler("debug.log"),
                            logging.StreamHandler(sys.stdout)
                        ]
                        )

    # Run main ingest method
    fetch_data()

