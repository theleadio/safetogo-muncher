"""
    Created by: Nazmi Asri
    Email: nazmiasri95@gmail.com
"""

import re
import json
import requests
import pandas as pd
import traceback
import logging


from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from .services.sql_runner import SqlRunner


# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
# logging.getLogger('sqlalchemy.dialects').setLevel(logging.DEBUG)
#
# logging.getLogger('sqlalchemy.orm').setLevel(logging.DEBUG)
# logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)


def map_f(d):
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    site = d["properties"]["site"]
    if site.startswith('pic/'):
        site = f"http://www.redangpow.com/covid-19/{site}"

    source = d["properties"]["source"]
    if source.startswith('pic/'):
        source = f"http://www.redangpow.com/covid-19/{source}"

    j_data = {}
    j_data["lat"] = d["properties"]["latitude"]
    j_data["lng"] = d["properties"]["longitude"]
    j_data["description"] = d["properties"]["description"]
    j_data["locationName"] = d["properties"]["locationName"]
    j_data["caseNo"] = None
    j_data["source"] = source
    j_data["reportedDate"] = d["properties"]["reportedDate"]
    j_data["site"] = site
    j_data["text_show"] = d["properties"]["show"]
    j_data["createdBy"] = "Nazmi Asri"
    j_data["img_url"] = "https://lh3.googleusercontent.com/a-/AOh14GhAduy8fsb2SvVs72Ro0MrGI5FmKerv1Ge9iRD5iA"
    j_data["email"] = "nazmiasri95@gmail.com"
    j_data["upvote"] = 0
    j_data["downvote"] = 0
    j_data["last_updated"] = datetime.now()
    j_data["scrapped"] = 1
    j_data["outlier"] = 0 if re.match(regex, site) is not None else 1
    return j_data


def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None, ):
    """
    Requests with retry
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


if __name__ == "__main__":
    URL_REDANGPOW = "http://redangpow.com/covid-19/data/map.geojson"
    HEADERS = {
        'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 ' +
            '(KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
    }

    DB_HOST = "safetogo.caapwlyn1xci.ap-southeast-1.rds.amazonaws.com"
    DB_USER = ""  # Please remove when push to Git
    DB_PASSWORD = ""  # Please remove when push to Git
    DB_DATABASE = "safetogo_dev"
    DB_TABLE = "redangpow_markers"
    DB_URI = 'mysql://{}@{}:3306/{}'.format(DB_USER, DB_HOST, DB_DATABASE)

    # Creating session
    s = requests.Session()
    s.headers.update(HEADERS)
    r = requests_retry_session(session=s).get(URL_REDANGPOW)

    if not r.content:
        raise Exception(f"URL response content is not JSON !")
    raw_d = r.json()["features"]

    processed_d = map(map_f, raw_d)
    df = pd.DataFrame(processed_d)
    df["last_updated"] = pd.to_datetime(df["last_updated"]).values.astype('datetime64[ms]')
    df["reportedDate"] = pd.to_datetime(df["reportedDate"]).values.astype('datetime64[ms]')

    # Save data
    runner = SqlRunner(DB_URI, DB_PASSWORD)
    runner.to_sql(df, DB_TABLE, chunksize=100)
