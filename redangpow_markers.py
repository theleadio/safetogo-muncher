import re
import json
import requests
import pandas as pd
import traceback
import logging

from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from sqlalchemy.engine.url import make_url, URL

from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


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


class SqlRunner:

    def __init__(self, uri, password):
        self.uri = uri
        self.password = password

    @staticmethod
    def get_connection_url(db_uri, password):
        url = make_url(db_uri)
        query = {"charset": "utf8mb4"}
        connection_url = URL(url.drivername, url.username, password, url.host, url.port, url.database, query)
        return connection_url

    def get_sql_engine(self):
        connection_url = SqlRunner.get_connection_url(self.uri, self.password)
        return create_engine(connection_url, echo=False, execution_options=dict(stream_results=True),
                             server_side_cursors=True, encoding="utf-8")

    def run_query(self, sql_query, result_size):
        print("Running Query => ", sql_query)

        engine = self.get_sql_engine()
        query_results = pd.read_sql_query(sql_query, engine, chunksize=result_size)
        return query_results

    @staticmethod
    def pgsql_upsert(table, conn, keys, data_iter):

        @compiles(Insert, 'postgresql')
        def prefix_inserts(insert, compiler, **kw):
            str_val = compiler.visit_insert(insert, **kw)
            fields = str_val[str_val.find("(") + 1:str_val.find(")")].replace(" ", "").split(",")
            generated_directive = ["{0}=%({0})s".format(field) for field in fields if field != "id"]
            str_val += " ON CONFLICT (id) DO UPDATE SET " + ",".join(generated_directive)
            return str_val

        data = [dict(zip(keys, row)) for row in data_iter]
        conn.execute(table.table.insert(), data)

    @staticmethod
    def mysql_replace_into(table, conn, keys, data_iter):

        @compiles(Insert)
        def replace_string(insert, compiler, **kw):
            str_val = compiler.visit_insert(insert, **kw)
            str_val = str_val.replace("INSERT INTO", "REPLACE INTO")
            return str_val

        data = [dict(zip(keys, row)) for row in data_iter]
        conn.execute(table.table.insert(), data)

    def to_sql(self, data: pd.DataFrame, table_name: str, chunksize: int = 1, method=None, if_exists="append",
               schema=None):
        url = make_url(self.uri)
        db_name = url.drivername.split('+')
        if ("postgresql" in db_name) or ("pgsql" in db_name):
            method = self.pgsql_upsert if method == "replace" else method
        else:
            method = self.mysql_replace_into if method == "replace" else method

        try:
            data.to_sql(name=table_name,
                        con=self.get_sql_engine(),
                        if_exists=if_exists,
                        schema=schema,
                        chunksize=chunksize,
                        method=method,
                        index=False)
        except Exception as ex:
            print(traceback.format_exc())


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
