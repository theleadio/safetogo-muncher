"""
    Copyright 2020 Edmund Hee, All Right Reserved
    Email: edmund.hee05@gmail.com
"""
import requests
import re
import os
import json
import pandas as pd
from pathlib import Path
from services.sql_runner import SqlRunner
from dotenv import load_dotenv
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

env_path = Path('/root/.env')
load_dotenv(dotenv_path=env_path)


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

    index = int(d["properties"]["longitude"] * 100000) % 5

    user_profile=[
        {
            "name": "Edmund Hee",
            "email": "edmund.hee05@gmail.com",
            "img_url": "https://lh3.googleusercontent.com/a-/AOh14Gj03lsgLneIxLB4rq_HHaeHooKHegPlad_U85YR"
        },
        {
            "name": "Nazmi Asri",
            "email": "nazmiasri95@gmail.com",
            "img_url": "https://lh3.googleusercontent.com/a-/AOh14GhAduy8fsb2SvVs72Ro0MrGI5FmKerv1Ge9iRD5iA"
        },
        {
            "name": "Dr. Cher Han Lau",
            "email": "laucherhan@gmail.com",
            "img_url": "https://lh3.googleusercontent.com/a-/AOh14GjtLk2V_1kuvszOIWOzOTN5ndMSCLoiGUeYdpc5EQ"
        },
        {
            "name": "Lim Jyy Bing",
            "email": "ljyybing@gmail.com",
            "img_url": "https://lh4.googleusercontent.com/-S4tlIqApV44/AAAAAAAAAAI/AAAAAAAAAAA/AAKWJJN824W1vbeLi9SAmTAzKDcGrsW8-g/photo.jpg"
        },
        {
            "name": "SafeToGo",
            "email": "",
            "img_url": ""
        }
    ]

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
    j_data["createdBy"] = user_profile[index]["name"]
    j_data["email"] = user_profile[index]["email"]
    j_data["img_url"] = user_profile[index]["img_url"]
    j_data["upvote"] = 0
    j_data["downvote"] = 0
    j_data["last_updated"] = datetime.now()
    j_data["scrapped"] = 1
    j_data["outlier"] = 0 if re.match(regex, site) is not None else 1
    j_data["country"] = "Malaysia"
    return j_data


class GeneratorLen(object):
    def __init__(self, gen, length):
        self.gen = gen
        self.length = length

    def __len__(self):
        return self.length

    def __iter__(self):
        return self.gen


if __name__ == "__main__":
    URL = "http://redangpow.com/covid-19/data/map.geojson"
    UPDATE_URL = "https://api.safetogo.live/map/v2/votes/update?reference=location"
    # UPDATE_URL = "http://localhost:3000/map/v2/votes/update?reference=location"
    HEADERS = {
        'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36'
    }
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")  # Please remove when push to Git
    DB_PASSWORD = os.getenv("DB_PASSWORD")  # Please remove when push to Git
    DB_DATABASE = os.getenv("DB_DATABASE")
    TARGET_TABLE = os.getenv("REDANGPOW_MARKER_TABLE")
    DB_URI = os.getenv("DB_URI").format(DB_USER, DB_HOST, DB_DATABASE)
    print("Requesting...")
    s = requests.Session()
    s.headers.update(HEADERS)
    r = requests_retry_session(session=s).get(URL)
    print(f"Requested : {URL}")
    if not r.content:
        raise Exception(f"URL response content is not JSON !")
    raw_d = r.json()["features"]

    # with open('redangpow.json', 'r') as myfile:
    #     data = myfile.read()
    # raw_d = json.loads(data)["features"]

    print("Map Data...")
    processed_d = list(map(map_f, raw_d))
    df = pd.DataFrame(processed_d)
    df["last_updated"] = pd.to_datetime(df["last_updated"]).values.astype('datetime64[ms]')
    df["reportedDate"] = pd.to_datetime(df["reportedDate"]).values.astype('datetime64[ms]')
    df_json = df.to_json(orient='records')
    runner = SqlRunner(DB_URI, DB_PASSWORD)

    print("Check Data...")
    for item in json.loads(df_json):
        query = f"SELECT lat, lng FROM {TARGET_TABLE} WHERE lat = {item.get('lat')} AND lng = {item.get('lng')} AND locationName=\"{item.get('locationName','')}\" AND site=\"{item.get('site','')}\""
        result = runner.run_query(query, result_size=100)
        if not sum(1 for x in result):
            print(f"Insert Data... {item['lat']} - {item['lng']}")
            data_df = pd.DataFrame.from_dict(item, orient="index").transpose()
            runner.to_sql(data_df, TARGET_TABLE, chunksize=1)

    resp = requests.get(UPDATE_URL)
    print(resp)
