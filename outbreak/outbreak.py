"""
    Copyright 2020 Edmund Hee, All Right Reserved
    Email: edmund.hee05@gmail.com
"""
import requests
import os
import pandas as pd
from pathlib import Path
from services.sql_runner import SqlRunner
from bs4 import BeautifulSoup as bs
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


if __name__ == "__main__":
    URL = "https://www.outbreak.my/"
    UPDATE_URL = "https://api.safetogo.live/map/v2/votes/update?reference=summary"
    HEADERS = {
        'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 ' +
            '(KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
    }
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")  # Please remove when push to Git
    DB_PASSWORD = os.getenv("DB_PASSWORD")  # Please remove when push to Git
    DB_DATABASE = os.getenv("DB_DATABASE")
    TARGET_TABLE = os.getenv("DISTRICT_SUMMARY_TABLE")
    DB_URI = os.getenv("DB_URI").format(DB_USER, DB_HOST, DB_DATABASE)

    s = requests.Session()
    s.headers.update(HEADERS)
    resp = requests_retry_session(session=s).get(URL)

    respond_bd = bs(resp.content, "html.parser")

    result = respond_bd.find_all("table", {"class": "table-states"})
    print(result)
    if not len(result) > 0:
        raise Exception("States Table not found!")

    districts_raw = result[0].find_all("td", {"class": "text-value"})
    totals_raw = result[0].find_all("td", {"class": "text-value-total"})
    deaths_raw = result[0].find_all("td", {"class": "text-value-black"})

    districts = [district.text.replace("\n", "").strip() for district in districts_raw]
    totals = [total.text.replace("\n", "") for total in totals_raw]
    deaths = [death.text.replace("\n", "") for death in deaths_raw]

    districts.pop()
    totals.pop()
    deaths.pop()

    processed_data = []
    for district, total, death in zip(districts, totals, deaths):
        processed_data.append({
            "confirmed": total,
            "district": district,
            "death": death,
            "country": "Malaysia",
            "created_at": datetime.utcnow().strftime('%Y%m%d %H:%M:%S'),
            'upvote': 0,
            'downvote':0
        })

    df = pd.DataFrame(processed_data)
    df["created_at"] = pd.to_datetime(df["created_at"]).values.astype('datetime64[ms]')

    runner = SqlRunner(DB_URI, DB_PASSWORD)
    runner.to_sql(df, TARGET_TABLE, chunksize=100, if_exists="replace")

    resp = requests.get(UPDATE_URL)
    print(resp)
