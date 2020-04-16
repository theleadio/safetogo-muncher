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

env_path = Path('./.env')
load_dotenv(dotenv_path=env_path)

def map_redangpow(d):
    j_data = {}
    j_data["lat"] = d["lat"]
    j_data["lng"] = d["lng"]
    j_data["description"] = "Data provided by sgwuhan.xose.net"
    j_data["locationName"] = d["location"] if d["location"] else d["visited"]
    j_data["caseNo"] = d["caseNo"]
    j_data["source"] = f"https://www.moh.gov.sg/news-highlights/details/{d['mohURL']}" if d['mohURL'] else "https://sgwuhan.xose.net"
    j_data["reportedDate"] = None
    j_data["site"] = d["mohURL"]
    j_data["text_show"] = d["visited"]
    j_data["createdBy"] = d["created_by"]
    j_data["email"] = d["email"]
    j_data["img_url"] = d["img_url"]
    j_data["upvote"] = 0
    j_data["downvote"] = 0
    j_data["last_updated"] = datetime.now()
    j_data["scrapped"] = 1
    j_data["outlier"] = 0 
    j_data["country"] = "Singapore"
    j_data["district"] = "Singapore"
    return j_data


if __name__ == "__main__":
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")  # Please remove when push to Git
    DB_PASSWORD = os.getenv("DB_PASSWORD")  # Please remove when push to Git
    DB_DATABASE = os.getenv("DB_DATABASE")
    TARGET_TABLE = os.getenv("REDANGPOW_MARKER_TABLE")
    DB_URI = os.getenv("DB_URI").format(DB_USER, DB_HOST, DB_DATABASE)

    runner = SqlRunner(DB_URI, DB_PASSWORD)
    query = "SELECT * FROM safetogo.sgxos_markers"
    result = runner.run_query(query, result_size=500)
    for x in result:
        result_json = json.loads(x.to_json(orient='records'))
        content = map(map_redangpow, result_json)
        df = pd.DataFrame(content)
        runner.to_sql(df, TARGET_TABLE, chunksize=100, if_exists="append")


