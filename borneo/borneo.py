"""
    Copyright 2020 Edmund Hee, All Right Reserved
    Email: edmund.hee05@gmail.com
"""
import requests
import os
import json
import pandas as pd
from datetime import datetime
from services.sql_runner import SqlRunner
from dotenv import load_dotenv
from pathlib import Path

env_path = Path('/root/.env')
load_dotenv(dotenv_path=env_path)

def map_info_f(payload):
    return {
        "region": payload["Region"],
        "state": payload["State"],
        "country": payload["Country"],
        "sources": payload["Sources"],
        "reflink": payload["RefLink"],
        "newly": int(payload["Newly"]),
        "confirmed": int(payload["Confirmed"]),
        "death": int(payload["Death"]),
        "cured": int(payload["Cured"])
    }

def map_state_f(location, payload):
    return {
        "city": payload["City"],
        "state": location["state"],
        "country": location["country"],
        "last_updated": payload["LatestUpdated"],
        "lat": float(payload["Lat"]),
        "lng": float(payload["Lng"]),
        "level": payload["Level"],
        "zone": payload["Zone"],
        "active_case": int(payload["ActiveCases"]) if "ActiveCases" in payload else 0,
        "total_confirmed": int(payload["TotalConfirmed"]) if "TotalConfirmed" in payload else 0,
        "total_deaths": int(payload["TotalDeaths"]) if "TotalDeaths" in payload else 0,
        "total_recovered": int(payload["TotalRecovered"]) if "TotalRecovered" in payload else 0,
        "newly_positive": int(payload["NewlyPositive"]) if "NewlyPositive" in payload else 0,
        "source": location["reflink"],
        "source_name": location["sources"],
        "created_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "scrapped": 1,
        "locationName": f"[ {payload['Level'].title()} ] - {payload['City']}, {payload['Region']}",
        "email": "tanghoong.com@gmail.com",
        "img_url": "https://scontent.fkul14-1.fna.fbcdn.net/v/t1.0-1/cp0/p80x80/67345235_1147574472095737_4139901689371033600_o.jpg?_nc_cat=111&_nc_sid=dbb9e7&_nc_ohc=jmKJer2fMn8AX8TpjUQ&_nc_ht=scontent.fkul14-1.fna&oh=5a1f69fcb4b44f25b134db8062d131d8&oe=5EB91F84",
        "createdBy": "Tang Hoong",
        "reference": "borneo",
        "upvote": 0,
        "downvote": 0
    }

def map_trend(location, payload):
    return {
        "city": payload["City"],
        "state": location["state"],
        "country": location["country"],
        "created_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "active": int(payload["ActiveCases"]) if "ActiveCases" in payload else 0,
        "confirmed": int(payload["TotalConfirmed"]) if "TotalConfirmed" in payload else 0,
        "death": int(payload["TotalDeaths"]) if "TotalDeaths" in payload else 0,
        "recovered": int(payload["TotalRecovered"]) if "TotalRecovered" in payload else 0,
        "newly_positive": int(payload["NewlyPositive"]) if "NewlyPositive" in payload else 0,
    }


if __name__ == "__main__":
    URL_INFO = "https://gs.kamfu.dev/?page=stg-info"
    URL_MARKERS = "https://gs.kamfu.dev/?page=stg-{}"

    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")  # Please remove when push to Git
    DB_PASSWORD = os.getenv("DB_PASSWORD")  # Please remove when push to Git
    DB_DATABASE = os.getenv("DB_DATABASE")
    TARGET_TABLE = os.getenv("BORNEO_TARGET_TABLE")
    BORNEO_HISTORY_TABLE =  os.getenv("BORNEO_HISTORY_TABLE")
    DB_URI = os.getenv("DB_URI").format(DB_USER, DB_HOST, DB_DATABASE)

    info_resp = requests.get(URL_INFO)

    try:
        info_resp = info_resp.json()["results"]
    except Exception as e:
        print(str(e))
    
    for location in list(map(map_info_f,info_resp)):

        state_resp = requests.get(
            URL_MARKERS.format(
                location["region"].lower()
            )
        )

        print(f"Running {location['region']}")

        if "results" not in state_resp.json():
            pass

        markers = state_resp.json()["results"]
        state_data = map(
            map_state_f,
            [location]*len(markers),
            markers
        )

        state_trend_data = map(
            map_trend,
            [location]*len(markers),
            markers            
        )
        runner = SqlRunner(DB_URI, DB_PASSWORD)

        # STORE HISTORY DATA
        print("Append history data")
        df_hist = pd.DataFrame(state_trend_data)
        runner.to_sql(df_hist, BORNEO_HISTORY_TABLE, chunksize=100)


        df_state = pd.DataFrame(state_data)
        df_state["lat"] = df_state["lat"].values.astype('float32')
        df_state["lng"] = df_state["lng"].values.astype('float32')
        df_json = df_state.to_json(orient='records')

        print("Check Data...")
        for item in json.loads(df_json):

            query = f"SELECT id FROM {TARGET_TABLE} WHERE city = \"{item.get('city')}\" AND state = \"{item.get('state')}\" AND country = \"{item.get('country')}\""
            result = runner.run_query(query, result_size=10)

            if not sum(1 for x in result):

                print(f"Insert Data... {item.get('city')}, {item.get('state')}, {item.get('country')}")
                data_df = pd.DataFrame.from_dict(item, orient="index").transpose()
                runner.to_sql(data_df, TARGET_TABLE, chunksize=1)

            else:
                print(f"Update Data... {item.get('city')}, {item.get('state')}, {item.get('country')}")

                query = f"UPDATE {TARGET_TABLE} SET active_case = {item.get('active_case')}, active_case = {item.get('active_case')}, active_case = {item.get('active_case')}, active_case = {item.get('active_case')}, active_case = {item.get('active_case')}, last_updated=\"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\" WHERE city = \"{item.get('city')}\" AND state = \"{item.get('state')}\" AND country = \"{item.get('country')}\""

                result = runner.run_query(query, result_size=10)
        


