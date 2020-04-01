"""
    Created by: Edmund Hee
    Email: edmund.hee05@gmail.com
"""
import requests
import logging
import pandas as pd
from datetime import datetime
from services.sql_runner import SqlRunner


def map_info_f(payload):
    return {
            "region": payload["Region"],
            "source": payload["RefLink"],
            "source_name": payload["Sources"],
            "confirmed": int(payload["Confirmed"]),
            "death": int(payload["Death"]),
            "cured": int(payload["Cured"])
        }


def map_marker_f(location, payload):
    return {
        "city": payload["City"],
        "last_updated": payload["LatestUpdated"],
        "region": payload["Region"],
        "lat": float(payload["Lat"]),
        "lng": float(payload["Lng"]),
        "level": payload["Level"],
        "zone": payload["Zone"],
        "active_case": int(payload["ActiveCases"]),
        "total_confirmed": int(payload["TotalConfirmed"]),
        "total_deaths": int(payload["TotalDeaths"]),
        "total_recovered": int(payload["TotalRecovered"]),
        "source": location["source"],
        "source_name": location["source_name"],
        "reportedDate": datetime.utcnow().strftime('%Y%m%d %H:%M:%S'),
        "scrapped": 1,
        "text_show": f"Active Cases : {payload['ActiveCases'] if 'ActiveCases' in payload else 0}|Total Confirmed : {payload['TotalConfirmed'] if 'TotalConfirmed' in payload else 0}|Total Deaths : {payload['TotalDeaths'] if 'TotalDeaths' in payload else 0}|Total Recovered : {payload['TotalRecovered'] if 'TotalRecovered' in payload else 0}",
        "locationName": f"[ {payload['Level'].title()} ] - {payload['City']}, {payload['Region']}",
        "email": "tanghoong.com@gmail.com",
        "img_url": "https://lh3.googleusercontent.com/a-/AOh14Gg61l26tmwJpJ28UdDtvrqRxqlqAKakKfTNN66q3Dc",
        "createdBy": "T H",
        "reference": "summary"
    }


if __name__ == "__main__":
    URL_INFO = "https://gs.kamfu.dev/?page=stg-info"
    URL_MARKERS = "https://gs.kamfu.dev/?page=stg-{}"

    DB_HOST = "safetogo.caapwlyn1xci.ap-southeast-1.rds.amazonaws.com"
    DB_USER = ""  # Please remove when push to Git
    DB_PASSWORD = ""  # Please remove when push to Git
    DB_DATABASE = "safetogo_dev"
    DB_TABLE = "borneo_markers_test"
    DB_URI = 'mysql://{}@{}:3306/{}'.format(DB_USER, DB_HOST, DB_DATABASE)

    info_resp = requests.get(URL_INFO)
    runner = SqlRunner(DB_URI, DB_PASSWORD)

    for location in list(map(map_info_f, info_resp.json()["results"])):
        markers_resp = requests.get(
            URL_MARKERS.format(
                location["region"].lower()
            )
        )
        if "results" not in markers_resp.json():
            pass

        markers = markers_resp.json()["results"]
        processed_data = map(
            map_marker_f,
            [location]*len(markers),
            markers
        )
        df = pd.DataFrame(processed_data)
        df["last_updated"] = pd.to_datetime(df["last_updated"]).values.astype('datetime64[ms]')
        df["reportedDate"] = pd.to_datetime(df["reportedDate"]).values.astype('datetime64[ms]')
        runner.to_sql(df, DB_TABLE, chunksize=100, if_exists="append")

    result = runner.run_query("""
    SELECT
        city,
        last_updated,
        region,
        lat,
        lng,
        lvl1.level,
        zone,
        active_case,
        case when flow = 2 then active_case ELSE 0 END as previous_active,
        total_confirmed,
        case when flow = 2 then total_confirmed ELSE 0 END as previous_confirmed,
        total_deaths,
        case when flow = 2 then total_deaths ELSE 0 END as previous_deaths,
        total_recovered,
        case when flow = 2 then total_recovered ELSE 0 END as previous_recovered,
        source,
        source_name,
        reportedDate,
        scrapped,
        text_show,
        locationName,
        email,
        img_url,
        createdBy,
        reference
            FROM(
                SELECT
                    *,
                    RANK() OVER(PARTITION BY city, region ORDER BY reportedDate DESC) as flow
                FROM
                    borneo_markers_test
            ) as lvl1
        WHERE flow = 1
    """, result_size=None)
    granular = pd.DataFrame(result)
    runner.to_sql(granular, "borneo_markers", chunksize=100, if_exists="replace")

