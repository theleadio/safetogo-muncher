from bs4 import BeautifulSoup
import requests
import os
import json
import re
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from dotenv import load_dotenv
from pathlib import Path

env_path = Path('.env')
load_dotenv(dotenv_path=env_path)

if __name__ == "__main__":
    URL = 'https://sgwuhan.xose.net/api/?1586840411248'

    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")  # Please remove when push to Git
    DB_PASSWORD = os.getenv("DB_PASSWORD")  # Please remove when push to Git
    DB_DATABASE = os.getenv("DB_DATABASE")
    TARGET_TABLE = os.getenv("SGXOS_SUMMARY_TABLE")
    DB_URI = os.getenv("DB_URI").format(DB_USER, DB_HOST, DB_DATABASE)

    print("Requesting...")
    page_response = requests.get(URL, timeout=5)
    page_content = BeautifulSoup(page_response.content, "html.parser")
    site_json = json.loads(page_content.text)
    print(f"Requested : {URL}")

    try:
        connection = mysql.connector.connect(host=DB_HOST,
                                             database=DB_DATABASE,
                                             user=DB_USER,
                                             password=DB_PASSWORD)
        count = 0
        for (k, v) in site_json.items():
            if k == "data":
                for e in v:
                    # lat,lng,caseType,age,gender,from,stayed,visited,caseNo,citizenship,relatedArrayNo,
                    # mohURL,confirmDate,location,relatedCaseNo
                    query = "INSERT INTO sgxos_markers(lat,lng,caseType,age,gender,_from,stayed,visited,caseNo,citizenship,relatedArrayNo,mohURL,confirmDate,location,relatedCaseNo)" \
                            "values ('"+str(e['lat'])+"','"+str(e['lng'])+"','"+str(e['caseType'])+"','"+str(e['age'])+"','"+str(e['gender'])+"','"+str(e['from']).replace("'", r"\'")+"','"+str(e['stayed']).replace("'", r"\'")+"','"+str(e['visited']).replace("'", r"\'")+"','"+str(e['caseNo'])+"','"+str(e['citizenship'])+"','"+str(e['relatedArrayNo'])+"','"+str(e['mohURL'])+"','"+str(e['confirmDate'])+"','"+str(e['location'])+"','"+str(e['relatedCaseNo'])+"')"
                    print("Successfully stored into db")
                    cursor = connection.cursor()
                    cursor.execute(query)
                    connection.commit()
                    cursor.close()
                    count += 1

        print(cursor.rowcount, "Record inserted successfully into sgxos_markers table")

        resp = requests.get(URL)
        print(resp)

    except mysql.connector.Error as error:
        print("Failed to insert record into sgxos_markers table {}".format(error))

    finally:
        if (connection.is_connected()):
            connection.close()
            print("MySQL connection is closed")
