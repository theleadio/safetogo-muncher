from bs4 import BeautifulSoup
import requests
import os
import json
import re
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

env_path = Path('.env')
load_dotenv(dotenv_path=env_path)

if __name__ == "__main__":
    URL = 'http://api.coronatracker.com/v3/stats/worldometer/country'
    imgURL = "https://www.coronatracker.com/_nuxt/assets/image/logo.png"

    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")  # Please remove when push to Git
    DB_PASSWORD = os.getenv("DB_PASSWORD")  # Please remove when push to Git
    DB_DATABASE = os.getenv("DB_DATABASE")
    TARGET_TABLE = os.getenv("CORONATRACKER_SUMMARY_TABLE")
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
        for e in site_json:
            # countryCode,country,lat,lng,totalConfirmed,totalDeaths,totalRecovered,dailyConfirmed,dailyDeaths,activeCases,
            # totalCritical,totalConfirmedPerMillionPopulation,totalDeathsPerMillionPopulation,FR,PR,lastUpdated
            # read from db
            readQuery = "SELECT countryCode from ct_markers where countryCode='"+str(e['countryCode'])+"'"
            cursor = connection.cursor()
            cursor.execute(readQuery)
            result = cursor.fetchall()
            if result == []:
                # insert into sqldb when empty
                print(f"{e['countryCode']} - Not Found, Creating data")
                newQuery = "INSERT INTO ct_markers(lat,lng,countryCode,country,totalConfirmed,totalDeaths,totalRecovered,dailyConfirmed,dailyDeaths,activeCases," \
                            "totalCritical,totalConfirmedPerMillionPopulation,totalDeathsPerMillionPopulation,FR,PR,lastUpdated,img_url,createdAt)" \
                            "values ('"+str(e['lat'])+"','"+str(e['lng'])+"','"+str(e['countryCode'])+"','"+str(e['country']).replace("'", r"\'")+"','"+str(e['totalConfirmed'])+"','"+str(e['totalDeaths'])+"','"+str(e['totalRecovered'])+"','"+str(e['dailyConfirmed'])+"','"+str(e['dailyDeaths'])+"','"+str(e['activeCases'])+"'," \
                             "'"+str(e['totalCritical'])+"','"+str(e['totalConfirmedPerMillionPopulation'])+"','"+str(e['totalDeathsPerMillionPopulation'])+"','"+str(e['FR'])+"','"+str(e['PR'])+"','"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"','"+str(imgURL)+"','"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"')"
                cursor = connection.cursor()
                cursor.execute(newQuery)
                connection.commit()
                print(f"{e['countryCode']} - Successfully stored into db")
                cursor.close()
            else:
                for r in result: #read record from sqldb
                    # update record
                    if str(e['countryCode']) != ('' or 'None' or 'null'):
                        if str(e['countryCode']) == str(r[0]):
                            print(f"{e['countryCode']} - Found, Updating")
                            updateQuery = "UPDATE ct_markers SET totalConfirmed='"+str(e['totalConfirmed'])+"',totalDeaths='"+str(e['totalDeaths'])+"',totalRecovered='"+str(e['totalRecovered'])+"',dailyConfirmed='"+str(e['dailyConfirmed'])+"'," \
                                            "dailyDeaths='"+str(e['dailyDeaths'])+"',activeCases='"+str(e['activeCases'])+"',totalCritical='"+str(e['totalCritical'])+"',totalConfirmedPerMillionPopulation='"+str(e['totalConfirmedPerMillionPopulation'])+"'," \
                                            "totalDeathsPerMillionPopulation='"+str(e['totalDeathsPerMillionPopulation'])+"',FR='"+str(e['FR'])+"',PR='"+str(e['PR'])+"',lastUpdated='"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"' " \
                                            "WHERE countryCode='"+str(e['countryCode'])+"'"
                            cursor = connection.cursor()
                            cursor.execute(updateQuery)
                            connection.commit()
                            print(f"{e['countryCode']} - Successfully updated into db")
                            cursor.close()
                        else:
                            # insert into db
                            print(f"{e['countryCode']} - Not Found, Creating data")
                            newQuery = "INSERT INTO ct_markers(lat,lng,countryCode,country,totalConfirmed,totalDeaths,totalRecovered,dailyConfirmed,dailyDeaths,activeCases," \
                                    "totalCritical,totalConfirmedPerMillionPopulation,totalDeathsPerMillionPopulation,FR,PR,lastUpdated,img_url,createdAt)" \
                                    "values ('"+str(e['lat'])+"','"+str(e['lng'])+"','"+str(e['countryCode'])+"','"+str(e['country']).replace("'", r"\'")+"','"+str(e['totalConfirmed'])+"','"+str(e['totalDeaths'])+"','"+str(e['totalRecovered'])+"','"+str(e['dailyConfirmed'])+"','"+str(e['dailyDeaths'])+"','"+str(e['activeCases'])+"'," \
                                     "'"+str(e['totalCritical'])+"','"+str(e['totalConfirmedPerMillionPopulation'])+"','"+str(e['totalDeathsPerMillionPopulation'])+"','"+str(e['FR'])+"','"+str(e['PR'])+"','"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"','"+str(imgURL)+"','"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"')"
                            cursor = connection.cursor()
                            cursor.execute(newQuery)
                            connection.commit()
                            print(f"{e['countryCode']} - Successfully stored into db")
                            cursor.close()
                    else:
                        # read from db
                        readQuery = "SELECT country from ct_markers where country='" + str(e['country']) + "'"
                        cursor = connection.cursor()
                        cursor.execute(readQuery)
                        result = cursor.fetchall()
                        if result == []:
                            # insert into db
                                print(f"{e['country']} - Not Found, Creating data")
                                newQuery = "INSERT INTO ct_markers(lat,lng,countryCode,country,totalConfirmed,totalDeaths,totalRecovered,dailyConfirmed,dailyDeaths,activeCases," \
                                        "totalCritical,totalConfirmedPerMillionPopulation,totalDeathsPerMillionPopulation,FR,PR,lastUpdated,img_url,createdAt)" \
                                        "values ('"+str(e['lat'])+"','"+str(e['lng'])+"','"+str(e['countryCode'])+"','"+str(e['country']).replace("'", r"\'")+"','"+str(e['totalConfirmed'])+"','"+str(e['totalDeaths'])+"','"+str(e['totalRecovered'])+"','"+str(e['dailyConfirmed'])+"','"+str(e['dailyDeaths'])+"','"+str(e['activeCases'])+"'," \
                                         "'"+str(e['totalCritical'])+"','"+str(e['totalConfirmedPerMillionPopulation'])+"','"+str(e['totalDeathsPerMillionPopulation'])+"','"+str(e['FR'])+"','"+str(e['PR'])+"','"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"','"+str(imgURL)+"','"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"')"
                                cursor = connection.cursor()
                                cursor.execute(newQuery)
                                connection.commit()
                                print(f"{e['country']} - Successfully stored into db")
                                cursor.close()
                        else:
                            for r in result:
                                if str(e['country']) == str(r[0]):
                                    print(f"{e['country']} - Found, Updating")
                                    updateQuery = "UPDATE ct_markers SET totalConfirmed='"+str(e['totalConfirmed'])+"',totalDeaths='"+str(e['totalDeaths'])+"',totalRecovered='"+str(e['totalRecovered'])+"',dailyConfirmed='"+str(e['dailyConfirmed'])+"'," \
                                                    "dailyDeaths='"+str(e['dailyDeaths'])+"',activeCases='"+str(e['activeCases'])+"',totalCritical='"+str(e['totalCritical'])+"',totalConfirmedPerMillionPopulation='"+str(e['totalConfirmedPerMillionPopulation'])+"'," \
                                                    "totalDeathsPerMillionPopulation='"+str(e['totalDeathsPerMillionPopulation'])+"',FR='"+str(e['FR'])+"',PR='"+str(e['PR'])+"',lastUpdated='"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"' " \
                                                    "WHERE country='"+str(e['country'])+"'"
                                    cursor = connection.cursor()
                                    cursor.execute(updateQuery)
                                    connection.commit()
                                    print(f"{e['country']} - Successfully updated into db")
                                    cursor.close()
                                else:
                                    # insert into db
                                    print(f"{e['country']} - Not Found, Creating data")
                                    newQuery = "INSERT INTO ct_markers(lat,lng,countryCode,country,totalConfirmed,totalDeaths,totalRecovered,dailyConfirmed,dailyDeaths,activeCases," \
                                            "totalCritical,totalConfirmedPerMillionPopulation,totalDeathsPerMillionPopulation,FR,PR,lastUpdated,img_url,createdAt)" \
                                            "values ('"+str(e['lat'])+"','"+str(e['lng'])+"','"+str(e['countryCode'])+"','"+str(e['country']).replace("'", r"\'")+"','"+str(e['totalConfirmed'])+"','"+str(e['totalDeaths'])+"','"+str(e['totalRecovered'])+"','"+str(e['dailyConfirmed'])+"','"+str(e['dailyDeaths'])+"','"+str(e['activeCases'])+"'," \
                                             "'"+str(e['totalCritical'])+"','"+str(e['totalConfirmedPerMillionPopulation'])+"','"+str(e['totalDeathsPerMillionPopulation'])+"','"+str(e['FR'])+"','"+str(e['PR'])+"','"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"','"+str(imgURL)+"','"+datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')+"')"
                                    cursor = connection.cursor()
                                    cursor.execute(newQuery)
                                    connection.commit()
                                    print(f"{e['country']} - Successfully stored into db")
                                    cursor.close()

        print(cursor.rowcount, "Record inserted successfully into sgxos_markers table")
        resp = requests.get(URL)
        print(resp)

    except mysql.connector.Error as error:
        print("Failed to insert record into ct_markers table {}".format(error))

    finally:
        if (connection.is_connected()):
            connection.close()
            print("MySQL connection is closed")
