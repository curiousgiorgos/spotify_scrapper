# Scrappig tool used to extract the top 200 weekly charts provided on charts.spotify.com/
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
import numpy as np
from selenium.webdriver.support import expected_conditions as EC

from selenium import webdriver 
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os.path

from url_crafter import create_urls


# spotify id
# login-username
# login password


def extract():
    url = 'https://accounts.spotify.com/en/login?continue=https%3A%2F%2Fcharts.spotify.com/login'

    # valid spotify account credentials
    username = ''
    password = ''

    chrome_options = Options()
    chrome_options.add_argument("--headless") # Ensure GUI is off
    chrome_options.add_argument("--no-sandbox")
    homedir = os.path.expanduser("~")

    webdriver_service = Service(f"{homedir}/chromedriver/stable/chromedriver")
    driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)

    driver.get(url)
    driver.find_element(By.ID,"login-username").send_keys(username)
    driver.find_element(By.ID,"login-password").send_keys(password)
    driver.find_element(By.ID, "login-button").click()

    def extract_data(cou, u, short, driver):

        wait = WebDriverWait(driver, 30)
        driver.get(u)
        wait.until(EC.presence_of_element_located((By.XPATH, f"//a[contains(@href, '/charts/overview/{short}')]")))


        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')

        ls = soup.find('table').findAll('tr')
        data_cou = []
        for pos, tr in enumerate(ls[1:]):
            span = [i for i in tr.findAll('a')][1:]            
            datum = [] 

            # track
            datum.append(span[0].text[1:])
            # artist
            datum.append(span[1].text)

            # feats
            feats = [] 
            for i in span[2:]:
                feats.append(i.text)
            datum.append(feats)
            datum.append(pos+1)

            datum.extend([i.text for i in tr.findAll('td')][-6:-2])
            datum.append(cou)
            datum.append(short)
            # track uri
            datum.append(span[0]['href'].split("/")[-1])
            #artist uri
            datum.append(span[1]['href'].split("/")[-1])

            data_cou.append(datum)

        return pd.DataFrame(data_cou, columns=[u'track', u'artist', u'feat',u'current_pos', 'peak_pos', 'prev_pos', 'streak', 'streams', 'country', 'country_short' ,'track_uri', 'artist_uri'])
    
    
    df = pd.DataFrame(columns=[u'track', u'artist', u'feat',u'current_pos', 'peak_pos', 'prev_pos', 'streak', 'streams', 'country', 'country_short' ,'track_uri', 'artist_uri'])
    df.astype({'track': 'str', 'artist' : 'str','current_pos':'str', 'feat': 'str', 'peak_pos' : 'int','prev_pos':'int', 'streak' : 'int', 'streams' : 'int','country': 'str','country_short': 'str','track_uri': 'str','artist_uri': 'str'})

    # NEED TO EXTRACT LINKS 
    links = create_urls()
    # wait for redirect
    sleep(2)
    for country, u, short in links:
        data = extract_data(country, u, short, driver)
        df = pd.concat([df,data], ignore_index=True)

    # TAKE A LOOK AT THIS AGAIN
    for col in ['peak_pos', 'prev_pos', 'streak', 'streams', 'current_pos']:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',',''), errors='coerce')
        # replace NaN with string NaN
        df[col].fillna("NaN",inplace=True)
                

    print("Completed data scrappin, extracting to csv")
    # path to extraction location
    full_path = os.path.expanduser('')
    df.to_csv(full_path, index=False, encoding='utf-8-sig')
    print("Data extracted")

    driver.quit()


