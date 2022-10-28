from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from dateutil.relativedelta import relativedelta
import datetime
import time
import pandas as pd
from pandas.io.json import json_normalize
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

client = MongoClient('localhost', 27017)
db1 = client.music_cow
col3 = db1.copyright_price
dateToday = datetime.datetime.today()


def copyrightCrawler(musicCowSongNumList):

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    # chrome_options.binary_location = "C:\Program Files\Google\Chrome Beta\Application\chrome.exe"
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    song_num = []       # musicCowData 에 있는 곡 번호 리스트

    for x in musicCowSongNumList:
        song_num.append(x['num'])

    # musicCowData 에 있는 곡 번호를 대상으로 저작권료 크롤링 진행
    for x in song_num:
        try:
            print('{} 번 시작'.format(x))

            # 몇월까지 데이터 모았는지 확인
            try:
                listcurrentMonth = list(col3.find_one({'num': x}))
            except TypeError:
                listcurrentMonth = []
            [listcurrentMonth.remove(key) for key in ['_id', 'num'] if key in listcurrentMonth]
            listcurrentMonth.sort()

            if len(listcurrentMonth) == 0:        # 그동안의 저작권료가 하나도 안 쌓인 경우 (저작권료를 처음 긁는 곡인 경우)
                currentYear = int((dateToday - relativedelta(years=4)).strftime('%Y')) # 2018년
                currentMonth = 1    # 1월 부터 긁는다 (현재 2022년 이고, 5년치 데이터를 긁으므로, 2018년 1월 데이터 부터 긁음)
            else:
                currentYear = int(listcurrentMonth[-1].split("-")[0]) # 몇 년 데이터까지 모았는지 (년)
                currentMonth = 1

            URL = 'https://www.musicow.com/song/{}?tab=price'.format(x)
            driver.get(url=URL)
            time.sleep(1)
            button = driver.find_element(By.CSS_SELECTOR, 'div.container.song_body > nav > ul > li.t_i > a')
            button.click()
            time.sleep(1)

            # '현재 년도 - 4년' 년도로 format 안 숫자를 변경해야 함.
            flag = 0  # 이중 for 문 탈출을 위함 flag
            yearChanged = False # 해가 넘어갔는지 여부

            for i in range(currentYear, datetime.date.today().year+1):
                driver.find_element(By.CSS_SELECTOR, '#btn_year_graph1_{}'.format(i)).send_keys(Keys.ENTER)
                # button.click()
                time.sleep(1)

                for j in range(currentMonth+1, 14):
                    price = driver.find_element(By.CSS_SELECTOR, "#graph1 > span:nth-child({})".format(j)).get_attribute("data-bar-value")

                    # if (currentYear == dateToday.year & currentMonth >= dateToday.month & price == "0"):       # 데이터가 쌓인 부분까지 크롤링하고, 이후에 없는 경우 이중 for문 탈출을 위해 flag값 변경
                    #                        # ex. (2022년 2월까지만 뮤카에 저작권료 있고, 3월부터 데이터 0이면 다음곡으로 넘어감.
                    #     flag = 1
                    #     break

                    if j == 13:     # 해가 넘어갔으면 다시 1월부터 긁기 위해 yearChanged 를 True 로 함.
                        yearChanged = True

                    if len(str(j-1)) == 1:
                        year = "0" + str(j-1)
                    elif str(j-1) == 10:
                        year = str(j-1)
                    else:
                        year = str(j-1)
                    col3.update_one({'num': x}, {'$set': {'{0}-{1}'.format(i, year) : price}}, upsert=True)       # 디비에 누적

                # if flag == 1:
                #     break

            print('{} 번 완료'.format(x))

        except:
            pass

    print("<< 저작권료 크롤링을 마쳤습니다 >>")
    driver.close()


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.article

    # music cow
    col1 = db1.musicCowData

    SongNumListCurrent = list(col1.find({}))
    copyrightCrawler(SongNumListCurrent)