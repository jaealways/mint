import pandas as pd
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import selenium
from selenium import webdriver
import time
import pickle
from time import sleep

class CopyrightPriceCrawler:
    def __init__(self):
        self.copyright()

    def copyright(self):

        list_db_gen_daily = col1.find({}, {'num': {"$slice": [1, 1]}})
        #list_db_gen_daily = col1.find({'num':{'$gte':204}})  204번 부터

        driver = webdriver.Chrome(executable_path='./chromedriver.exe')

        song_num = []

        for a in list_db_gen_daily:
            song_num.append(a['num'])

        #self.copyright_price = {}

        # ====== music_list 에 있는 814 곡 저작권료 크롤링 -> 딕셔너리 형태로 만들기 ====== #
        # ex) self.copyright_price = {26 : [[2017년 저작권료], [2018년 저작권료], [2019년 저작권료], [2020년 저작권료], [2021년 저작권료]], 27 : ... }


        for x in song_num:

            #self.copyright_price[x] = [[],[],[],[],[]]

            URL = 'https://www.musicow.com/song/{}?tab=price'.format(x)

            driver.get(url=URL)

            time.sleep(1)

            button = driver.find_element_by_css_selector('#page_market > div.container > nav > ul > li.t_i > a')
            button.click()

            for i in range(0,5):

                button = driver.find_element_by_css_selector('#btn_year_graph1_{}'.format(2017+i))
                button.click()

                for j in range(2,14):
                    price = driver.find_elements_by_css_selector("#graph1 > span:nth-child({})".format(j))[0].get_attribute("data-bar-value")
                    #self.copyright_price[x][i].append(price)

                    col4.update_one({'num': x}, {'$set': {'{0}-{1}'.format(2017+i, j-1) : price}},upsert=True)


            print('{} 번 완료 '.format(x))

        driver.close()

        # 완성된 저작권료 딕셔너리 print
        #print(self.copyright_price)



        # pickle 로 만들기
        #with open('copyright_price.pkl','wb') as f:
            #pickle.dump(self.copyright_price, f)



        # ========== 이전 BeautifulSoup 시도 ======
        # list_db_gen_daily = col1.find({}, {'num': {"$slice": [1, 1]}})
        #
        # self.copyright_price = {}
        #
        # for x in list_db_gen_daily:
        #     if x['num'] == 1388:  #### 임시 data 개수 맞지 않아 dataframe 으로 matching 안됨.
        #         break
        #
        #     # '함께 구매한 곡' 크롤링
        #     url = "https://www.musicow.com/song/{0}?tab=info".format(x['num'])
        #     resp = requests.get(url)
        #     html = resp.text
        #
        #     soup = BeautifulSoup(html, 'html.parser')
        #
        #     links = soup.select("graph1 > span:nth-of-type(8)")
        #     #[3].attrs['data-bar-value']
        #
        #
        #
        #     # attrs['data-bar-value']
        #
        #     self.copyright_price[x['num']] = []
        #
        #     for a in links[1:]:
        #         href = a.attrs['data-bar-value']
        #         self.copyright_price[x['num']].append(int(href))

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    # music cow
    col1 = db1.daily_music_cow
    col2 = db1.music_cow_ratio
    col3 = db1.music_list
    col4 = db1.copyright_price


    CopyrightPriceCrawler()