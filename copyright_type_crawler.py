from pymongo import MongoClient
import selenium
from selenium import webdriver
import time
import pickle
from time import sleep

class CopyrightTypeCrawler:
    def __init__(self):
        self.copyright_type()

    def copyright_type(self):
        list_db_gen_daily = col1.find({}, {'num': {"$slice": [1, 1]}})
        # list_db_gen_daily = col1.find({'num':{'$gte':204}})  204번 부터

        driver = webdriver.Chrome(executable_path='./chromedriver.exe')

        song_num = []

        for a in list_db_gen_daily:
            song_num.append(a['num'])

        for x in song_num:

            URL = 'https://www.musicow.com/song/{}?tab=price'.format(x)

            driver.get(url=URL)

            time.sleep(1)

            button = driver.find_element_by_css_selector('#page_market > div.container > nav > ul > li.t_i > a')
            button.click()

            yearly_price = driver.find_element_by_css_selector('#song_info_royalty > div.card_body > div > div > div:nth-child(2) > div > div.title_area > strong').text

            broadcast = driver.find_element_by_css_selector('#song_info_royalty > div.card_body > div > div > div:nth-child(2) > div > div.old_money > div.tbl_flex > dl:nth-child(1) > dd').text
            media = driver.find_element_by_css_selector('#song_info_royalty > div.card_body > div > div > div:nth-child(2) > div > div.old_money > div.tbl_flex > dl:nth-child(2) > dd').text
            copy = driver.find_element_by_css_selector('#song_info_royalty > div.card_body > div > div > div:nth-child(2) > div > div.old_money > div.tbl_flex > dl:nth-child(3) > dd').text
            show = driver.find_element_by_css_selector('#song_info_royalty > div.card_body > div > div > div:nth-child(2) > div > div.old_money > div.tbl_flex > dl:nth-child(4) > dd').text
            abroad = driver.find_element_by_css_selector('#song_info_royalty > div.card_body > div > div > div:nth-child(2) > div > div.old_money > div.tbl_flex > dl:nth-child(5) > dd').text
            etc = driver.find_element_by_css_selector('#song_info_royalty > div.card_body > div > div > div:nth-child(2) > div > div.old_money > div.tbl_flex > dl:nth-child(6) > dd').text

            type_info = {
                'num' : x,
                'price_recent_year' : yearly_price,
                'broadcast' : broadcast,
                'transmission' : media,
                'copy' : copy,
                'performance' : show,
                'abroad' :abroad,
                'etc' : etc
            }

            col4.insert_one(type_info).inserted_id


            print('{} 번 완료 '.format(x))

        driver.close()

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    # music cow
    col1 = db1.daily_music_cow
    col2 = db1.music_cow_ratio
    col3 = db1.music_list
    col4 = db1.copyright_type

    CopyrightTypeCrawler()