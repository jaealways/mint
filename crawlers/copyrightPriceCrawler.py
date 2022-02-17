# << 곡별 5년치 저작권료 크롤링 코드 >>
# 작성자 : 정예원
#
# 코드 설명
# 1. 현재 몽고디비의 music_cow 디비의 musicCowData 콜렉션에 있는 곡들을 대상으로
#    각 곡들의 현재년도 기준 과거 5년치 저작권료 데이터를 년별, 월별로 수집합니다.
#    ex) 현재 2022년 -> 2018 2019 2020 2021 2022 년 수집
# 2. self.copyrightPrice 멤버변수에
#     {26 : [[2018년 저작권료], [2019년 저작권료], [2020년 저작권료], [2021년 저작권료], [2022년 저작권료]], 27 : ... } 형태로 저장됩니다.
#     mainCrawler.py 에서 이 멤버를 참조하면 크로링 결과를 얻을 수 있습니다.
#
# 코드 수행시간 :  60분
#
# 코드 개선사항
# 1. 해가 바뀌면 뮤직카우에서 제공하는 5년치 자료 또한 바뀌므로 (2021년에는 2017~2021 자료, 2022년에는 2018~2022 자료 제공)
#    이중 for문 내의 format의 숫자를 바꿔줘야함.

from pymongo import MongoClient
from selenium import webdriver
import time
from pandas.io.json import json_normalize

class CopyrightPriceCrawler:
    def __init__(self, col1, col3):
        self.col1 = col1        # 현재 몽고디비에 등록된 song number 리스트가 있는 콜렉션
        self.col3 = col3        # copyrightPrice 를 저장할 몽고디비의 콜렉션
        self.musicCowSongNumListCurrent = col1.find({}, {'num': {"$slice": [1, 1]}})  # 새로 갱신된 뮤직카우 song number 리스트

    def copyrightPrice(self):

        self.copyrightPrice = {}  # 곡별 copyrightPrice 들이 저장될 딕셔너리

        driver = webdriver.Chrome(executable_path='chromedriver.exe')

        song_num = []

        for x in self.musicCowSongNumListCurrent:
            song_num.append(x['num'])


        # ====== 딕셔너리 형태(self.copyrightPrice)로 만들기 ====== #
        # ex) self.copyright_price = {26 : [[2018년 저작권료], [2019년 저작권료], [2020년 저작권료], [2021년 저작권료], [2022년 저작권료]], 27 : ... }
        for x in song_num:

            print('{} 번 시작'.format(x))

            # 몇월까지 데이터 모았는지 확인
            currentData = self.col3.find({'num': x})
            df = json_normalize(currentData)
            #currentYear = int(df.columns[len(df.columns) - 1].split("-")[0])      # 몇 년 데이터까지 모았는지 (년)
            #currentMonth = int(df.columns[len(df.columns) - 1].split("-")[1])     # 몇 월 데이터까지 모았는지 (월)

            self.copyrightPrice[x] = [[],[],[],[],[]]

            URL = 'https://www.musicow.com/song/{}?tab=price'.format(x)

            driver.get(url=URL)

            time.sleep(1)

            button = driver.find_element_by_css_selector('#page_market > div.container > nav > ul > li.t_i > a')
            button.click()

            # '현재 년도 - 4년' 년도로 format 안 숫자를 변경해야 함.
            # ex ) 2021년 - 4년 = 2017  // ex) 2022년 - 4년 = 2018
            flag = 0  # 이중 for 문 탈출을 위함 flag
            for i in range(2018,2023):

                button = driver.find_element_by_css_selector('#btn_year_graph1_{}'.format(i))
                button.click()

                #

                for j in range(2,14):
                    price = driver.find_elements_by_css_selector("#graph1 > span:nth-child({})".format(j))[0].get_attribute("data-bar-value")

                    if price == "0":
                        flag = 1
                        break

                    #self.copyrightPrice[x][i].append(price)
                    self.col3.update_one({'num': x}, {'$set': {'{0}-{1}'.format(i, j-1) : price}},upsert=True)

                if flag == 1:
                    break

            print('{} 번 완료'.format(x))


        print("<< 저작권료 크롤링을 마쳤습니다 >>")
        driver.close()

        # # 완성된 저작권료 딕셔너리 print
        # print(self.copyrightPrice)





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
