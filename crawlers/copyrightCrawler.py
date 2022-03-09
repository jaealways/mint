# << 곡별 5년치 저작권료 크롤링 코드 >>
# 작성자 : 정예원
#
# [코드 설명]
# 1. 현재 몽고디비의 music_cow 디비의 musicCowData 콜렉션에 있는 곡들을 대상으로
#    각 곡들의 현재년도 기준 과거 5년치 저작권료 데이터를 년별, 월별로 수집합니다.
#    ex) 현재 2022년 -> 2018 2019 2020 2021 2022 년 수집
#
# 코드 수행시간 :  이전 크롤링 시점이 얼마나 최근인가에 따라 상이함. (한 월씩 814곡 + 5년씩 신곡 323개 긁을때 : 약 40분)
#
# [코드 주의사항]
# 1. song_num을 대상으로 하는 for문 내부에 이중 for문 중 겉 for문 루프의 '2023'은 현재년도가 2022년 이기 때문에 2022+1 = 2023 으로 한 것 입니다.
#    따라서 해가 바뀌면 (크롤링을 하는 시점의 년도 + 1) 의 수로 바꿔야 합니다.
# 2. 위와 같은 이유로 if df.empty 문의 currentYear 를 설정하는 부분도 2022년 기준으로 2018년으로 할당한 것이고, 
#    해가 바뀌면 2019, 2020 ... 등 기준년도에서 5년전 년도로 수정해야 합니다.
#
# [코드 오류사항]
# 코드는 정상적으로 끝까지 잘 돌아가나, 몽고디비에 모든 곡이 적재되지는 않았습니다. (1137곡 저작권료 크롤링 완료했으나, 디비에는 899곡만 적용됨)
# 아마 몽고디비 연결 끊김 오류라고 생각됩니다.

from pymongo import MongoClient
from selenium import webdriver
import time
import pandas as pd
from pandas.io.json import json_normalize
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


def copyrightCrawler(col1, col3):

    driver = webdriver.Chrome(executable_path='chromedriver.exe')

    song_num = []       # musicCowData 에 있는 곡 번호 리스트

    musicCowSongNumListCurrent = col1.find({}, {'num': {"$slice": [1, 1]}})

    for x in musicCowSongNumListCurrent:
        song_num.append(x['num'])

    # musicCowData 에 있는 곡 번호를 대상으로 저작권료 크롤링 진행
    for x in song_num:

        print('{} 번 시작'.format(x))

        # 몇월까지 데이터 모았는지 확인
        currentData = col3.find({'num': x})
        df = pd.json_normalize(currentData)

        if df.empty:        # 그동안의 저작권료가 하나도 안 쌓인 경우 (저작권료를 처음 긁는 곡인 경우)
            currentYear = 2018  # 2018년
            currentMonth = 1    # 1월 부터 긁는다 (현재 2022년 이고, 5년치 데이터를 긁으므로, 2018년 1월 데이터 부터 긁음)
        else:
            currentYear = int(df.columns[len(df.columns) - 1].split("-")[0])      # 몇 년 데이터까지 모았는지 (년)
            currentMonth = int(df.columns[len(df.columns) - 1].split("-")[1])     # 몇 월 데이터까지 모았는지 (월)

        URL = 'https://www.musicow.com/song/{}?tab=price'.format(x)
        driver.get(url=URL)
        time.sleep(1)
        button = driver.find_element(By.CSS_SELECTOR, 'div.container.song_body > nav > ul > li.t_i > a')
        button.click()

        # '현재 년도 - 4년' 년도로 format 안 숫자를 변경해야 함.
        flag = 0  # 이중 for 문 탈출을 위함 flag
        yearChanged = False # 해가 넘어갔는지 여부

        for i in range(currentYear, 2023):
            driver.find_element(By.CSS_SELECTOR, '#btn_year_graph1_{}'.format(i)).send_keys(Keys.ENTER)
            # button.click()

            if yearChanged == True:    # 해가 넘어갔을 때 1월부터 다시 긁기 위해 currentMonth 값을 1로 변경.
                currentMonth = 1

            for j in range(currentMonth+1,14):
                price = driver.find_element(By.CSS_SELECTOR, "#graph1 > span:nth-child({})".format(j)).get_attribute("data-bar-value")

                if price == "0":       # 데이터가 쌓인 부분까지 크롤링하고, 이후에 없는 경우 이중 for문 탈출을 위해 flag값 변경
                                       # ex. (2022년 2월까지만 뮤카에 저작권료 있고, 3월부터 데이터 0이면 다음곡으로 넘어감.
                    flag = 1
                    break

                if j == 13:     # 해가 넘어갔으면 다시 1월부터 긁기 위해 yearChanged 를 True 로 함.
                    yearChanged = True

                if len(j-1) == 1:
                    year = "0" + str(j-1)
                col3.update_one({'num': x}, {'$set': {'{0}-{1}'.format(i, year) : price}},upsert=True)       # 디비에 누적

            if flag == 1:
                break

        print('{} 번 완료'.format(x))

    print("<< 저작권료 크롤링을 마쳤습니다 >>")
    driver.close()

