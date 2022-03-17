# << 뮤직카우 관련 데이터 크롤링 & 신곡감지 & 새로 등록된 가수 감지 코드 >> : musicCowCrawler.py
# 작성자 : 정예원
#
# [코드 설명]
# 1. 'songCrawler' 와 'songCrawlerNew' 함수로 이루어져 있습니다
# 2. <Track1> (DB에 있는 기존 곡 대상으로 크롤링) 에서는 songCrawler를,
# 3. <Track2> (2000~3000 신곡 감지 크롤링) 에서는 songCrawlerNew 를 사용합니다
#
# [ <Track 1> 의 songCrawler 작동원리 ]
# 1. musicCowData 디비에 있던 기존 곡들을 대상으로, (가장 최근에 크롤링을 한 날짜 이후 ~ 크롤링 시점 이전 날짜) 까지 데이터를 각 곡의 필드마다 누적하여 저장합니다.
# 2. 이미 크롤링이 가장 최근까지 완료된 데이터에 대해서는 "- 이미 가장 최근 데이터가 저장되어있습니다." 라고 print합니다.
# - 코드 수행 시간 : 1137개 곡 일주일 치 코드 돌린다고 가정했을 때 15분 소요, 하루 치는 8분 소요
#
# [ <Track 2> 의 songCrawlerNew 작동원리 ]
# 1. 2000~3000 중 이미 musicCowData 에 있는 곡 번호를 제외한 번호 리스트 (song_list) 를 대상으로 for문을 돌려 신곡을 검사합니다.
# 2. 검사를 완료하면 새로 추가된 곡의 데어터(newSongList)가 반환됩니다.
# 3. newSongList 에는 예를들어, 2494 번이 신곡으로 감지되면, 아래와 같은 형식으로 데이터가 저장되므로, 딕셔너리 인덱싱을 활용하여 새로 추가된 곡들의 곡번호들, 가수명 등을 조회할 수 있습니다.
# {2466:
#      {'num': 2466,
#       'song_title': '왜 몰랐을까',
#       'song_artist': '로이킴',
#       '2022-02-22': {'price_high': '30500', 'price_low': '30500', 'price_close': '30500', 'pct_price_change': '0', 'cnt_units_traded': '0'},
#       '2022-02-23': {'price_high': '59100', 'price_low': '30000', 'price_close': '30500', 'pct_price_change': '0', 'cnt_units_traded': '296'}
#         ......}


# [ 코드 개선 권장 사항 ]
# 1. 여러 스레드로 코드수행시간 단축
# 2. 신곡말고, 기존에 있었는데 삭제된 곡들도 감지
#
# * 비고 : 몽고디비에서 find로 데이터 호출 후 for문에 한번 쓰면 이후에는 데이터가 소멸된다는 걸 확인했습니다.
#         따라서 for문의 in에 몽고디비 데이터를 써야 한다면 필요할 때 마다 find로 호출해야 하므로, 필요한 for문 앞에 마다 find하여 지역변수로 할당했습니다.
#
# [코드 미완성 사항]
# 1. songCrawlerNew 에 새로 뽑아온 신곡들에 대해 songSeparator 를 적용하지 못했습니다.


from pymongo import MongoClient
import requests as r
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd

# 2000~3000 중 새로 추가된 곡 코드 수행시간 : 10분
def songCrawlerNew(col, musicCowSongNumListCurrent):

    newSongList = []  # 신곡의 뮤직카우 데이터 모음

    # musicCowSongNumListCurrent = col.find({}, {'num': {"$slice": [1, 1]}})

    song_list = list(range(0, 3000))  # 2000~3000 까지 신곡들 검사할 list
    for x in musicCowSongNumListCurrent:
        if x['num'] in song_list:
            song_list.remove(x['num'])

    option = Options()
    option.headless = False
    driver = webdriver.Chrome(options=option, executable_path='crawlers/chromedriver.exe')

    detectedSongNumber = 0      # 감지한 신곡 개수

    print("========== << 2000 ~ 3000 중 새로 추가된 곡 크롤링을 시작합니다 >> =========")

    # 2000~3000 중에 이미 디비에 있는 곡 번호는 제외한 list 를 대상으로 신곡 탐지
    for x in song_list:

        # Opening the connection and grabbing the page
        my_url = 'https://www.musicow.com/song/{}'.format(x)
        driver.get(my_url)

        song_title = driver.find_element(By.XPATH, '/ html / body / div[2] / div[1] / div[2] / div[1] / div[2] / div[1] / strong').text

        if song_title == "":
            pass
        else :
            detectedSongNumber = detectedSongNumber + 1

            print("{}번 곡 시작".format(x))

            # url to post
            action_postURL = "https://www.musicow.com/api/song_prices"

            # use get to pull cookies information
            res = r.get(action_postURL)

            # Get the Cookies
            search_cookies = res.cookies

            # post method data
            post_data = {"song_id": "{}".format(x), "period": "60"}

            # headers information
            headers = {
                'user-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36"}

            # request post data
            res_post = r.post(action_postURL, data=post_data, cookies=search_cookies, headers=headers)

            # pull data into json format
            values = res_post.json()

            # normalize data with Brazilian gold values
            song_prices = res_post.json()["prices"]
            df = pd.json_normalize(song_prices)

            # song_title_add, song_artist_add
            song_title_add = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div[1]/div[2]/div[1]/div[2]/div[1]/strong'))).text
            song_artist_add = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div[1]/div[2]/div[1]/div[2]/div[2]/div'))).text

            dict1 = {
                'num': x,
                'song_title': "{}".format(song_title_add),
                'song_artist': "{}".format(song_artist_add)
            }

            # songsplit 적용 후 아래 조건 검사 해야함
            # ex 뮤직카우 : IU(아이유) 로 등록 됨
            #    현재디비 : IU 로 등록 됨
            # -> 아래 코드 적용시 IU(아이유)가 새로운 가수라고 판단하기 때문에
            # IU(아이유)를 songSeparator 를 거친 후 IU 라고 여과한 후 조건 탐색해야
            # # 새로 등록된 가수 명단 누적
            # if song_artist_add in self.musicCowArtistListCurrent:
            #     pass
            # else:
            #     self.newArtistList.append(song_artist_add)


            # 뮤직카우에 신곡으로 등록이 됐지만, 등록된지 얼마 되지 않아 아직 price 데이터가 누적되지 않은 경우
            if df.empty:
                pass
            else:
                dict2 = df.set_index('ymd').T.to_dict()
                dict1.update(dict2)

            newSongList.append(dict1)

            # 디비 업데이트
            col.insert_one(dict1).inserted_id

            print(x, "번 곡 종료")

    driver.close()
    print("\n\n========== << 총 {} 개 신곡 크롤링을 마쳤습니다 >> ==========".format(detectedSongNumber))

    f = open("./storage/check_new/newSongList.txt", 'w')
    [f.write("%d %s %s \n" % (i['num'], i['song_title'], i['song_artist'])) for i in newSongList]
    f.close()

    return newSongList

# 기존 db에 있는 곡 크롤링 (songCrawler) 코드 수행시간 : 매일 돌릴다고 했을 때 1137곡 기준으로 8분
def songCrawler(col, musicCowSongNumListCurrent):

    print("========== << 기존 db에 있는 곡 크롤링을 시작합니다 >> ==========")

    # url to post
    action_postURL = "https://www.musicow.com/api/song_prices"

    # use get to pull cookies information
    res = r.get(action_postURL)

    # Get the Cookies
    search_cookies = res.cookies

    # headers information
    headers = {
        'user-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36"}

    # musicCowSongNumListCurrent = col.find({}, {'num': {"$slice": [1, 1]}})
                                # 호출하고 for문에서 한번 사용시 소멸되는 듯 하니, for문에서 쓰기 전마다 한 번씩 호출하여 지역변수로 선언하기로 함.

    for x in musicCowSongNumListCurrent:
        print(x['num'], "번 곡 시작")

        # post method data
        post_data = {"song_id": "{}".format(x['num']),"period": "60"}

        # request post data
        res_post = r.post(action_postURL, data = post_data, cookies = search_cookies, headers = headers)

        # pull data into json format
        values = res_post.json()

        # normalize data with song prices
        song_prices = res_post.json()["prices"]
        df = pd.json_normalize(song_prices)

        list_music_cow = {
            'num': x['num'],
            'song_title': "{}".format(x['song_title']),
            'song_artist': "{}".format(x['song_artist'])
        }

        # ymd : 거래 일자
        # price_high : 최고가
        # price_low : 최저가
        # price_close : 하루 마감 가격
        # pct_price_change : 전일 대비 가격증감률
        # cnt_unit_trade : 거래량

        # 현재까지 모은 데이터 날짜 확인
        currentData = col.find_one({'num': x['num']})

        # df2 = pd.json_normalize(currentData)
        # currentDate = df2.columns[len(df2.columns)-1].split(".")[0]

        list_currentDate = currentData.keys()
        list_currentDate = list(set(list_currentDate) - set(['_id', 'num', 'song_title', 'song_artist']))
        list_currentDate.sort()

        if len(list_currentDate) > 0:
            currentDate = list_currentDate[-1]
        else:
            pass

        # currentData 이후 시점부터 ymd 를 인덱스로 하여 딕셔너리 만들기
        if currentDate == df.iloc[len(df.index)-1, 0]:        # 이미 디비에 가장 최근 데이터로 갱신이 완료된 경우
            print("  - 이미 가장 최근 데이터가 저장되어있습니다.")
            print(x['num'], "번 곡 종료")
            continue
        else:
            index = df.index[df['ymd'] == currentDate].tolist()[0]
            dict1 = df.iloc[index+1 :, :].set_index('ymd').T.to_dict()

        # db 업데이트
        col.update_one(list_music_cow, {'$set': dict1}, upsert=True)

        print(x['num'], "번 곡 종료")

    print("========== << 기존 db에 있는 곡 크롤링이 끝났습니다 >> ==========\n")



client = MongoClient('localhost', 27017)
db1 = client.music_cow

# music cow
col = db1.musicCow_Volume
list_db_gen_daily = col.find({}, {'num': {"$slice": [1, 1]}})

#MusicCowCrawler()

