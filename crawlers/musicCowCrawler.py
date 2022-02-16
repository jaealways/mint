# 코드 설명
# [[ 현재일자 전날 까지의 뮤직카우의 거래량, 최저/최고가, 하루마감가격, 전날대비증감률 크롤링 코드 ]]
# 1. 두 함수 내의 'headers information' 부분의 user-Agent 를 자신의 주소로 변경해야 합니다.
#
# - 현재 일자 전날 까지의 데이터가 디비에 저장됩니다.
# - 200~3000 번 중에서 새로 추가된 곡을 감지하여 필드가 추가됩니다. (2000번 이전에는 크롤링을 몇번 시도해도 신곡이 아예 없어서 시간 단축 위해 2000 이후를 크롤링)
# - pickle 로 담고자 하면 dictionary ={} 에 할당된 내용을 맨 아래 코드의 주석을 풀어 pickle 로 만들면 됩니다.
# - 코드 수행 시간 :

# 코드 개선사항
# 1. 여러 스레드로 코드수행시간 단축
# 2. 신곡말고, 기존에 있었는데 삭제된 곡들도 감지
# 3. 이미 갱신 완료됐는데 또 코드 돌리려고 할 때 예외처리
# 4. 신곡 등록이 됐지만, 등록된지 얼마 되지 않아 아직 price 데이터가 누적되지 않은 경우 예외처리 if문  (개선 완료)
# 5. 2000~ 3000 번 으로 변경 (개선 완료)

from pymongo import MongoClient
import requests as r
from pandas.io.json import json_normalize
import pickle
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


class MusicCowCrawler:
    def __init__(self, col1, musicCowSongNumListCurrent, musicCowArtistListCurrent):
        self.col1 = col1
        self.list = list(range(2000, 3000))
        self.musicCowSongNumListCurrent = musicCowSongNumListCurrent
        self.musicCowArtistListCurrent = musicCowArtistListCurrent
        self.dictionary = {}  # 피클로 만들 때 필요한 딕셔너리형 자료형 선언 (피클로 만들지 않을 시 필요없음)
        self.newArtistList = []


    # 2000~3000 중 새로 추가된 곡 (songCrawlerAdditional) 코드 수행시간 : 20분
    def songCrawlerNew(self):

        option = Options()
        option.headless = False
        driver = webdriver.Chrome(options=option)

        detectedSongNumber = 0


        print("========== << 2000 ~ 3000 중 새로 추가된 곡 크롤링을 시작합니다 >> =========")
        for x in self.musicCowSongNumListCurrent:
            if x['num'] in self.list:
                self.list.remove(x['num'])


        for x in self.list:

            # Opening the connection and grabbing the page
            my_url = 'https://www.musicow.com/song/{}'.format(x)
            driver.get(my_url)

            song_title = driver.find_element_by_xpath('/ html / body / div[2] / div[1] / div[2] / div[1] / div[2] / div[1] / strong').text

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
                df = json_normalize(song_prices)

                # song_title_add, song_artist_add
                song_title_add = WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div[1]/div[2]/div[1]/div[2]/div[1]/strong'))).text
                song_artist_add = WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div[1]/div[2]/div[1]/div[2]/div[2]/div'))).text

                dict1 = {
                    'num': x,
                    'song_title': "{}".format(song_title_add),
                    'song_artist': "{}".format(song_artist_add)
                }

                # 새로 등록된 가수 명단 누적
                if song_artist_add in self.musicCowArtistListCurrent:
                    pass
                else:
                    self.newArtistList.append(song_artist_add)

                # 신곡 등록이 됐지만, 등록된지 얼마 되지 않아 아직 price 데이터가 누적되지 않은 경우
                if df.empty:
                    pass
                else:
                    dict2 = df.set_index('ymd').T.to_dict()
                    dict1.update(dict2)

                self.dictionary[x] = dict1

                # 디비 업데이트
                self.col1.insert_one(dict1).inserted_id

                print(x, "번 곡 종료")

        print("\n\n========== << 총 {} 개 신곡 크롤링을 마쳤습니다 >> ==========".format(detectedSongNumber))

    # 기존 db에 있는 곡 크롤링 (songCrawler) 코드 수행시간 : 6분
    def songCrawler(self):


        print(" ========== << 기존 db에 있는 곡 크롤링을 시작합니다 >> ==========")

        # url to post
        action_postURL = "https://www.musicow.com/api/song_prices"

        # use get to pull cookies information
        res = r.get(action_postURL)

        # Get the Cookies
        search_cookies = res.cookies

        # headers information
        headers = {
            'user-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36"}

        for x in self.musicCowSongNumListCurrent:
            print(x['num'], "번 곡 시작")

            # post method data
            post_data = {"song_id": "{}".format(x['num']),"period": "60"}

            # request post data
            res_post = r.post(action_postURL, data = post_data, cookies = search_cookies, headers = headers)

            # pull data into json format
            values = res_post.json()

            # normalize data with song prices
            song_prices = res_post.json()["prices"]
            df = json_normalize(song_prices)

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
            currentData = self.col1.find({'num': x['num']})
            df2 = json_normalize(currentData)
            currentDate = df2.columns[len(df2.columns)-1].split(".")[0]


            # currentData 이후 시점부터 ymd 를 인덱스로 하여 딕셔너리 만들기
            index = df.index[df['ymd'] == currentDate].tolist()[0]
            dict1 = df.iloc[index+1 :, :].set_index('ymd').T.to_dict()

            self.dictionary[x['num']] = dict1

            # db 업데이트
            self.col1.update_one(list_music_cow, {'$set': dict1}, upsert=True)

            print(x['num'], "번 곡 종료")

        print("========== << 기존 db에 있는 곡 크롤링이 끝났습니다 >> ==========\n")
        # self.songCrawlerAdditional()



if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow

    # music cow
    col = db1.musicCow_Volume
    list_db_gen_daily = col.find({}, {'num': {"$slice": [1, 1]}})

    MusicCowCrawler()


# 피클로 만들 때
# with open('musicCow_All_0111.pkl', 'wb') as f:
#     pickle.dump(dictionary, f)
