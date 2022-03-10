# << 뮤직카우의 곡별 정보 크롤링 코드 >>
# 작성자 : 정예원
#
# 코드설명
# [[ 뮤직카우 정보란에 '1,2차 옥션결과' 와 '저작권 정보' 를 모으는 코드 입니다 ]]
# 1. 곡 페이지 링크, 1,2 차 옥션 결과 , 공표일자, 저작권료 지분  을 크롤링 합니다.
# 2. 크롤링된 데이터는 musicInfo 명의 콜렉션으로 디비에 저장됩니다.
# 3. 한번 크롤링된 데이터는 변동되지 않으므로 다시 크롤링 할 필요가 없기 때문에,
#    musicCowData에 있는, 그러나 musicInfo에 없는 곡들에 대해서만 크롤링을 합니다.
# 4. musicInfo 에 저장되는 song_title과 song_artist는 따로 split 을 거치지 않은 뮤직카우 에 등록된 곡명과 가수명 입니다.
# 5. 크롤링 완료 후 다시 코드를 돌리면 "이미 musicInfo에 모든 데이터가 들어있습니다" 메시지를 print 합니다.
#
# 코드 수행시간 : 5분

import requests
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
import numpy as np

def musicInfoCrawler(col1, col4):

    musicInfoSongNumListCurrent = col4.find({}, {'num': {"$slice": [1, 1]}})  # 현재 musicInfoData 디비에 있는 곡 번호 리스트
    musicCowSongNumListCurrent = col1.find({}, {'num': {"$slice": [1, 1]}})
    # 현재 musicCowData <-> musicInfoData 디비 비교했을 때, "musicCowData에 있는, 그러나 musicInfo에 없는 곡"들을 정확히 찾아내기 위해
    # 정확히 지금 현재 시점에서 musicCowData 에 있는 곡들을 다시 한번 탐색하여 self.musicCowSongNumCurrent 변수에 할당.


    songList = []  # musicCowData에 있는, 그러나 musicInfo에 없는 곡
    for x in musicCowSongNumListCurrent:
        songList.append(x['num'])
    for x in musicInfoSongNumListCurrent:
        if x['num'] in songList:
            songList.remove(x['num'])


    if len(songList) == 0:
        print("이미 musicInfo에 모든 데이터가 들어있습니다")
    else:
        for x in songList:
            page = "https://www.musicow.com/song/{0}?tab=info".format(x)
            url = requests.get(page)
            html = url.text
            soup = BeautifulSoup(html, 'html.parser')

            song_title = str(soup.select('div.song_header > div.song_header--bar.song_header--bar_1 > div.song_header--title.txt_ellipsis > strong'))
            song_title = re.sub('\<.+?>|\[|\]', '', song_title, 0).replace('&amp;', '&').strip()

            #print("{0}번 곡 확인 중".format(x))

            crawl_link(x, col4, soup, page, song_title)

        print("========= << 곡 information 크롤링을 마쳤습니다 >> ========== ")

def crawl_link(x, col4, soup, page, song_title):
    print("{0}번 곡 뮤직카우 크롤링 시작".format(x))

    song_artist = str(soup.select('div.card_body > div > dl > dd:nth-child(4)'))
    song_artist = re.sub('\<.+?>|\[|\'|\]', '', song_artist, 0).replace('&amp;', '&').strip()

    auc_date_1 = str(soup.select('div:nth-child(1) > h2 > small'))
    auc_date_1 = re.sub('\<.+?>|\[|\'|\]', '', auc_date_1, 0).strip()
    auc_date_1_start = auc_date_1.split('~')[0].strip()
    auc_date_1_end = auc_date_1.split('~')[1].strip()

    auc_stock_1 = str(soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(2)'))
    auc_stock_1 = re.sub('\<.+?>|\[|\'|\]|\,', '', auc_stock_1, 0).replace('주','').strip()
    auc_price_1 = str(soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(8)'))
    auc_price_1 = re.sub('\<.+?>|\[|\'|\]|\,', '', auc_price_1, 0).replace('캐쉬','').strip()

    auc_date_2 = str(soup.select('div:nth-child(2) > h2 > small'))
    auc_date_2 = re.sub('\<.+?>|\[|\'|\]', '', auc_date_2, 0).strip()
    if auc_date_2[0:-1] == '':
        # None 말고 NaN과 0 이라는 값 넣은 이유는 string과 int로 type 통일하기 위해서
        auc_date_2_start = np.nan; auc_date_2_end = np.nan
        auc_stock_2 = np.nan; auc_price_2 = np.nan
    else:
        auc_date_2_start = auc_date_2.split('~')[0].strip()
        auc_date_2_end = auc_date_2.split('~')[1].strip()

    auc_stock_2 = str(soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(2)'))
    auc_stock_2 = re.sub('\<.+?>|\[|\'|\]|\,', '', auc_stock_2, 0).replace('주','').strip()
    auc_price_2 = str(soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(8)'))
    auc_price_2 = re.sub('\<.+?>|\[|\'|\]|\,', '', auc_price_2, 0).replace('캐쉬','').strip()

    song_release_date = str(soup.select('div.card_body > div > dl > dd:nth-child(2)'))
    song_release_date = re.sub('\<.+?>|\[|\'|\]', '', song_release_date, 0).strip()

    # stock_num 앞뒤 공백 제거 후 숫자만 추출
    stock_num = str(soup.select('div.lst_copy_info dd p')).split('2차적')[0]
    stock_num = re.sub('\<.+?>|\[|\'|\]|\t|\n|\,', '', stock_num, 0).replace('1/','').strip()

    print("{0}번 곡 DB 입력 중".format(x))

    list_music = {
        'num': x,
        'song_title': song_title,
        'song_artist': song_artist,
        'page': page,
        'auc1_info': {
            'auc_start_date': auc_date_1_start, 'auc_end_date': auc_date_1_end,
            'auc_stock': auc_stock_1, 'auc_price': auc_price_1},
        'auc2_info': {
            'auc_start_date': auc_date_2_start, 'auc_end_date': auc_date_2_end,
            'auc_stock': auc_stock_2, 'auc_price': auc_price_2},
        'song_release_date': song_release_date,
        'stock_num': stock_num
    }

    col4.insert_one(list_music).inserted_id

