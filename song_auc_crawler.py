import requests
import re
from bs4 import BeautifulSoup

#, 1118, 625번
# 옥션 마감곡 784곡, 진행중 5곡 20210328기준, 마켓 731곡(최근거래곡)

#곡 넘버링 임의로 되어있으므로, 0~2000까지 무작위로 루프

for num in range(0,27):

    #page 밑에 따로 출력하기 위해 url과 별도로 분리
    page = "https://www.musicow.com/song/{0}?tab=info".format(num)
    url = requests.get(page)
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')

    song_title = str(soup.select('div.song_header > div.information > p > strong'))
    song_title = re.sub('<.+?>', '', song_title, 0).strip()

    #노래 제목이 없을 경우 비어있는 페이지이므로 pass
    if song_title[1:-1]=='':
        pass
    else:

        #위에서 pass되지 않은 곡만 기록
        #re.sub -> 정규표현식
        song_title = str(soup.select('div.song_header > div.information > p > strong'))
        song_title = re.sub('<.+?>', '', song_title, 0).strip()
        song_artist = str(soup.select('div.song_header > div.information > em'))
        song_artist = re.sub('<.+?>', '', song_artist, 0).strip()

        auc_date_1 = str(soup.select('div:nth-child(1) > h2 > small'))
        auc_date_1 = re.sub('<.+?>', '', auc_date_1, 0).strip()
        auc_stock_1 = str(soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(2)'))
        auc_stock_1 = re.sub('<.+?>', '', auc_stock_1, 0).strip()
        auc_price_1 = str(soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(8)'))
        auc_price_1 = re.sub('<.+?>', '', auc_price_1, 0).strip()

        auc_date_2 = str(soup.select('div:nth-child(2) > h2 > small'))
        auc_date_2 = re.sub('<.+?>', '', auc_date_2, 0).strip()
        auc_stock_2 = str(soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(2)'))
        auc_stock_2 = re.sub('<.+?>', '', auc_stock_2, 0).strip()
        auc_price_2 = str(soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(8)'))
        auc_price_2 = re.sub('<.+?>', '', auc_price_2, 0).strip()

        song_date = str(soup.select('div.card_body > div > dl > dd:nth-child(2)'))
        song_date = re.sub('<.+?>', '', song_date, 0).strip()

        #stock_num 앞뒤 공백 제거 후 숫자만 추출(어떻게 간추릴 수 있을지 고민 중)
        stock_num = str(soup.select('div.card_body > div > dl > dd:nth-child(20) > p:nth-child(1)'))
        stock_num = re.sub('<.+?>', '', stock_num, 0).strip()
        stock_num = stock_num.replace('\t','').replace('\n','').replace('1/','').replace(',','')


        # print("{0}번 곡".format(num), song_title, song_artist)
        # print(auc_date_1, auc_stock_1, auc_price_1)
        # print(auc_date_2, auc_stock_2,auc_price_2)
        # print(song_date, stock_num)
        print(soup)
    
    
        #클라우드에 저장하는 것 까지 구현할 것
