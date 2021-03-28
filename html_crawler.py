import requests
import re
from bs4 import BeautifulSoup

#, 1118, 625번
# 옥션 마감곡 784곡, 진행중 5곡 20210328기준, 마켓 731곡(최근거래곡)


for num in range(26,1118):
    page = "https://www.musicow.com/song/{0}?tab=info".format(num)
    url = requests.get(page)
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')

    # for auc_num in range(1,2):
    #
    #     auc_stock = soup.select('div.card_body > div > div:nth-child({0}) > dl > dd:nth-child(2)'.fomrat(auc_num))

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
    stock_num = str(soup.select('div.card_body > div > dl > dd:nth-child(20) > p:nth-child(1)'))
    stock_num = re.sub('<.+?>', '', stock_num, 0).strip()


    print("{0}번 곡".format(num), song_title, song_artist)
    print(auc_date_1, auc_stock_1, auc_price_1)
    print(auc_date_2, auc_stock_2,auc_price_2)
    print(song_date, stock_num)
    print(page)
    
    
    
#총 주식 결측값들은 child(18)을 값으로 가짐
#page_market > div > div.song_tab.tab_info.on > section:nth-child(7) > div.card_body > div > dl > dd:nth-child(18) > p:nth-child(1)
