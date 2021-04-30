import requests
import re
from bs4 import BeautifulSoup


count=0
list=[]
for num in range(0,2000):
    page = "https://www.musicow.com/song/{0}?tab=info".format(num)
    url = requests.get(page)
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')
    
    song_title = str(soup.select('div.song_header > div.information > p > strong'))
    song_title = re.sub('<.+?>', '', song_title, 0).strip()


    if song_title[1:-1]=='':
        pass
    else:
        list.append(num)

        
        
import openpyxl
wb = openpyxl.Workbook() 
sheet = wb.active
sheet.append(['노래제목', '가수', '1차 옥션기간', '1차 발행 주식 수', '1차 낙찰가', '총 주식수', '저작권료'])

for num in list:
    page = "https://www.musicow.com/song/{0}?tab=info".format(num)
    url = requests.get(page)
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')

    copy_right = str(soup.select("div.card_body > div > div > div:nth-child(2) > div > div.title_area > strong "))
    copy_right = re.sub('<.+?>', '', copy_right, 0).strip() 

    copy_right_wotcomma = copy_right.replace(",", "")
    copy_right_wotwon = copy_right_wotcomma.replace("원","")
    
    song_title = str(soup.select('div.song_header > div.information > p > strong'))
    song_title = re.sub('<.+?>', '', song_title, 0).strip()

    song_artist = str(soup.select('div.song_header > div.information > em'))
    song_artist = re.sub('<.+?>', '', song_artist, 0).strip()

    auc_date_1 = str(soup.select('div:nth-child(1) > h2 > small'))
    auc_date_1 = re.sub('<.+?>', '', auc_date_1, 0).strip()
    auc_stock_1 = str(soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(2)'))
    auc_stock_1 = re.sub('<.+?>', '', auc_stock_1, 0).strip()

    auc_stock_1_wotcomma = auc_stock_1.replace(",", "")
    auc_stock_1_wotwjoo = auc_stock_1_wotcomma.replace(" 주","")

    auc_price_1 = str(soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(8)'))
    auc_price_1 = re.sub('<.+?>', '', auc_price_1, 0).strip()

    auc_price_1_wotcomma = auc_price_1.replace(",", "")
    auc_price_1_wotcash = auc_price_1_wotcomma.replace(" 캐쉬","")

    stock_num=soup.select_one('div.lst_copy_info dd p').text

    if song_title[1:-1]=='':
        pass
    else:
        sheet.append([song_title[1:-1], song_artist[1:-1], auc_date_1[1:-1], auc_stock_1_wotwjoo[1:-1], auc_price_1_wotcash[1:-1], ''.join(re.findall("\d", stock_num))[1:], copy_right_wotwon[1:-1], page])

wb.save('copyright_price.xlsx')