import openpyxl

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
        count+=1
        list.append(num)

        
        
import openpyxl
wb = openpyxl.Workbook() 
sheet = wb.active
sheet.append(['제목','가수','1차 옥션 기간','1차 발행 주식 수','1차 낙찰가','2차 옥션 기간','2차 발행 주식 수','2차 낙찰가','곡 출시일','총 주식수','링크'])

for num in list:
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
    #stock_num = str(soup.select('div.card_body > div > dl > dd:nth-child(20) > p:nth-child(1)'))
    #stock_num = re.sub('<.+?>', '', stock_num, 0).strip()
    stock_num=soup.select_one('div.lst_copy_info dd p').text
    
    
    
    if song_title[1:-1]=='':
        pass
    else:
        sheet.append([song_title[1:-1], song_artist[1:-1], auc_date_1[1:-1], auc_stock_1[1:-1], auc_price_1[1:-1], auc_date_2[1:-1], auc_stock_2[1:-1], auc_price_2[1:-1], song_date[1:-1], ''.join(re.findall("\d", stock_num))[1:], page])


wb.save('market2.xlsx')
