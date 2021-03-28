import requests
import re
from bs4 import BeautifulSoup

#, 1118
song_list = []


for num in range(26,1118):
    url = requests.get("https://www.musicow.com/song/{0}?tab=info".format(num))
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')

    # for auc_num in range(1,2):
    #
    #     auc_stock = soup.select('div.card_body > div > div:nth-child({0}) > dl > dd:nth-child(2)'.fomrat(auc_num))

    song_title = str(soup.select('div.song_header > div.information > p > strong'))
    song_title = re.sub('<.+?>', '', song_title, 0).strip()
    
    song_list.append(song_title)




print(song_list)


import pandas as pd

df = pd.DataFrame(song_list)

df


df.isnull()
