import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from tqdm import tqdm


def genie_genre():
    list_db_music = col1.find({'genre': {'$exists': False}})
    for x in tqdm(list_db_music):
        num, song_artist, song_title = x['num'], x['song_artist'], x['song_title']
        pair = '%s %s' % (song_artist, song_title)
        pair = pair.replace('%', '%2525')
        pair = pair.replace('&', '%2526')

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        page = 'https://www.genie.co.kr/search/searchSong?query={0}&Coll='.format(pair)
        url = requests.get(page, headers=headers)
        soup = BeautifulSoup(url.text, 'html.parser')

        html_name = soup.select('tr a.title')
        html_num = soup.select('a.btn-info')

        count = 0

        for n, i in enumerate(html_name):
            if song_title.lower().replace(' ', '') in i.text.lower().replace(' ', ''):
                count += 1

                url_detail = html_num[n].attrs['onclick']
                link_genie = 'https://www.genie.co.kr/detail/songInfo?xgnm='+re.findall('\d+', url_detail)[0]
                info_url = requests.get(link_genie, headers = headers)
                soup = BeautifulSoup(info_url.text, 'html.parser')
                genre = soup.select('span.value')[2].text
                genre = genre.split('/')[-1].replace(' ', '')

                if genre == '전체':
                    genre = '일반가요'

                break

        col4.update_one({'num': num}, {'$set': {'genre': genre}})


client = MongoClient('localhost', 27017)
db1 = client.music_cow
col1 = db1.musicCowData
col4 = db1.musicInfo


if __name__ == '__main__':
    genie_genre()

