import datetime
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
from multiprocessing import Pool


class update_article_info:
    def __init__(self, dateToday, col5, ArtistList):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        [self.make_link_list(dateToday, col5, artist) for artist in ArtistList]


    def make_link_list(self, dateToday, col5, artist):
        articles = list(col5.find({'artist': artist, 'text': {'$exists': False}}).distinct("link"))
        print('artist: {0}, {1}개'.format(artist, len(articles)))
        [self.text_crawler(doc, col5, self.headers, dateToday) for doc in articles]

    def text_crawler(self, doc, col, dateToday):
        link = doc['link']
        #             print(doc['link_num'])

        try:
            req = requests.get(link, headers=self.headers)
            soup = BeautifulSoup(req.text, 'html.parser')
            try:
                if '/news.naver' in link:
                    try:
                        text = soup.select_one('div#articleBodyContents').text
                    except:
                        try:
                            text = soup.select_one('div#articeBody').text
                        except:
                            print('오류난 링크 :', link)
                            error.append(link)
                elif 'sports.news.naver' in link:
                    text = soup.select_one('div#newsEndContents').text
                elif 'entertain.naver' in link:
                    text = soup.select_one('div#articeBody').text
                elif 'topstarnews' in link:
                    text = soup.select_one('div#article-view-content-div div.article-view-page').text
                elif 'slist.kr' in link:
                    text = soup.select_one('article.grid.body').text
                elif 'isplus' in link:
                    text = soup.select_one('div#adiContents').text
                elif 'topdaily' in link:
                    text = soup.select_one('div#article-view-content-div').text
                elif 'breaknews' in link:
                    text = soup.select_one('div#CLtag').text
                elif 'gukjenews' in link:
                    text = soup.select_one('article#article-view-content-div').text
                elif 'etoday' in link:
                    text = soup.select_one('div#articleBody').text
                elif 'joynews24' in link:
                    text = soup.select_one('article#articleBody').text
                elif 'onews' in link:
                    text = soup.select_one('article#article-view-content-div').text
                elif 'wikitree' in link:
                    text = soup.select_one('div#wikicon').text
                else:
                    text = ' '
            except:
                print('오류난 링크 :', link)
                error.append(link)
                text = ' '
            col.update_one({'link': doc['link']}, {'$set': {'text': text, 'date_crawler': dateToday.strftime('%Y-%m-%d')}}, upsert=True)

        except:
            pass


error = []


# from multiprocessing import Process, Pool
#
#
# client = MongoClient('localhost', 27017)
# db1 = client.music_cow
# db2 = client.article
# col5 = db2.article_info
# ArtistList = []
# dateToday = datetime.datetime.today()
# update_article_info(dateToday, col5, ArtistList)

# p1 = Process(target=update_article_info(dateToday, col5, ArtistList))
# p1.start()
# p2 = Process(target=update_article_info(dateToday, col5, ArtistList))
# p2.start()
