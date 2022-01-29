import datetime
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
from multiprocessing import Pool


class update_article_info:
    def __init__(self):
        self.text_crawler()

    def text_crawler(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        articles = col.find({'link_num':{'$gte':1015500},'text': {'$exists': False}})
        for doc in articles:
            link = doc['link']
            #             print(doc['link_num'])
            if doc['link_num'] % 100 == 0:
                print(doc['link_num'])

            req = requests.get(link, headers=headers)
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
                            continue
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
                    text = ''
            except:

                print('오류난 링크 :', link)
                error.append(link)
                continue
            col.update_one({'link': doc['link']}, {'$set': {'text': text}}, upsert=True)


error = []

if __name__ == '__main__':
    client = MongoClient('localhost', 27017)

    db = client.article
    col = db.article_info

    pool = Pool(processes=4)
    pool.map(update_article_info())



