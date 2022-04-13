import datetime
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
from multiprocessing import Pool


class update_article_info:
    def __init__(self, dateToday):
        self.dateToday = dateToday
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        # self.make_link_list(dateToday)


    def make_link_list(self):
        articles = list(col5.find({'date_crawler': self.dateToday, 'text': {'$exists': False}}))
        print('{0}: {1}개'.format(self.dateToday, len(articles)))
        [self.text_crawler(doc, col5, self.dateToday) for doc in articles]

    def text_crawler(self, doc):
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
            col5.update_one({'_id': doc['_id']}, {'$set': {'text': text, 'date_crawler': self.dateToday.strftime('%Y-%m-%d')}}, upsert=True)

        except:
            pass


error = []
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}


client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article
col5 = db2.article_info
dateToday = datetime.datetime.today()


def text_crawler(doc):
    link = doc['link']
    print(doc['doc_num'])

    try:
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
                        text = ' '
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
        col5.update_one({'_id': doc['_id']}, {'$set': {'text': text}}, upsert=True)

    except:
        pass


if __name__ == '__main__':
    # list_date = ['2022-03-29', '2022-03-30', '2022-04-01', '2022-04-03', '2022-04-05', '2022-04-06', '2022-04-07', '2022-04-08', '2022-04-09', '2022-04-10', '2022-04-11', '2022-04-12']
    from multiprocessing import Process, Pool, Queue

    list_blank = list(col5.find({'text': ' '}))
    print(len(list_blank))
    #
    # # [temp_text(x) for x in list_doc_num]
    #
    # pool = Pool(20)
    # pool.map(text_crawler, list_blank)
