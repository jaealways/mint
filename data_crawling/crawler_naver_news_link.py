import datetime
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

from data_crawling import artist_for_nlp


class daily_Naver:
    def __init__(self, col, ArtistList, NewsArtistListLong, NewsDateListLong):
        self.article_num = 0
        self.count = 0
        self.col = col
        self.read_db(ArtistList, NewsArtistListLong, NewsDateListLong)

    def read_db(self, ArtistList, NewsArtistListLong, NewsDateListLong):
        self.link_num = 0
        artist_list = ArtistList

        for idx, artist in enumerate(artist_list):
            self.num = idx
            # self.num = 30
            self.keyword = artist
            print('{0} 검색 시작'.format(artist))
            self.listing_article(NewsArtistListLong, NewsDateListLong)

    def listing_article(self, NewsArtistListLong, NewsDateListLong):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        today = datetime.date.today()
        # today = datetime.date(2020, 8, 9)
        try:
            # mongoDate = list(map(lambda x: x['date'], list(self.col.find({'artist': self.keyword}))))
            idx_list = list(filter(lambda x: NewsArtistListLong[x] == self.keyword, range(len(NewsArtistListLong))))
            list_date = [NewsDateListLong[x] for x in idx_list]
            liststop = list(set(list_date))
            liststop.sort()
            stop = liststop[-1]
        except:
            stop = '2019-01-01'

        while 1:
            today += datetime.timedelta(-1)
            if today.strftime('%Y-%m-%d') <= stop:
                break

            self.page = 'https://search.naver.com/search.naver?where=news&sm=tab_pge&query={0}&sort=1&photo=0&field=0&pd=3&ds={1}&de={1}&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:dd,p:from20210603to20210713,a:all&start='.format(
                self.keyword, today.strftime('%Y.%m.%d'))
            #             print(self.page)
            print('날짜:', today.strftime('%Y.%m.%d'))
            for page in range(1, 10000, 10):

                print('검색 페이지 :', page)
                article_link = self.page + str(page)
                #                 print(article_link)
                temp_res = requests.get(article_link, headers=headers)
                temp_soup = BeautifulSoup(temp_res.text, 'html.parser')
                cons = temp_soup.select('ul.list_news li')
                if cons != []:
                    for num, con in enumerate(cons):

                        print('{}번째 con'.format(num + 1))
                        try:
                            temp = con.select('a.info')[1]
                            press = con.select_one('a.info.press')
                            title = con.select_one('a.news_tit')
                            link = temp.attrs['href']
                            print('{0} 검색 결과 ----------------------------------------------------'.format(self.keyword))
                            print('날짜 {0}'.format(today.strftime('%Y-%m-%d')))
                            print('네이버 검색 페이지: ', article_link)
                            print('기사 링크: ', link)
                            self.articles = {
                                'num': self.num,
                                'link_num': self.link_num,
                                'artist': self.keyword,
                                'link': link,
                                'article_title': title.text,
                                'publish': press.text,
                                'date': today.strftime('%Y-%m-%d')
                            }
                            self.col.insert_one(self.articles).inserted_id
                            print(self.articles)
                            print('{0}.번째 링크 정보 기입됨'.format(self.link_num))
                        except:
                            try:
                                press = con.select_one('a.info.press')
                                title = con.select_one('a.news_tit')
                                link = title.attrs['href']
                                if press.text not in ['톱스타뉴스', '싱글리스트', '일간스포츠', '톱데일리', '브레이크뉴스', '국제뉴스', '비즈엔터',
                                                      '조이뉴스24', '열린뉴스통신', '위키트리']:
                                    text = con.select_one('a.api_txt_lines.dsc_txt_wrap')
                                    print('{0} 검색 결과 ----------------------------------------------------'.format(
                                        self.keyword))
                                    print('{0}번째 기사'.format(self.link_num))
                                    print('네이버 검색 페이지: ', article_link)
                                    print('기사 링크: ', link)
                                    self.articles = {
                                        'num': self.num,
                                        'link_num': self.link_num,
                                        'artist': self.keyword,
                                        'link': link,
                                        'article_title': title.text,
                                        'publish': press.text,
                                        'text': text.text,
                                        'date': today.strftime('%Y-%m-%d')
                                    }
                                    self.col.insert_one(self.articles).inserted_id
                                    print(self.articles)
                                    print('{0}.번째 링크 정보 기입됨'.format(self.link_num))
                                else:
                                    print('{0} 검색 결과 ----------------------------------------------------'.format(
                                        self.keyword))
                                    print('{0}번째 기사'.format(self.link_num))
                                    print('네이버 검색 페이지: ', article_link)
                                    print('기사 링크: ', link)
                                    self.articles = {
                                        'num': self.num,
                                        'link_num': self.link_num,
                                        'artist': self.keyword,
                                        'link': link,
                                        'article_title': title.text,
                                        'publish': press.text,
                                        'date': today.strftime('%Y-%m-%d')
                                    }
                                    self.col.insert_one(self.articles).inserted_id
                                    print(self.articles)
                                    print('{0}.번째 링크 정보 기입됨'.format(self.link_num))

                            except:
                                self.articles = {
                                    'num': self.num,
                                    'link_num': self.link_num,
                                    'artist': self.keyword,
                                    'link': " ",
                                    'article_title': " ",
                                    'publish': " ",
                                    'date': today.strftime('%Y-%m-%d')
                                }
                                self.col.insert_one(self.articles).inserted_id
                                pass
                        self.link_num += 1

                else:
                    self.articles = {
                        'num': self.num,
                        'link_num': self.link_num,
                        'artist': self.keyword,
                        'link': " ",
                        'article_title': " ",
                        'publish': " ",
                        'date': today.strftime('%Y-%m-%d')
                    }
                    self.col.insert_one(self.articles).inserted_id
                    break
