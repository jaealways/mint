import datetime
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

class daily_Naver:
    def __init__(self):
        self.article_num = 0
        self.read_db()
        self.count=0

    def read_db(self):
        list_db_music = col1.find({}, {'num': {"$slice": [1, 1]}})
        
        
        for x in list_db_music:
            self.num = x['num']
            if 'song_artist_main_kor1' in x['list_split']:
                self.song_artist = x['list_split']['song_artist_main_kor1']
            else:
                self.song_artist = x['list_split']['song_artist_main_eng1']
            if 'song_title_main_kor' in x['list_split']:
                self.song_title = x['list_split']['song_title_main_kor']
            else:
                self.song_title = x['list_split']['song_title_main_eng']
            self.pair = self.song_artist + ' ' + self.song_title
            self.pair = self.pair.replace('%', '%25')
            self.pair = self.pair.replace('&', '%26')

            self.listing_article()
            self.count=0
        print(self.count)

    def listing_article(self):
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        today=datetime.date.today()
        start_date='2021.06.07'
        
        self.page='https://search.naver.com/search.naver?where=news&sm=tab_pge&query={0}&sort=1&photo=0&field=0&pd=3&ds={1}&de={2}&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:dd,p:from20210603to20210713,a:all&start='.format(self.pair,start_date,today.strftime('%Y.%m.%d'))
            #today += datetime.timedelta(-1)
            
        #self.page='https://search.naver.com/search.naver?where=news&sm=tab_pge&query={0}&sort=1&photo=0&field=0&pd=2&ds=2021.06.03&de=2021.07.03&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:dd,p:1m,a:all&start='.format(self.pair)
        #print(self.page)
        for page in range(1,10000):

            article_link=self.page+str(page)
            print(article_link)
            temp_res=requests.get(article_link, headers=headers)
            temp_soup=BeautifulSoup(temp_res.text, 'html.parser')

            cons=temp_soup.select('ul.list_news li')
            if cons!=[]:

                for con in cons:


                    try:
                        temp=con.select('a.info')[1]
                        link=temp.attrs['href']
                        print(link)
                        temp_res=requests.get(link, headers=headers)
                        temp_soup=BeautifulSoup(temp_res.text, 'html.parser')
                        try:
                            article_title=temp_soup.select_one('h2.end_tit')
                            text=temp_soup.select_one('div.end_body_wrp>div')
                            publish=temp_soup.select_one('a.press_logo>img')
                            publish=publish.attrs['alt']
                            print(text.text)
                        #crawled_count+=1
                            self.article_num+=1
                            self.articles = {
                                'num': self.num,
                                'song_title': self.song_title,
                                'song_artist': self.song_artist,
                                'link': link,
                                'article_title' : article_title.text,
                                'publish':publish,
                                'text':text.text,
                                'date':today.strftime('%Y-%m-%d')

                            }        
                            col2.insert_one(self.articles).inserted_id
                        except:
                            article_title=temp_soup.select_one('h3.tts_head')
                            text=temp_soup.select_one('div#articleBodyContents')
                            publish=temp_soup.select_one('div.press_logo img')
                            publish=publish.attrs['alt']
                            print(text.text)
                        #crawled_count+=1
                            self.article_num+=1
                            self.articles = {
                                'num': self.num,
                                'song_title': self.song_title,
                                'song_artist': self.song_artist,
                                'link': link,
                                'article_title' : article_title.text,
                                'publish' : publish,
                                'text':text.text,
                                'date':today.strftime('%Y-%m-%d')

                            }        
                            col2.insert_one(self.articles).inserted_id                            
                    except:
                        pass
            else:
                break



                


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    col1 = db1.music_list_split
    col2 = db1.daily_article

    daily_Naver()
