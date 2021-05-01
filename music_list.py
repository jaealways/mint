import requests
import re
from bs4 import BeautifulSoup
from pprint import pprint
import boto3

class MusicList():
    def __init__(self):
        # 뮤직카우 크롤링 시작
        self.crawl_link()
        # 아티스트랑 타이틀 분류하기(feat, prod)
        #self.name_classifier()
        # DB에 저장하기
        self.collect_db()


    def crawl_link(self):
        print("뮤직카우 크롤링 시작")
        for self.num in range(0,2000):
            self.page = "https://www.musicow.com/song/{0}?tab=info".format(self.num)
            self.url = requests.get(self.page)
            self.html = self.url.text
            self.soup = BeautifulSoup(self.html, 'html.parser')

            self.song_title = str(self.soup.select('div.song_header > div.information > p > strong'))
            self.song_title = re.sub('<.+?>', '', self.song_title, 0).strip()

            print("{0}번째 노래 저장 중".format(self.num))

            #노래 제목이 없을 경우 비어있는 페이지이므로 pass
            if self.song_title[1:-1]=='':
                pass
            else:

                #위에서 pass되지 않은 곡만 기록
                #re.sub -> 정규표현식
                self.song_title = str(self.soup.select('div.song_header > div.information > p > strong'))
                self.song_title = re.sub('<.+?>', '', self.song_title, 0).strip()
                self.song_artist = str(self.soup.select('div.song_header > div.information > em'))
                self.song_artist = re.sub('<.+?>', '', self.song_artist, 0).strip()

                self.auc_date_1 = str(self.soup.select('div:nth-child(1) > h2 > small'))
                self.auc_date_1 = re.sub('<.+?>', '', self.auc_date_1, 0).strip()
                self.auc_stock_1 = str(self.soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(2)'))
                self.auc_stock_1 = re.sub('<.+?>', '', self.auc_stock_1, 0).strip()
                self.auc_price_1 = str(self.soup.select('div.card_body > div > div:nth-child(1) > dl > dd:nth-child(8)'))
                self.auc_price_1 = re.sub('<.+?>', '', self.auc_price_1, 0).strip()

                self.auc_date_2 = str(self.soup.select('div:nth-child(2) > h2 > small'))
                self.auc_date_2 = re.sub('<.+?>', '', self.auc_date_2, 0).strip()
                self.auc_stock_2 = str(self.soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(2)'))
                self.auc_stock_2 = re.sub('<.+?>', '', self.auc_stock_2, 0).strip()
                self.auc_price_2 = str(self.soup.select('div.card_body > div > div:nth-child(2) > dl > dd:nth-child(8)'))
                self.auc_price_2 = re.sub('<.+?>', '', self.auc_price_2, 0).strip()

                self.song_date = str(self.soup.select('div.card_body > div > dl > dd:nth-child(2)'))
                self.song_date = re.sub('<.+?>', '', self.song_date, 0).strip()

                #stock_num 앞뒤 공백 제거 후 숫자만 추출(어떻게 간추릴 수 있을지 고민 중)
                self.stock_num = str(self.soup.select('div.card_body > div > dl > dd:nth-child(20) > p:nth-child(1)'))
                self.stock_num = re.sub('<.+?>', '', self.stock_num, 0).strip()
                self.stock_num = self.stock_num.replace('\t','').replace('\n','').replace('1/','').replace(',','')

                self.collect_db()


    #def name_classifier

    def collect_db(self, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

        print("{0}번째 노래 클라우드 입력 중".format(self.num))

        table = dynamodb.create_table(
            TableName='music_cow',
            KeySchema=[
                {
                    'AttributeName': 'song_title',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'num',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
    return table


        table = dynamodb.Table('Music_cow')
        response = table.put_item(
            Item={
                'num': self.num,
                'page': self.page,
                'song_title': self.song_title,
                'song_artist': self.song_artist,
                'auc1_info':{
                    'auc_date': self.auc_date_1, 'auc_stock': self.auc_stock_1, 'auc_price': self.auc_price_1},
                'auc2_info':{
                    'auc_date': self.auc_date_2, 'auc_stock': self.auc_stock_2, 'auc_price': self.auc_price_2},
                'auc_song_date': self.song_date,
                'stock_num': self.stock_num

                }
            )
        return response


if __name__ == '__main__':
    MusicList()