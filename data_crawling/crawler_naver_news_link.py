import datetime
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient


class daily_Naver:
    def __init__(self):
        self.article_num = 0
        self.read_db()
        self.count = 0

    def read_db(self):
        self.link_num = 0
        artist_list = ['가수 별', '일리네어 레코즈', '임창정', '가수 아이비', '씨야 성유진', '김장훈', '아이돌 인피니트', '한동근',
                       '다비치', '90년대 가수 심신', '발라드 리사', '가수 박원', '현아', '효린', '지드래곤', '진해성', 'NS윤지',
                       '아이돌 여자친구', '자이언티', '가수 구창모', '마이티마우스', '김홍우 레디', '아이즈원', '정유지', '태진아',
                       '2NE1', '아이유', '규현', '가수 훈스', '기리보이', '화요비', '가수 이수영', '가수 원모어찬스', '가수 소방차',
                       '이승철', '김재중', '가수 칵스', '박명수 지드래곤 GG', '소향', '가수 일락', '천상지희 더 그레이스', '쿨 이재훈',
                       '가수 벤', '지천비화', '악동뮤지션', '투개월', '전효성', '가수 김태우', '가수 10CM', '뉴이스트', '김필', '씨잼',
                       '가수 고유진', '가수 박혜경', '빅뱅', '유세윤 UV', '변진섭', '리쌍', '양혜승', '유재석 X Dok2', '가수 렉시',
                       '티아라', '엑소', '샤이니', '백예린', '모모랜드', '광해 왕이 된 남자', '신민아 영화 키친', '공유 영화 용의자',
                       '선미', '가수 박지훈', 'GD&TOP', 'SG워너비', '아이돌 에이프릴', '박봄', '정기고', '코요태', 'B1A4', '발라드 지아',
                       '가수 정인', '바비킴', '가수 김현성', '가수 제로 박성철', '파이브돌스', '가수 수지', '스테디 이현경', '제국의아이들',
                       '오투포', '가수 강수지', '매드클라운', '비투비', '송하예', '가수 거미', '정한해', '하동균', '아이콘 바비', '빅뱅 태양',
                       '엑소 수호', '노을 전우성', '케이윌', '제리케이', '시크릿 송지은', '루그', '래퍼 크러쉬', '김나희', '워너원',
                       '크레용팝', '발라드 버즈', '조성모', '아월', '업텐션 이진혁', '가수 라디', '컨츄리꼬꼬', '마마무', '래퍼 팔로알토',
                       '가수 김보경', 'yg 아이콘', '가수 황보', '래퍼 박주석', '양요섭', '애즈원', 'MC몽', '이병헌', '길구봉구',
                       '씨스타 소유', '뮤직카우']


        for idx, artist in enumerate(artist_list):
            # self.num = idx
            self.num = 30
            self.keyword = artist
            print('{0}검색 시작'.format(artist))
            self.listing_article()

    def listing_article(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
        today = datetime.date.today()
        # today = datetime.date(2020, 8, 9)

        stop = '2019-01-01'
        while 1:

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
                            col.insert_one(self.articles).inserted_id
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
                                    col.insert_one(self.articles).inserted_id
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
                                    col.insert_one(self.articles).inserted_id
                                    print(self.articles)
                                    print('{0}.번째 링크 정보 기입됨'.format(self.link_num))

                            except:
                                pass
                        self.link_num += 1


                else:
                    break
            today += datetime.timedelta(-1)
            if today.strftime('%Y-%m-%d') == stop:
                break


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)

    db = client.article
    col = db.article_info

    daily_Naver()

