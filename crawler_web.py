from crawler_v1 import *
from crawler_db_fun import *


class CrawlerWeb:
    def __init__(self, crawl_num):
        self.crawl_num = int(crawl_num)
        self.variable_setting()

    def variable_setting(self):
        if self.crawl_num == 1:
            print('뮤직카우 곡 리스트 크롤링을 시작합니다.')
            self.list_music_cow()
        elif self.crawl_num == 2:
            print('뮤직카우 Daily 크롤링을 시작합니다.')
            self.daily_music_cow()
        elif self.crawl_num == 3:
            print('유튜브 동영상 리스트 크롤링을 시작합니다.')
            self.list_youtube()
        elif self.crawl_num == 4:
            print('유튜브 Daily 크롤링을 시작합니다.')
            self.daily_youtube()
        elif self.crawl_num == 5:
            print('유튜브 Daily 크롤링을 시작합니다.')
            self.comment_youtube()
        elif self.crawl_num == 6:
            print('지니 곡 리스트 크롤링을 시작합니다.')
            self.list_genie()
        elif self.crawl_num == 7:
            print('지니 Daily 크롤링을 시작합니다.')
            self.daily_genie()
        else:
            print('========= 올바른 번호를 입력해주세요 ==========')
            CrawlerV1()

    def list_music_cow(self):
        if self.check_exist():
            print('check complete')

    # def check_exist(self):
    #     is_exist


if __name__ == '__main__':
    print('crawler_v1.py로 실행합니다.')
    CrawlerV1()