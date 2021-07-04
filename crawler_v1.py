import sys
from datetime import datetime
import crawler_web as cw


class CrawlerV1:
    def __init__(self):
        if len(sys.argv) == 1:
            self.print_info()
        elif len(sys.argv) == 2:
            self.crawl_num = sys.argv[1]
        self.input_value()

    def print_info(self):
        date_today = datetime.now().strftime('%Y-%m-%d')
        print('크롤러 v1 시작', '오늘은 %s 입니다. 다음 중 원하는 번호를 (숫자만) 입력하세요.' % date_today,
              '1: 뮤직카우 곡 리스트 크롤링', '2: 뮤직카우 Daily 크롤링', '3: 유튜브 동영상 리스트 크롤링',
              '4: 유튜브 Daily 크롤링', '5: 유튜브 댓글 크롤링', '6: 지니 곡 리스트 크롤링', '7: 지니 Daily 크롤링', sep='\n')
        self.crawl_num = int(input("크롤링할 번호를 입력하세요: "))

    def input_value(self):
        cw.CrawlerWeb(self.crawl_num)


if __name__ == "__main__":
    CrawlerV1()
