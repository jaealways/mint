import requests
import re
from selenium import webdriver
from bs4 import BeautifulSoup

from crawler_v1 import *
from crawler_db_fun import DBRead as Dr
from crawler_db_fun import DBWrite as Dw



class CrawlerWeb:
    def __init__(self, crawl_num):
        self.crawl_num = int(crawl_num)
        # self.daily_to_cow_db()
        # self.list_music_cow()
        # self.list_youtube()
        self.variable_setting()
        # self.update_to_git
        # self.cow_to_copy_db()

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
        for num in range(24, 28):
            num_check, to_check_list = Dr.db_check_exist('music_cow', 'music_list', num)
            if num_check != 0:
                for to_check in to_check_list:
                    if to_check['num'] == num:
                        print("%s 번 곡은 이미 DB에 존재합니다." % num)
                        pass
                    else:
                        print("DB 입력 값 %s과 홈페이지 넘버 %s이 일치하지 않습니다." % (to_check['num'], num))
                        raise IndexError
            else:
                page = "https://www.musicow.com/song/{0}?tab=info".format(num)
                tag_music_list = [['song_artist', 'div.song_header > div.information > em', 1, 0, 'NaN'],
                                  ['auc_date_{0}', 'div > div:nth-of-type({0}) > h2 > small', 2, 0, 'NaN'],
                                  ['auc_stock_{0}', 'div.card_body > div > div:nth-of-type({0}) > dl > dd:nth-of-type(1)', 2, 0, '주'],
                                  ['auc_price_{0}', 'div.card_body > div > div:nth-of-type({0}) > dl > dd:nth-of-type(4)', 2, 0, '캐쉬'],
                                  ['song_release_date', 'div.card_body > div > dl > dd:nth-of-type(1)', 1, 0, 'NaN'],
                                  ['stock_num', 'div.card_body > div > dl > dd > p:nth-of-type(1)', 1, 0, '1/'],
                                  ['copy_revenue_{0}', '#graph1 > span:nth-of-type({0})', 12, 'data_var_value', 'Nan'],
                                  ['relevant_song_{0}', 'div.card_body > div > div.lst_rcmd.swiper-wrapper > div:nth-of-type({0}) > a:nth-of-type(2)', 10, 'href', '/song/']]

                print("{0}번 곡 뮤직카우 크롤링 시작".format(num))
                soup = self.crawl_soup(page=page)
                out_music_list = dict(self.crawl_variable(soup, tag_music_list, not_tag='song_artist', num=num))
                Dw.db_write_value(self, db='music_cow', col='music_list', dict_list=out_music_list, num=num)


        # [[print(num) for num in range(0, 2500) if num in read['num']] for read in read_list]

    def daily_music_cow(self):
        read_list = Dr.db_read_value(self, 'music_cow', 'music_list_split')

    def list_youtube(self):
        read_list = Dr.db_read_value(self, 'music_cow', 'music_list_split')

    def daily_youtube(self):
        read_list = Dr.db_read_value(self, 'music_cow', 'youtube_list')

    def comment_youtube(self):
        read_list = Dr.db_read_value(self, 'music_cow', 'youtube_list')

    def list_genie(self):
        read_list = Dr.db_read_value(self, 'music_cow', 'music_list_split')

    def daily_genie(self):
        read_list = Dr.db_read_value(self, 'music_cow', 'genie_list')

    def crawl_soup(self, page=None, web_op='rq'):
        if web_op == 'sl':
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--incognito')
            driver = webdriver.Chrome(options=chrome_options)

            soup = BeautifulSoup(driver.get(page).text, 'html.parser')
        else:
            soup = BeautifulSoup(requests.get(page).text, 'html.parser')
        return soup

    def crawl_variable(self, soup=None, tag_list=None, not_tag=None, num=0):
        mode = 'keep'
        for tag in tag_list:
            if mode == 'pass':
                break
            for n in range(1, tag[2]+1):
                if tag[3] == 0:
                    soup_input = soup.select(tag[1].format(n))
                else:
                    soup_input = soup.select(tag[1].format(n))[0].attrs['%s' % tag[3]]
                crawl_result = re.sub('\<.+?>|\[|\'|\]|\,', '', str(soup_input)).replace('%s' % tag[4], '').strip()
                tag_name = tag[0].format(n)
                if tag_name == not_tag:
                    if crawl_result == '':
                        print('%s 번은 존재하지 않는 곡입니다.' % num)
                        mode = 'pass'
                        break
                    else:
                        tag_name, crawl_result
                else:
                    tag_name, crawl_result
                if crawl_result == '':
                    pass
                yield tag_name, crawl_result



    # def check_crawl_exist(self, tag, crawl_result, num, not_tag=None):
    #     if tag == not_tag:
    #         if crawl_result == '':
    #             print('%s 번은 존재하지 않는 곡입니다.' % num)
    #             pass # break
    #     else:
    #         print(tag, crawl_result)


    # def crawl_var(self, soup=None):
    #
    #
    #     song_title = str(soup.select('div.song_header > div.information > p > strong'))
    #     song_title = re.sub('\<.+?>|\[|\]', '', song_title, 0).replace('&amp;', '&').strip()
    #
    #     if song_title[0:-1] == '':
    #         print("{0}번 곡은 존재하지 않습니다.".format(num))
    #         pass
    #     else:
    #         print(song_title)
    #
    # def music_list_split(self):
    #     # ???
    # def check_exist(self):
    #     is_exist


if __name__ == '__main__':
    print('crawler_v1.py로 실행합니다.')
    CrawlerV1()