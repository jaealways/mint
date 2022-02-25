# << MCPI 지수 크롤링 코드 >>
# 작성자 : 정예원
#
# 코드설명
# [[ 이전에 크롤링한 날짜 이후에 쌓인 MCPI 데이터를 크롤링합니다 ]]
# 1. 클래스의 객체를 생성함과 동시에 현재 mcpi 라는 콜렉션에 mcpi 데이터가 있는지. 있다면 현재 무슨 날짜까지(currentData) 데이터가 적재되어있는지 파악합니다.
# 2. 'mcpi' 메소드를 수행하면 적재된 날짜 이후 날짜 부터 현재 날짜 까지의 mcpi 데이터를 크롤링 합니다.
# 3. 'mcpi_to_mongo' 메소드를 수행하면 크롤링된 딕셔너리형 데이터(self.mcpiDict)를 디비에 넣을 수 있는 형식(self.mcpiDictTransformedSorted) 으로 변환하여 디비에 저장합니다.
#
# 코드 수행시간 : 최대 5분


from pymongo import MongoClient
from selenium import webdriver
import time
from pandas.io.json import json_normalize


class McpiCrawler:
    def __init__(self, col2):
        self.mcpiDict = {}  # 크롤링한 mcpi 지수가 저장되는 딕셔너리
        self.col2 = col2    # mcpi 콜렉션
        self.readDB()       # 객체 생성과 동시에 이전에 크롤링한 mcpi 데이터가 현재 디비에 있는지/없는지 디비를 read함.

    def readDB(self):
        currentData = self.col2.find({})        # 현재 mcpi 디비에 있는 데이터 조회
        df = json_normalize(currentData)
        if df.empty:
            self.currentDate = None     # 현재 디비에 mcpi 데이터가 하나도 없는 경우 (mcpi 크롤링이 처음인 경우)
        else:
            self.currentDate = df.columns[len(df.columns)-1][2:].replace("-", ".")      # 현재까지 디비에 모은 mcpi 지수 가장 최근 날짜


    # 크롤링 완료한 mcpi 딕셔너리를 몽고디비에 추가
    def mcpi_to_mongo(self):

        self.mcpiDictTransformed = {}       # yy.mm.dd -> 20yy-mm-dd 형식으로 바꾼 딕셔너리
        for k, v in self.mcpiDict.items():
            new_k = ('20' + k).replace('.', '-')
            self.mcpiDictTransformed[new_k] = v

        mcpiDictTransformedSortedtuple = sorted(self.mcpiDictTransformed.items(), reverse=False)       # 날짜 오름차순 정렬
        self.mcpiDictTransformedSorted = dict((x,y) for x,y in mcpiDictTransformedSortedtuple)

        if self.currentDate == None:        # mcpi 처음 크롤링 시 디비에 insert
            self.col2.insert_one(self.mcpiDictTransformedSorted)
        else:
            self.col2.update_one({},{'$set': self.mcpiDictTransformedSorted}, upsert=True)


    # mcpi 크롤링
    def mcpi(self):

        driver = webdriver.Chrome(executable_path='./chromedriver.exe')
        URL = 'https://www.musicow.com/mcpi'
        driver.get(url=URL)
        time.sleep(1)

        buttons = [
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[2]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[3]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[4]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[5]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[6]'),
            driver.find_element_by_class_name('num_box')
        ]

        p = 0       # 크롤링 완료한 페이지 count

        # 반복문으로 mcpi 페이지 크롤링
        while(1):

            flag = 0

            for i in range(0,5):

                # tbl_mcpi > table > tbody > tr:nth-child(2) > td.mcpi
                # tbl_mcpi > table > tbody > tr:nth-child(2) > td.date.only_pc
                date_list = driver.find_elements_by_class_name('date.only_pc')
                mcpi_list = driver.find_elements_by_class_name('mcpi')

                for k in range(0,len(date_list)):

                    # 이미 디비에 있었던 mcpi 지수 가장 최근 날짜 다음날 까지 크롤링 완료시 크롤링 멈춤
                    # (ex. 디비에 22.02.25 (=self.currentData) 까지 있고 22.02.28 에 크롤링 시
                    # 22.02.28, 22.02.27, 22.02.26까지 긁고 22.02.25 긁기 전에 멈춤
                    if self.currentDate == date_list[k].text:
                        flag = 1
                        break

                    self.mcpiDict[date_list[k].text] = float(mcpi_list[k].text)

                if flag == 1:
                    break

                print('{}번 페이지 완료'.format(p+1))

                if i==4:
                    print('======')

                # 2019/01/01 날짜까지 크롤링 (19.01.01 ~ 현재 일자 데이터밖에 없기 때문)
                if date_list[len(date_list)-1].text == '19.01.01' :
                    flag = 1
                    break

                time.sleep(1)
                buttons[i].click()

                p = p+1
                time.sleep(1)

            if flag == 1:
                break

        print("========== << mcpi 크롤링을 마쳤습니다 >> ==========")
        driver.close()

        #print(self.mcpiDict)   # 긁은 mcpi 출력


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    # music cow
    col2 =  db1.mcpi


    McpiCrawler()
