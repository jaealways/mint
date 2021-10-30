from pymongo import MongoClient
import selenium
from selenium import webdriver
import time
import pickle
from time import sleep
import time

class MCPICrawler:
    def __init__(self):
        #self.MCPI()
        self.MCPI_to_Mongo()

    def MCPI_to_Mongo(self):
        with open('MCPI.pkl', 'rb') as f:
            MCPI = pickle.load(f)



            print(MCPI)

        # col4.update_one({'Title': 'MCPI'},{'$set': MCPI}, upsert=True)




    def MCPI(self):

        #list_db_gen_daily = col1.find({'num':{'$gte':204}})  204번 부터

        driver = webdriver.Chrome(executable_path='./chromedriver.exe')


        #self.copyright_price[x] = [[],[],[],[],[]]

        URL = 'https://www.musicow.com/mcpi'

        driver.get(url=URL)

        time.sleep(1)

        # tbl_mcpi > div.paging > a.page-item.cur_num

        mcpi_dic = {}

        # 일반 페이지
        button_list = [
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[2]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[3]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[4]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[5]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[6]'),
        ]

        # 마지막 페이지
        button_list2 = [
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[2]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[3]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[4]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[5]')
        ]

        buttons = [
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[2]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[3]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[4]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[5]'),
            driver.find_element_by_xpath('//*[@id="tbl_mcpi"]/div[2]/a[6]'),
            driver.find_element_by_class_name('num_box')
        ]


        while(1):

            flag = 0

            for i in range(0,5):

                # tbl_mcpi > table > tbody > tr:nth-child(2) > td.mcpi
                # tbl_mcpi > table > tbody > tr:nth-child(2) > td.date.only_pc
                date_list = driver.find_elements_by_class_name('date.only_pc')
                mcpi_list = driver.find_elements_by_class_name('mcpi')

                for k in range(0,5):
                    mcpi_dic[date_list[k].text] = float(mcpi_list[k].text)

                 #col4.update_one({'date': date}, {'$set': {'mcpi': date}}, upsert=True)

                # date = driver.find_elements_by_css_selector("# tbl_mcpi > table > tbody > tr:nth-child(2) > td.date.only_pc".format(j))[0].get_attribute("date.only_pc")
                # mcpi = driver.find_elements_by_css_selector("# tbl_mcpi > table > tbody > tr:nth-child(2) > td.mcpi".format(j))[0].get_attribute("mcpi")
                #self.copyright_price[x][i].append(price)



                # tbl_mcpi > div.paging > a:nth-child(3)
                # tbl_mcpi > div.paging > a:nth-child(4)
                # tbl_mcpi > div.paging > a:nth-child(7)
                # tbl_mcpi > div.paging > a:nth-child(3)

                # button = driver.find_element_by_css_selector('# tbl_mcpi > div.paging > a:nth-child({})'.format(i))




                # if int(button_list[i].text) == 196:
                #      button_list2[i].click()
                #      time.sleep(2)


                print('{}번 페이지 완료'.format(i+1))

                if i==4:
                    print('======')

                if buttons[i].text == '200':
                    flag = 1
                    break


                time.sleep(1)

                buttons[i].click()

                time.sleep(1)

                # for b in button_noncurrent:
                #     if int(b.text) == int(button_current) + 1:
                #         b.click()
                #         break
                #     else:
                #         pass

            if flag == 1:
                break

        print('3')
        driver.close()

        #col4.update_one({'$set': mcpi_dic}, upsert=True)

        # pickle 로 만들기
        with open('MCPI.pkl','wb') as f:
            pickle.dump(mcpi_dic, f)

        print(mcpi_dic)
        print('3')

        # 완성된 저작권료 딕셔너리 print
        #print(self.copyright_price)



        # pickle 로 만들기
        #with open('copyright_price.pkl','wb') as f:
            #pickle.dump(self.copyright_price, f)





if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    # music cow
    col1 = db1.daily_music_cow
    col2 = db1.music_cow_ratio
    col3 = db1.music_list
    col4 = db1.MCPI


    MCPICrawler()
