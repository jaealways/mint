from pymongo import MongoClient
from datetime import datetime

class DailyToCowDB:
    def __init__(self):
        # self.date_today = datetime.now().strftime('%Y-%m-%d')
        self.date_today = '2021-06-29'
        self.db_youtube()


    def db_youtube(self):
        for num in range(7422, 7831):
            list_db_gen_daily = col1.find({'video_num': num})
            list_db_mc = col1.find({'video_num': num})
            for x in list_db_gen_daily:
                self.date_data_0610 = x['2021-06-28']
                self.name_0610 = x['title_video']

                break
            for y in list_db_mc:
                self.date_data_0608 = y['2021-06-30']
                self.name_0608 = y['title_video']

                if self.name_0608 is None:
                    break

                # if self.name_0608 != self.name_0610:
                #     print('{0}번 비디오 에러!'.format(num))
                #     # raise IndexError
                #     col1.delete_one({'video_num': num})
                #     col1.insert_one(x).inserted_id
                #     break

                else:
                    for z in self.date_data_0608:
                        self.date_data_0609 = self.date_data_0608
                        self.date_data_0609[z] = (int(self.date_data_0608[z]) + int(self.date_data_0610[z])) / 2

                print('db에 {0}번 곡 업로드 중'.format(num))
                col1.update_one({'video_num': num}, {'$set': {'2021-06-29': self.date_data_0609}}, upsert=True)


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    # daily youtube update
    col1 = db1.daily_youtube
    col2 = db2.daily_youtube
    # daily genie update
    col3 = db1.daily_genie
    col4 = db2.daily_genie
    # daily youtube update
    col5 = db1.daily_music_cow
    col6 = db2.daily_music_cow

    DailyToCowDB()
###JYW
## LJH
