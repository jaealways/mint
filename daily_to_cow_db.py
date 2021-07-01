from pymongo import MongoClient
from datetime import datetime
from bson.json_util import dumps
import subprocess as cmd

class DailyToCowDB:
    def __init__(self):
        self.date_today = datetime.now().strftime('%Y-%m-%d')
        # self.date_today = '2021-06-06'
        self.upload_github()
        # self.db_youtube()
        # self.db_genie()
        # self.db_music_cow()
        # self.db_back_up()
        # 쿠글 2팀 작업

    def upload_github(self):
        # github 업로드
        for x in [col2, col4, col6]:
            list_db_you_daily = list(x.find({}))
            json_you_daily = dumps(list_db_you_daily, indent=2)
            with open('daily_youtube#{0}.json'.format(self.date_today), 'w') as file:
                file.write(json_you_daily)


        # list_db_gen_daily = list(col4.find({}))
        # list_db_muc_daily = list(col6.find({}))
        #
        # json_gen_daily = list_db_gen_daily.to_json("daily_genie-{0}.json".format(self.date_today))
        # json_muc_daily = list_db_muc_daily.to_json("daily_music_cow-{0}.json".format(self.date_today))

    def db_youtube(self):
        list_db_you_daily = col2.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_you_daily:
            video_num = x['video_num']
            self.date_data = x['{0}'.format(self.date_today)]
            col1.update_one({'video_num': video_num}, {'$set': {self.date_today: self.date_data}}, upsert=True)
        col2.delete_many({})

    def db_genie(self):
        list_db_gen_daily = col4.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_gen_daily:
            link = x['link']
            self.date_data = x['{0}'.format(self.date_today)]
            col3.update_one({'link': link}, {'$set': {self.date_today: self.date_data}}, upsert=True)
        col4.delete_many({})

    def db_music_cow(self):
        list_db_muc_daily = col6.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_muc_daily:
            num = x['num']
            self.date_data = x['{0}'.format(self.date_today)]
            col5.update_one({'num': num}, {'$set': {self.date_today: self.date_data}}, upsert=True)
        col6.delete_many({})

    # def db_back_up(self):
    #     for i in range(1,7):
    #         ['col{}'.format(i)].aggregate([{'$match':{}}, {'$out': ['col{}'.format(i)]}])


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db1 = client.music_cow
    db2 = client.daily_crawler
    db3 = client.music_cow_backup
    # daily youtube update
    col1 = db1.daily_youtube
    col2 = db2.daily_youtube
    # daily genie update
    col3 = db1.daily_genie
    col4 = db2.daily_genie
    # daily youtube update
    col5 = db1.daily_music_cow
    col6 = db2.daily_music_cow
    # back_up db collection

    DailyToCowDB()