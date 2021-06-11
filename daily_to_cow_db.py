from pymongo import MongoClient
from datetime import datetime

class DailyToCowDB:
    def __init__(self):
        self.date_today = datetime.now().strftime('%Y-%m-%d')
        # self.date_today = '2021-06-06'
        self.db_youtube()
        self.db_genie()
        self.db_music_cow()

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
        list_db_gen_daily = col6.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_gen_daily:
            num = x['num']
            self.date_data = x['{0}'.format(self.date_today)]
            col5.update_one({'num': num}, {'$set': {self.date_today: self.date_data}}, upsert=True)
        col6.delete_many({})


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