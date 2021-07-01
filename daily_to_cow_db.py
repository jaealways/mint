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
        # 쿠글 2팀 이윤수



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
            col1.update_
