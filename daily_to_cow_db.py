from pymongo import MongoClient
from datetime import datetime
from bson.json_util import dumps, loads
import subprocess as cmd
import json
import git

class DailyToCowDB:
    def __init__(self):
        self.date_today = datetime.now().strftime('%Y-%m-%d')
        # self.date_today = '2021-06-06'
        self.export_json()
        self.pull_github()
        # self.push_github()
        # self.db_youtube()
        # self.db_genie()
        # self.db_music_cow()
        # self.db_back_up()

    def export_json(self):
        x = col2 # 각자 맡은 col 숫자 변경해서 입력
        list_db_daily = list(x.find({}))
        json_daily = dumps(list_db_daily, indent=2)
        self.json_name = '%s#%s.json' % (x.name, self.date_today)
        with open('%s' % self.json_name, 'w') as file:
            file.write(json_daily)

    def pull_github(self):
        message = 'Update_Daily_JSON_%s' % self.date_today

        cmd.run("cd c:/music_cow", check=True, shell=True) # 각자 로컬에 저장한 곳 입력
        cmd.run("git reset HEAD", check=True, shell=True)
        cmd.run("git add %s" % self.json_name, check=True, shell=True)
        # cmd.run("git config credential.helper store", check=True, shell=True)
        # cmd.run("git push https://github.com/jaealways/music_cow.git, check=True, shell=True)
        cmd.run("git commit -m {0}".format(message), check=True, shell=True)
        cmd.run("git push -u origin master -f", check=True, shell=True)

    def push_github(self):
        cmd.run("cd c:/music_cow", check=True, shell=True) # 각자 로컬에 저장한 곳 입력
        # cmd.run("git config core.sparseCheckout true", check=True, shell=True)
        # cmd.run("git remote add -f origin https://github.com/jaealways/music_cow.git", check=True, shell=True)
        cmd.run("git pull origin master")
        # for x in [col2, col4, col6]:
            # self.json_name = '%s#%s.json' % (x.name, self.date_today)
            # cmd.run("echo jaealways/music_cow/%s > c:/music_cow" % self.json_name)
            # cmd.run("vi ./git/info/sparse-checkout")
        for x in [col2, col4, col6]:
            self.json_name = '%s#%s.json' % (x.name, self.date_today)
            with open('%s' % self.json_name, 'r', encoding='UTF8') as file:
                json_file = loads(file.read())

                for doc in json_file:
                    x.insert_one(doc)

        # client.close()

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