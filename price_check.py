from pymongo import MongoClient

client = MongoClient('localhost', 27017)

db1 = client.music_cow
col1 = db1.music_cow_ratio

def read_db():
    list_db = col1.find({})
    print(list_db)
    for x in list_db:
        for n in list(x)[2:]:
            if x[n] > 50:
                print(x['num'], n, x[n])
            else:
                pass
# type(x[n])==int and
read_db()