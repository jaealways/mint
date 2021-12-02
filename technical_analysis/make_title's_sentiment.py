import pandas as pd
from pymongo import MongoClient
import numpy as np
from kiwipiepy import Kiwi
import matplotlib.pyplot as plt
from collections import Counter
from sklearn.model_selection import train_test_split
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
import re
from tensorflow.keras.layers import Embedding, Dense, LSTM, Bidirectional
from tensorflow.keras.models import Sequential
from tensorflow.keras.models import load_model
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint


class BiLSTM:
    def __init__(self):
#         self.price_ratio()
#         self.price_binary()
        self.data_merge()
#         self.data_clean()
#         self.sentiment_predict()

    def price_ratio(self):
        list_db_gen_daily = col1.find({}, {'num': {"$slice": [1, 1]}})
        for x in list_db_gen_daily:
            ratio = []
            for price in range(4, len(x)-1):
                ratio.append(int(list(x.values())[price].get('price')))
                rate_of_change = ((int(list(x.values())[price+1].get('price')) - int(list(x.values())[price].get('price')))
                                  / int(list(x.values())[price].get('price'))) * 100
                col2.update_one({'num': x['num']}, {'$set': {list(x.keys())[price]: rate_of_change}}, upsert=True)

    def price_binary(self):
        list_db = col2.find({})
        data = []
        df = pd.DataFrame(data, columns=['song_num', 'date', 'price_ratio'])
        print(list_db)
        for x in list_db:
            for n in list(x)[2:]:
                if abs(x[n]) > 8:
                    if x[n] > 0:
                        result = {'song_num': x['num'], 'date': n, 'price_ratio': x[n], 'label': 1}
                        print(result)
                        df = df.append(result, ignore_index=True)
                    else:
                        result = {'song_num': x['num'], 'date': n, 'price_ratio': x[n], 'label': 0}
                        print(result)
                        df = df.append(result, ignore_index=True)
                else:
                    pass
        print(df)
        print('전체 리뷰 개수 :', len(df))
        df.to_pickle('df_price_binary.pkl')

    def data_merge(self):
        label_data = pd.read_pickle('df_price_binary.pkl')
        print(label_data)
        data = []
        global df
        df = pd.DataFrame(data, columns=['song_num', 'date', 'title', 'label'])
        for song_num, date, label in zip(label_data['song_num'], label_data['date'], label_data['label']):
            list_db = col3.find({'num': song_num, 'date': (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')})
            
            for y in list_db:
                result = {'song_num': song_num, 'date': y['date'], 'title': y['article_title'], 'label': label}
                print(result)
                df = df.append(result, ignore_index=True)
        
        df.to_pickle('df_title.pkl')

    def data_clean(self):
        total_data = pd.read_pickle('df_title.pkl')
        train_data, test_data = train_test_split(total_data, test_size=0.25, random_state=42)
        print('훈련용 리뷰의 개수 :', len(train_data))
        print('테스트용 리뷰의 개수 :', len(test_data))
        train_data['label'].value_counts().plot(kind='bar')
        print(train_data.groupby('label').size().reset_index(name='count'))

        train_data['title'] = train_data['title'].str.replace("[^ㄱ-ㅎㅏ-ㅣ가-힣 ]","")
        train_data['title'].replace('', np.nan, inplace=True)
        print(train_data.isnull().sum())

        test_data.drop_duplicates(subset = ['title'], inplace=True) # 중복 제거
        test_data['title'] = test_data['title'].str.replace("[^ㄱ-ㅎㅏ-ㅣ가-힣 ]","")
        test_data['title'].replace('', np.nan, inplace=True)
        test_data = test_data.dropna(how='any')
        print('전처리 후 테스트용 샘플의 개수 :',len(test_data))

        stopwords = ['도', '는', '다', '의', '가', '이', '은', '한', '에', '하', '고', '을', '를', '인', '듯', '과', '와', '네',
                     '들', '듯', '지', '임', '게', '만', '되', '음', '면']

        kiwi = Kiwi()

        train_data['tokenized'] = train_data['title'].apply(lambda x: kiwi.analyze(x))
        train_data['tokenized'] = train_data['tokenized'].apply(lambda x: [item for item in x if item not in stopwords])
        test_data['tokenized'] = test_data['title'].apply(lambda x: kiwi.analyze(x))
        test_data['tokenized'] = test_data['tokenized'].apply(lambda x: [item for item in x if item not in stopwords])

        tokenized_list = []
        for i in train_data['tokenized']:
            temp = []
            for j in i[0][0]:
                temp.append(j[0])
            tokenized_list.append(temp)
        train_data['after_token'] = tokenized_list

        tokenized_list = []
        for i in test_data['tokenized']:
            temp = []
            for j in i[0][0]:
                temp.append(j[0])
            tokenized_list.append(temp)
        test_data['after_token'] = tokenized_list

        negative_words = np.hstack(train_data[train_data.label == 0]['after_token'].values)
        positive_words = np.hstack(train_data[train_data.label == 1]['after_token'].values)

        negative_word_count = Counter(negative_words)
        print(negative_word_count.most_common(20))

        positive_word_count = Counter(positive_words)
        print(positive_word_count.most_common(20))

        fig,(ax1,ax2) = plt.subplots(1,2,figsize=(10,5))
        text_len = train_data[train_data['label']==1]['after_token'].map(lambda x: len(x))
        ax1.hist(text_len, color='red')
        ax1.set_title('Positive Reviews')
        ax1.set_xlabel('length of samples')
        ax1.set_ylabel('number of samples')
        print('긍정 리뷰의 평균 길이 :', np.mean(text_len))

        text_len = train_data[train_data['label']==0]['after_token'].map(lambda x: len(x))
        ax2.hist(text_len, color='blue')
        ax2.set_title('Negative Reviews')
        fig.suptitle('Words in texts')
        ax2.set_xlabel('length of samples')
        ax2.set_ylabel('number of samples')
        print('부정 리뷰의 평균 길이 :', np.mean(text_len))
        plt.show()

        X_train = train_data['after_token'].values
        y_train = train_data['label'].values
        X_test= test_data['after_token'].values
        y_test = test_data['label'].values

        tokenizer = Tokenizer()
        tokenizer.fit_on_texts(X_train)

        threshold = 2
        total_cnt = len(tokenizer.word_index) # 단어의 수
        rare_cnt = 0 # 등장 빈도수가 threshold보다 작은 단어의 개수를 카운트
        total_freq = 0 # 훈련 데이터의 전체 단어 빈도수 총 합
        rare_freq = 0 # 등장 빈도수가 threshold보다 작은 단어의 등장 빈도수의 총 합

        # 단어와 빈도수의 쌍(pair)을 key와 value로 받는다.
        for key, value in tokenizer.word_counts.items():
            total_freq = total_freq + value

            # 단어의 등장 빈도수가 threshold보다 작으면
            if(value < threshold):
                rare_cnt = rare_cnt + 1
                rare_freq = rare_freq + value

        print('단어 집합(vocabulary)의 크기 :',total_cnt)
        print('등장 빈도가 %s번 이하인 희귀 단어의 수: %s'%(threshold - 1, rare_cnt))
        print("단어 집합에서 희귀 단어의 비율:", (rare_cnt / total_cnt)*100)
        print("전체 등장 빈도에서 희귀 단어 등장 빈도 비율:", (rare_freq / total_freq)*100)

        vocab_size = total_cnt - rare_cnt + 2
        print('단어 집합의 크기 :',vocab_size)

        tokenizer = Tokenizer(vocab_size, oov_token = 'OOV')
        tokenizer.fit_on_texts(X_train)
        X_train = tokenizer.texts_to_sequences(X_train)
        X_test = tokenizer.texts_to_sequences(X_test)

        print('리뷰의 최대 길이 :',max(len(l) for l in X_train))
        print('리뷰의 평균 길이 :',sum(map(len, X_train))/len(X_train))
        plt.hist([len(s) for s in X_train], bins=50)
        plt.xlabel('length of samples')
        plt.ylabel('number of samples')
        plt.show()

        cnt = 0
        max_len, nested_list = 25, X_train
        for s in nested_list:
            if(len(s) <= max_len):
                cnt = cnt + 1
        print('전체 샘플 중 길이가 %s 이하인 샘플의 비율: %s'%(max_len, (cnt / len(nested_list))*100))

        X_train = pad_sequences(X_train, maxlen = max_len)
        X_test = pad_sequences(X_test, maxlen = max_len)

        model = Sequential()
        model.add(Embedding(vocab_size, 100))
        model.add(Bidirectional(LSTM(100)))
        model.add(Dense(1, activation='sigmoid'))

        es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=4)
        mc = ModelCheckpoint('best_model.h5', monitor='val_acc', mode='max', verbose=1, save_best_only=True)

        model.compile(optimizer='rmsprop', loss='binary_crossentropy', metrics=['acc'])
        history = model.fit(X_train, y_train, epochs=15, callbacks=[es, mc], batch_size=256, validation_split=0.2)

        loaded_model = load_model('best_model.h5')
        print("테스트 정확도: %.4f" % (loaded_model.evaluate(X_test, y_test)[1]))
        print("테스트 정확도: %.4f" % (loaded_model.evaluate(X_test, y_test)[1]))

    def sentiment_predict(new_sentence):
        new_sentence = re.sub(r'[^ㄱ-ㅎㅏ-ㅣ가-힣 ]','', new_sentence)
        new_sentence = new_sentence.apply(lambda x: Kiwi.analyze(x))
        new_sentence = new_sentence.apply(lambda x: [item for item in x if item not in stopwords])
        tokenized_list = []
        stopwords = ['도', '는', '다', '의', '가', '이', '은', '한', '에', '하', '고', '을', '를', '인', '듯', '과', '와', '네',
                     '들', '듯', '지', '임', '게', '만', '되', '음', '면']
        for i in new_sentence:
            temp = []
            for j in i[0][0]:
                temp.append(j[0])
            tokenized_list.append(temp)
        new_sentence = tokenized_list

        new_sentence = [word for word in new_sentence if not word in stopwords] # 불용어 제거
        encoded = tokenizer.texts_to_sequences([new_sentence])
        pad_new = pad_sequences(encoded, maxlen = max_len)
        score = float(loaded_model.predict(pad_new))
        if(score > 0.5):
            print("{:.2f}% 확률로 긍정 리뷰입니다.".format(score * 100))
        else:
            print("{:.2f}% 확률로 부정 리뷰입니다.".format((1 - score) * 100))


client = MongoClient('localhost', 27017)
db1 = client.music_cow
db2 = client.article
col1 = db1.daily_music_cow
col2 = db1.music_cow_ratio
col3 = db2.article_title3

BiLSTM()
