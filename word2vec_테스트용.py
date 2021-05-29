import requests
from gensim.models.word2vec import Word2Vec
import re
from bs4 import BeautifulSoup
df=pd.read_excel('./test_search.xlsx')
url_poly='https://search.naver.com/search.naver'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}
count=0
sentences=[]
for i in df:

    url='https://search.naver.com/search.naver?where=news&query={0}&sm=tab_opt&sort=1&photo=0&field=0&pd=4&ds=&de=&docid=&related=0&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so%3Add%2Cp%3A1d'.format(i)

    res=requests.get(url, headers= headers)
    soup=BeautifulSoup(res.text, 'html.parser')

    pages=soup.select('div.sc_page_inner a')
    try:
        for page in pages:
            link=page.attrs['href']
            temp_res=requests.get(url_poly+link, headers=headers)
            temp_soup=BeautifulSoup(temp_res.text, 'html.parser')
            cons=temp_soup.select('ul.list_news li')
            for con in cons:

                try:
                    temp=con.select('a.info')[1]
                    link=temp.attrs['href']
                    temp_res=requests.get(link, headers=headers)
                    temp_soup=BeautifulSoup(temp_res.text, 'html.parser')
                    text=temp_soup.select('div.end_body_wrp>div')

                    split=text[0].text.split('.')
                    for i in split:
                        sentences.append(re.sub(r'[^ ㄱ-ㅣ가-힣A-Za-z]', '', i))


                    count+=1

                    if temp_soup==[]:
                        print(link)

                except:
                    count+=1
    

    except:
        pass

print('기사 총 개수:',count)


from kiwipiepy import Kiwi,Option
kiwi=Kiwi()
kiwi.prepare()
a['tokenized']=a['text'].apply(lambda x: kiwi.analyze(x))
tokenized_list=[]
for i in a['tokenized']:
  temp=[]
  for j in i[0][0]:
    temp.append(j[0])
    
  tokenized_list.append(temp)

word2vec=Word2Vec(sentences=tokenized_list,size=100,iter=100,window=10,workers=4, min_count=10, sg=1)

word2vec.most_similar('역주행',topn=30)

