from urllib.request import urlopen
from bs4 import BeautifulSoup

present_value = []
for i in range(2000):
    html = urlopen("https://www.musicow.com/song/{}?tab=price".format(i))
    soup = BeautifulSoup(html, "html.parser")

    #price = soup.select('#market_table > table > tbody span')

    price = soup.find('strong', attrs = {'class' : 'amt_market_latest'})
    title = soup.find('p', attrs = {'class' : 'title'}).find('strong')

    #market_table > table > tbody > tr> > span

    #market_table > table > tbody span

    if price.get_text()[0] == '0':
        continue

    presentValue = price.get_text().replace(',','').replace('캐쉬','')
    present_value.append(presentValue)

    print("{}번곡 {}".format(i, title.get_text()))
    print("                                현재가 : {}".format(presentValue))