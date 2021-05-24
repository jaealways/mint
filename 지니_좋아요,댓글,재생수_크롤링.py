url='https://www.genie.co.kr/detail/songInfo?xgnm=16158949'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"}

res=requests.get(url, headers= headers)
soup=BeautifulSoup(res.text, 'html.parser')

total_listener=soup.select('div.total p')[0].text
total_play=soup.select('div.total p')[1].text

like=soup.select_one('em#emLikeCount').text

reply=soup.select_one('#replyTotalCnt').text

#reply가 0으로 출력됨..
print(total_listener, total_play,like,reply)
