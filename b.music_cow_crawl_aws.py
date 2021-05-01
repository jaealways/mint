import requests
import re
from bs4 import BeautifulSoup
from pprint import pprint
import boto3

count=0
for num in range(0,2000):
    page = "https://www.musicow.com/song/{0}?tab=info".format(num)
    url = requests.get(page)
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')
    song_title = str(soup.select('div.song_header > div.information > p > strong'))
    song_title = re.sub('<.+?>', '', song_title, 0).strip()
    if song_title[1:-1]=='':
        pass
    else:
        count+=1
        list.append(num)



def music_cow_db(page, song_title, plot, rating, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('Music_cow')
    response = table.put_item(
        Item={
            'year': year,
            'title': title,
            'info': {
                'plot': plot,
                'rating': rating
            }
        }
    )
    return response


if __name__ == '__main__':
    movie_resp = put_movie("The Big New Movie", 2015,
                           "Nothing happens at all.", 0)
    print("Put movie succeeded:")
    pprint(movie_resp, sort_dicts=False)
