"""
Scraping game metadata from gamespot
"""
import urllib2
from bs4 import BeautifulSoup
import difflib
import re
import json
import grequests
import requests
import pandas as pd
from dateutil.parser import parse
from datetime import datetime
import sys

from utils.mongo_conn import get_mongo_connection
from utils.utils import chunks

# gamespot game listing is not stable among pages
# thus try fetching game link listing n times
LIST_RETRIES = 1 # 0 to not fetching at all

client = get_mongo_connection('writer')
db = client.game_data

def insert_mongo(col_name, data):
    if len(data)>0:
        print '-- inserting %d rows to %s'% (len(data), col_name)
        db[col_name].insert(data)
    else:
        print '-- nothing to insert'
    
def main():
    #url_base = "http://www.gamespot.com/new-games/?sort=score&game_filter_type%5Bplatform%5D=19&game_filter_type%5Bgenres%5D=&game_filter_type%5BminRating%5D=&game_filter_type%5BtimeFrame%5D=&game_filter_type%5BstartDate%5D=&game_filter_type%5BendDate%5D=&game_filter_type%5Btheme%5D=&game_filter_type%5Bregion%5D=&game_filter_type%5Bletter%5D=&page="
    
    print '####'*10
    print '## Gamespot populator FIRED! %s ##' % str(datetime.now())
    # url with release date filter (>2012)
    url_base = "http://www.gamespot.com/new-games/?sort=score&game_filter_type%5Bplatform%5D=&game_filter_type%5B" \
            + "genres%5D=&game_filter_type%5BminRating%5D=&game_filter_type%5BtimeFrame%5D=4&game_filter_type%5B" \
            + "startDate%5D=01%2F01%2F2012&game_filter_type%5BendDate%5D=12%2F31%2F2016&game_filter_type%5Btheme%5D=&" \
            + "game_filter_type%5Bregion%5D=&game_filter_type%5Bletter%5D=&page="
    page = 1
    starting_url = url_base + str(page) 
    response = urllib2.urlopen(starting_url)
    soup = BeautifulSoup(response)
    domain = "http://www.gamespot.com"
    all_page_num = [int((domain + link.get('href')).replace(url_base,'')) for link in soup.find_all('a',{'class':'btn'})]
    page_num_max = max(all_page_num)
    print page_num_max , 'pages'
     
    all_pages = [url_base+str(i) for i in range(1,page_num_max+1)]
    all_game_links = db.gamespot.distinct('url')
    new_game_links = []
    print len(all_game_links), 'saved links'
    print 'Finding new games'
    import ipdb; ipdb.set_trace()
    for i in range(LIST_RETRIES): 
        for response in grequests.map((grequests.get(u) for u in all_pages)):
            soup = BeautifulSoup(response.text)
            game_links = [(domain + link.get('href')) for link in soup.find_all('a',{'class':'js-event-tracking'})]
            for l in game_links:
                if l not in all_game_links:
                    all_game_links.append(l)
                    new_game_links.append(l)
        print '---Round %d:'%(i+1), len(new_game_links), 'new games found'
    all_items = []
    #obj = retrive_game_info_by_link(requests.get('http://www.gamespot.com/bloodborne/'))
    #obj = retrive_game_info_by_link(requests.get('http://www.gamespot.com/i-am-bread/'))
    all_chunks = list(chunks(new_game_links, 50))
    for index, chunk in enumerate(all_chunks):
        print '##### Processing chunk ', index, 'out of', len(all_chunks)
        items = []
        for response in grequests.map((grequests.get(u) for u in chunk)):
            print '-- returned: %s' %(response.url)
            try:
                items.append(retrive_game_info_by_link(response))
            except Exception, e:
                with open('error_log', 'a') as h:
                    print l + '\t' + str(e) 
                    h.write(l + '\t' + str(e) + '\n')
                continue
        all_items+=items
        insert_mongo('gamespot', items)
        
    client.close()

def retrive_game_info_by_link(response):
    #response = urllib2.urlopen(link)
    soup = BeautifulSoup(response.text)
    r = soup.find("div", {"id": "object-stats-wrap"})
    #date published
    date_published = r.find('dd', {'class':'pod-objectStats-info__release'}).find('span').text   
    release_date = None # release date as datetime 
    try:
        release_date = parse(date_published) 
    except:
        pass
    #system
    systems = [i.text for i in r.find_all('li', {'class':'system'})]
    #title
    name = r.find('a',  {'data-event-tracking':'Tracking|games_overview|GameStats|Title'}).find(text=True).split('\n')[0]
    # game img link
    img_link = soup.find('div', {'class':'gameObject__img'}).find('img').get('src')
    # device platforms
    platforms = [span.text for span in soup.find('ul', {'class':'system-list'}).findAll('span')]
    #gs score
    #score = r.find('div', {'class':'gs-score__cell'}).text.strip()
    score = soup.find('div', {'class':'gs-score__cell'}).text.strip()
    try: 
        score = float(score)
    except ValueError:
        score = None
    # UserReviewScore
    #user_review_score = float(r.find('a',  {'data-event-tracking':'Tracking|games_overview|GameStats|UserReviewScore'}).text)
    user_review_score = soup.find('dl', {'class':'reviewObject__userAvg'}).find('dd').text.strip()
    try: 
        user_review_score = float(user_review_score)
    except ValueError:
        user_review_score = None
    try:
        user_review_count = int(re.findall('(\d+)\ Rating', r.find('dl', {'class':'breakdown-reviewScores__userAvg align-vertical--child'}).find('dd').text)[0])
    except:
        user_review_count = None
    # metacritic score
    meta_score = soup.find('dl', {'class':'reviewObject__metacritic'}).find('dd').text.strip()
    try: 
        meta_score = float(meta_score)
    except ValueError:
        meta_score = None
    # developer
    pattern = re.compile('Developer+')
    developer = [i.text for i in r.find_all('a', {'data-event-tracking':pattern})]
    # publisher
    pattern = re.compile('Publisher+')
    publisher = [i.text for i in r.find_all('a', {'data-event-tracking':pattern})]
    # genre
    pattern = re.compile('Genre+')
    genre = [i.text for i in r.find_all('a', {'data-event-tracking':pattern})]
    # theme
    pattern = re.compile('Theme+')
    theme = [i.text for i in r.find_all('a', {'data-event-tracking':pattern})]
    
    return {
        "name":name,
        'itad_id': '-', # IsThereAnyDeal ID
        'url':response.url,
        'img_url': img_link,
        'platforms': platforms,
        "systems":systems,
        "date_published":date_published,
        'release_date': release_date,
        "score":score,
        'meta_score': meta_score,
        "user_review_score":user_review_score,
        'user_review_count':user_review_count,
        "developer":developer,
        "publisher":publisher,
        "genre":genre,
        "theme":theme
    }
 

if __name__ == '__main__':
    if len(sys.argv) == 2:
        LIST_RETRIES = int(sys.argv[1])
    main()
