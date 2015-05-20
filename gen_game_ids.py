"""
- Going through gamespot game list 
- Search ITAD for top results
- parse out game id, filter out dlc/season pass, etc
- save (gamespot_game_title, itad_game_id) pairs
"""

import requests
from bs4 import BeautifulSoup as soup
import re
import pandas as pd
from datetime import datetime
import dateutil
#game = 'splinter cell blacklist'
#url = 'http://isthereanydeal.com/#/search:%s' % game

# if game id contains following words, ignore them
stop_words = ['dlc', 'pack', 'edition', 'season', 'preorder', 'deluxe',
                'pass', 'gameofyear', 'goty', 'bundle', 'soundtrack'] 
         
def main():

    df = pd.read_csv('games.csv')
    # clean up game published date
    def clean_date(dd):
        try:
            dd = dateutil.parser.parse(dd)
        except ValueError:
            print dd
            dd = datetime(2001,1,1)
        return dd
    df['date_published'] = df['date_published'].apply(clean_date)
    # filter for a shortlist of games published after 2012
    df = df[df['date_published']>datetime(2012, 1, 1)]
    titles = df.name.unique()
    print len(titles), 'game titles'
    
    results = []
    # TODO: parallelise
    for game in titles:
        print 'Processing:', game
        
        try:
            # need to scrap 2 ajax, 1 for list of games currently on sale, 1 for not currently on sales
            url_deal = 'http://isthereanydeal.com/ajax/data/lazy.deals.php?by=time%3Adesc&offset=0&limit=75&filter=%2Fsearch%3A' \
                + game +'&file=5.1432014545.8a99bdb28c1daf2474cf8790f3a756fb&lastSeen=0&region=us'
            url_no_deal = 'http://isthereanydeal.com/ajax/nondeal.php?by=time%3Adesc&offset=12&limit=75&filter=%2Fsearch%3A' \
                + game + '&file=5.1432014545.8a99bdb28c1daf2474cf8790f3a756fb&lastSeen=0&region=us'
            
            ids = set(find_game_ids(url_deal) + find_game_ids(url_no_deal))
            # Pick the shortest ids that are more likely to be the game_id
            # the heuristics is that dlc, season pass and other junks tend to have longer name
            ids = sorted(list(ids), key=lambda d:len(d))[:3] 
            for id in ids:
                print id, game
                results.append([id, game])
                with open('game_ids_full.txt', 'a') as h:
                    h.write('%s, %s\n'%(id, game))
        except Exception, e:
            print '%s Error: %s' %(game, str(e))
            with open('error_log.txt', 'a') as h:
                    h.write('%s Error: %s\n' %(game, str(e)))
    

        
def find_game_ids(url):
    res = requests.get(url)
    html = soup(res.text)
    ids = []
    for link in html.findAll('a'):
        id = re.findall(r'info\?plain\=(\S+)', link['href'])
        if len(id)>0: 
            id = id[0]
            include = True
            for w in stop_words:
                if w in id:
                    include = False
            if id not in ids and include:
                ids.append(id)
    return ids


if __name__ == '__main__':
    main()
"""
search_results = html.find('div', {'id':'games'})

children = search_results.findChildren()
games = []
for child in children:
    try:
        classes = child.get('class', [])
        if 'game' in child['class']:
            games.append(child) 
    except KeyError:
        continue

import ipdb; ipdb.set_trace()
"""