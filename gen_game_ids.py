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
import sys
import multiprocessing
import grequests

from utils.mongo_conn import get_mongo_connection

# if game id contains following words, ignore them
stop_words = ['dlc', 'pack', 'edition', 'season', 'preorder', 'deluxe',
              'pass', 'gameofyear', 'goty', 'bundle', 'soundtrack'] 

client = get_mongo_connection('writer')
db = client.game_data
col = db.gamespot

def main():

    #df = pd.read_csv('games.csv')
    data = list(col.find({'itad_id':'-'}))
    
    pool_num = 8
    if len(sys.argv)>=2:
        pool_num = int(sys.argv[1])
    
    print "#########\nProcessing %d games\n#########"%len(data)
    print "-- using %d cores" % pool_num
    q = multiprocessing.Queue()
    pool = multiprocessing.Pool(pool_num, process_init, [q])
    results = pool.imap(fetch_itad_id, data)
    
    pool.close()
    pool.join()
    
    print '###### END ######'

def fetch_itad_id(game_obj):
    game = game_obj['name']
    #print 'Processing:', game
    try:
        # need to scrap 2 ajax, 1 for list of games currently on sale, 1 for not currently on sales
        url_deal = 'http://isthereanydeal.com/ajax/data/lazy.deals.php?by=time%3Adesc&offset=0&limit=75&filter=%2Fsearch%3A' \
            + game +'&file=5.1432014545.8a99bdb28c1daf2474cf8790f3a756fb&lastSeen=0&region=us'
        url_no_deal = 'http://isthereanydeal.com/ajax/nondeal.php?by=time%3Adesc&offset=12&limit=75&filter=%2Fsearch%3A' \
            + game + '&file=5.1432014545.8a99bdb28c1daf2474cf8790f3a756fb&lastSeen=0&region=us'
        
        urls = [url_deal, url_no_deal]
        ids = []
        for response in grequests.map((grequests.get(u) for u in urls)):
            ids += find_game_ids(response)
        #ids = set(find_game_ids(url_deal) + find_game_ids(url_no_deal))
        ids = set(ids)
        # Pick the shortest ids that are more likely to be the game_id
        # the heuristics is that dlc, season pass and other junks tend to have longer name
        ids = sorted(list(ids), key=lambda d:len(d))[:3] 
        if len(ids)>0:
            set_q = {'itad_id': ids[0],
                     'itad_id_candidates': ids
                     }
            col.update({'_id': game_obj['_id']}, {'$set':set_q})
            print '[Sucess]\t %s -- id added' % game
            return [id, game]
        else:
            print '[Warning]\t %s -- NO id found' % game
            set_q = {'itad_id': 'not_found',
                     'itad_id_candidates': []
                     }
            col.update({'_id': game_obj['_id']}, {'$set':set_q})
            #with open('gen_id_error_log.txt', 'a') as h:
            #    h.write('[Warning]\t %s -- NO id found' % game)
        
    except Exception, e:
        print '[Error]\t %s -- ERROR: %s' %(game, str(e))
        #with open('gen_id_error_log.txt', 'a') as h:
        #        h.write('%s -- Error: %s\n' %(game, str(e)))

def process_init(q):
    """Initializer for multiprocessing"""
    fetch_itad_id.q = q

def find_game_ids(res):
    #res = requests.get(url)
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


def insert_mongo(col_name, data):
    if len(data)>0:
        print '-- inserting %d rows to %s'% (len(data), col_name)
        db[col_name].insert(data)
    else:
        print '-- nothing to insert'

if __name__ == '__main__':
    main()
