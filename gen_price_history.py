import dateutil
import requests
import grequests
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
import execjs # for executing javascript
import re

from utils.mongo_conn import get_mongo_connection
from utils.utils import chunks

client = get_mongo_connection('writer')
db = client.game_data
col_gamespot = db.gamespot
col_price = db.price_hist

def main():
    st = datetime.now()
    print '#####'*5
    print 'Game Price History Populator FIRED at', st
    print '#####'*5 
    # get all games with ITAD id
    # '-' is not processed game
    # 'not_found' is processed but no ID 
    # (no price history or not applicable, like ps4 exclusive)
    data = list(col_gamespot.find({'itad_id':{'$nin':['-', 'not_found']}}))
    print '#### Scraping price for %d games' % len(data)
    
    base_price_url = 'http://isthereanydeal.com/ajax/game/price?plain=%s' 
    
    urls = []
    refs = {}
    for row in data:
        refs[row['itad_id']] = row
        urls.append(base_price_url%row['itad_id'])
    
    all_chunks = list(chunks(urls, 50))
    for index, chunk in enumerate(all_chunks):
        print '##### Processing chunk ', index, 'out of', len(all_chunks)
        for response in grequests.map((grequests.get(u) for u in chunk)):
            meta = refs.get(response.url.split('=')[-1],{})
            game_nm = meta.get('name')
            try:
                df = grab_price_history(response)
                df = df[[col for col in df.columns if col not in ['certainty', 'emphasis']]]
                df.columns = [re.sub('[^A-Za-z0-9]+', '', col).lower() for col in df.columns]
                df['date']= df.date.apply(lambda d:parse(d))
                data = [v for k,v in df.T.to_dict().iteritems()]
                set_q = meta
                set_q['price_history'] = data
                col_price.update({'_id':meta['_id']},
                                 {'$set': set_q}, 
                                 upsert=True)
                print '[SUCCESS] Upserted: %s' % game_nm
            except Exception, e:
                with open('gen_price_error_log', 'a') as h:
                    print '[ERROR]' + game_nm + '\t' + str(e) 
                    h.write(game_nm + '\t' + str(e) + '\n')
                continue
    print '######## END ##########'
    print 'Run time: %s' % str(datetime.now()-st)
    #for id in df_gs.game_id.unique():
    #    grab_price_history(id)
    
    #import ipdb; ipdb.set_trace()
    pass

    client.close()

def grab_price_history(response):
    """Callback to parse out price history data"""
    cols = ''
    rows = ''
    # find js code lines with data
    for line in response.iter_lines():
        line = line.strip()
        if line.startswith('cols'):
            cols = ':'.join(line.split(':')[1:]).strip(',')
        elif line.startswith('rows'):
            rows = ':'.join(line.split(':')[1:]).strip()
    # eval js to to get data        
    rows = execjs.eval(rows)
    cols = execjs.eval(cols)
    # clean up cols and rows
    cols = [col.get('label') if col.get('label') else col.get('p').get('role') for col in cols]
    rows = [[f['v'] for f in row['c']] for row in rows]

    df = pd.DataFrame(rows)
    df.columns = cols

    return df

if __name__ == '__main__':
    main()