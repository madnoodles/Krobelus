import dateutil
import requests
import pandas as pd
from datetime import datetime
import execjs # for executing javascript

def main():

    h = open('samples/game_ids_full.txt', 'rU')
    # create a map of {game_spot_title: itad_id}
    name_id_map = {}
    for line in h:
        l = line.split(',')
        id = l[0].strip()
        title = ','.join(l[1:]).strip()
        # use the first id
        name_id_map[title] = name_id_map.get(title, id)
    
    # gamespot metadata df clean up
    df_gs = pd.read_csv('samples/games.csv')
    def clean_date(dd):
        try:
            dd = dateutil.parser.parse(dd)
        except ValueError:
            print dd
            dd = datetime(2001,1,1)
        return dd
    df_gs['date_published'] = df_gs['date_published'].apply(clean_date)
    # apply game id mapping
    df_gs['game_id'] = df_gs.name.apply(lambda d:name_id_map.get(d, '-'))
    
    # sort columns and generated a colum
    start_header = [ 'name', 'game_id', 'date_published']
    df_gs = df_gs[start_header + [c for c in df_gs.columns if c not in start_header]]
    df_gs.to_csv('games_with_id.csv')
    
    # prepare to run price scraping script
    # filter for only games with id and publish date > 2012-01-01
    df_gs = df_gs[df_gs['game_id']!='-']
    df_gs = df_gs[df_gs['date_published']>datetime(2012, 1, 1)]
    
    for id in df_gs.game_id.unique():
        grab_price_history(id)
    
    #import ipdb; ipdb.set_trace()
    pass

def grab_price_history(id):
    print 'Processing: ', id 
    url = 'http://isthereanydeal.com/ajax/game/price?plain=%s' % id

    response = requests.get(url)
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

    df.to_csv('price_data/%s.csv' %id)    
    print ' ---', id, 'completed'

if __name__ == '__main__':
    main()