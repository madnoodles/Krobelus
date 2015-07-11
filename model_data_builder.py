from datetime import datetime, timedelta
from dateutil.parser import parse
from numpy import nan
import pandas as pd
import math
import re
from optparse import OptionParser

from utils.mongo_conn import get_mongo_connection
from model_genre import add_primary_genre

# only include games release at lease N days ago
RECENT_RELEASE_MIN_DAYS = 60
# ignore games without sufficient starting data
MAX_MISS_STARTING_DAYS = 40

# exclude foreign vendors
exclude_vendors = ('nuuvem', 'gamesplanetuk', 
                   'gamesplanetfr', 'gamesplanetde', '')

# binary discount indicator to generate for model output 
# def = [tier_name, start_week, end_week, discount_pct_lower_bound, discount_pct_upper_bound]
# examples below:
sales_tiers = [
    ['dsct_1st_month_20_pct_plus' ,0, 4, 0.2, 1],
    ['dsct_2nd_month_20_pct_plus' ,5, 8, 0.2, 1],
    ['dsct_3rd_month_20_pct_plus' ,9, 12, 0.2, 1],
    ['dsct_4th_month_20_pct_plus' ,13, 16, 0.2, 1],
]

client = get_mongo_connection('writer')
db = client.game_data
col_price = db.price_hist
col_data = db.model_data

df = pd.read_csv('./mappings/genres.csv')
genre_map = df.set_index('genre').genre_standard.T.to_dict()

def main():
    
    parser = OptionParser()
    parser.add_option("-m", "--mode", dest="mode", default='dataset',
                      help="mode: dataset|price. 'dataset' mode generates prod model data(binary vars)." \
                      + " 'price' mode generate by week price listing for debugging.",)
    parser.add_option("-s", "--sample_num",
                      dest="sample_num", default=999999999,
                      help="whether to sample first n rows")
    parser.add_option("-v", "--verbose", action="store_true",
                      dest="verbose", default=False,
                      help="verbosity")
    
    (options, args) = parser.parse_args()
    sample_num = int(options.sample_num)
    verbose = options.verbose
    mode = options.mode
    
    print '#####'*10
    print 'Model Data Builer FIRED! ', datetime.now()
    if mode == 'price':
        print '-- generating price history listing (for qa/debugging), export csv'
    elif mode == 'dataset':
        print '-- generating model dataset (price hist converted to binary var), insert to mongo'
    print '#####'*10
    
    st = datetime.now()
    top_publishers = list(col_price.aggregate([{'$unwind':'$publisher'}, 
                         {'$group':{'_id':'$publisher', 'freq':{'$sum':1}}},
                         {'$sort': {'freq': -1}}
                         ]))
    print 'publishers:'
    print top_publishers[:5]
    print '...'
    top_genres = list(col_price.aggregate([{'$unwind':'$genre'}, 
                         {'$group':{'_id':'$genre', 'freq':{'$sum':1}}},
                         {'$sort': {'freq': -1}}
                         ]))
    print 'genres:'
    print top_genres[:5]
    # apply standard genre name mapping
    # get distinct list and print out not mapped 
    distinct_genres = []
    for row in top_genres:
        g = row['_id']
        g_standard = genre_map.get(g)
        if not g_standard:
            print '-- Undefined genre: %s' % g
            g_standard = 'Other'
        if g_standard not in distinct_genres:
            distinct_genres.append(g_standard)
    print '...'
    print '----'*10
    print 'Fetching and clean up game price history and metadata..'
    """
    vendors = {}
    for v in data:
        for col in v['price_history'][0].keys():
            if col != 'date':
                vendors[col] = vendors.get(col, 0)+1
    vendor_filt = [v for v in vendors if v not in exclude_vendors]
    """
    print 'Excluding following vendors:'
    print exclude_vendors
    print '----'*10
    #rows = []
    pieces = []
    count = 0
    # only include record released for at least 2 months
    date_threshold = datetime.now()-timedelta(days=RECENT_RELEASE_MIN_DAYS)
    for row in col_price.find({'release_date':{'$lte': date_threshold}}):
        #rows.append(row)
        # iterate through mongo records and parse data
        result = clean_up_sales_data(row, distinct_genres,
                                     mode, verbose)
        pieces.append(result)
        count += 1
        if count == sample_num:
            break
    pieces = [p for p in pieces if p]
    if mode == 'dataset':
        # generating model dataset (price history converted to binary vars)
        df = pd.DataFrame(pieces)
        
        genre_cols = [c for c in df.columns if 'genre' in c]
        discount_cols = [c for c in df.columns if 'dsct' in c and c != 'dsct_propensity']
        all_metadata =  [c for c in df.columns if c not in genre_cols and c not in discount_cols]
        dataset = df[all_metadata + discount_cols + genre_cols]
        dataset = add_primary_genre(dataset)
        print '----'*10
        print '-- generating ./samples/sample_model_dataset.csv'
        dataset.to_csv('./samples/sample_model_dataset.csv')
    elif mode == 'price':
        # generating price history sheet
        # good for QA/debugging
        print '----'*10
        print '-- generating ./samples/sample_prices.csv'
        pd.concat(pieces).to_csv('./samples/sample_prices.csv')
    
    print '---'*10
    print 'Run time:', str(datetime.now()-st)
    
    return


def clean_up_sales_data(game_data, genres, 
                        mode='dataset', verbose=False):
    """
    Parse game data
    
    |price| mode cleans price data (merge vendors, group by week, etc)
    |dataset| mode further converts price data to binary vars
    """
    name = game_data['name']
    release_date = game_data['release_date']
    publisher_str = ','.join(game_data['publisher'])
    try:
        score = float(game_data['score'])
    except TypeError:
        score = nan
    try:
        meta_score = float(game_data['meta_score'])
    except TypeError:
        meta_score = nan
    try:
        user_review_score = float(game_data['user_review_score'])
    except TypeError:
        user_review_score = nan
    #developer = game_data['developer']
    genre_str = ','.join(game_data['genre'])
    platform_str = ','.join(game_data['platforms'])
    #theme = game_data['theme']
    
    obj = {
        'name': name,
        'itad_id': game_data['itad_id'],
        'release_date':release_date,
        'release_price': 0,
        'score':score,
        'meta_score':meta_score,
        'user_review_score':user_review_score,
        'publisher':game_data['publisher'],
        'platform':game_data['platforms'],
        #'developer':game_data['developer'],
        'dsct_propensity': 0,
        'genre':game_data['genre'],
    }
    
    df = pd.DataFrame(game_data['price_history'])
    if verbose:
        print 'Processing %s, %s, %d records' % (game_data['name'], 
                                                 str(release_date.strftime('%Y-%m-%d')),
                                                 len(df))
    # clean columns, exclude foreign vendors
    df = df[[c for c in df.columns if c not in exclude_vendors]]
    # Assume max price on release day is original price
    df['date'] = df.date.apply(lambda d: datetime(d.year, d.month, d.day))
    original_price = 0
    release_day_record = df[df.date==release_date]
    if len(release_day_record)>0:
        original_price = release_day_record[[col for col in release_day_record.columns 
                                             if col != 'date']].T.max().values[0]
    else:
        # if somehow price on pub date is not available..
        # assume release price is the first non-zero max price among all vendors 
        for k,row in df.set_index('date').iterrows():
            cur_max_price = max(row.values)
            if cur_max_price>0:
                if (k-release_date).days>MAX_MISS_STARTING_DAYS:
                    print '-- [IGNORED] insufficient starting data for: %s ' % name
                    return
                original_price = cur_max_price
                break
    obj['release_price'] = original_price
    
    # stack vendors
    df = df.set_index('date').stack().reset_index()
    df.columns = ['date', 'vendor', 'price']
    # create a # weeks from release date field
    df['weeks_from_release']=df.date.apply(lambda d: (d-release_date).days/7)
    # aggregation: for each week, return lowest price, across all vendors, all days
    agg = df.groupby('weeks_from_release').apply(lambda d:d.price.min())
    agg = agg.reset_index()
    agg.columns = ['weeks_from_release', 'price']
    # add sales pct (maybe negative value is original_price is wrong)
    agg['pct_off'] = agg.price.apply(lambda d: (original_price-d)/original_price)
    
    if mode == 'dataset':
        for g in genres:
            has_genre = 0
            if g in [genre_map.get(gr, 'Other') for gr in game_data['genre']]:
                has_genre = 1
            obj['genre_'+re.sub('[^A-Za-z0-9]', '', g.lower())] = has_genre
        
        # create a field for each sales tier
        for k,row in agg.iterrows():
            for sales_tier in sales_tiers:
                tier_name, start_week, end_week, lower, upper = sales_tier
                is_true = obj.get(tier_name, 0)
                # if condition already satisfied (e.g. at first week)
                if is_true: 
                    continue 
                if (row['weeks_from_release']>=start_week and
                    row['weeks_from_release']<=end_week and
                    row['pct_off']>=lower and
                    row['pct_off']<=upper):
                    obj[tier_name] = 1
                else:
                    obj[tier_name] = 0
            #experimental discount score
            if row['weeks_from_release']>0:
                obj['dsct_propensity'] += math.fabs(row['pct_off']*1.0/row['weeks_from_release'])
        return obj
    elif mode == 'price':
        # metadata
        agg['game'] = game_data['name']
        agg['release_date'] = release_date
        agg['itad_id'] = game_data['itad_id']
        agg['platform'] = platform_str
        agg['release_price'] = original_price
        agg['pub_date'] = release_date
        agg['publisher'] = publisher_str
        agg['score'] = score
        agg['meta_score'] = meta_score
        agg['user_review_score'] = user_review_score
        #agg['developer'] = developer
        agg['genre'] = genre_str
        #agg['theme'] = theme
        
        leading_fields = ['game', 'weeks_from_release', 'price', 'pct_off']
        agg = agg[leading_fields + [c for c in agg.columns if c not in leading_fields]]
        
        return agg


if __name__ == '__main__':
    main()
    
