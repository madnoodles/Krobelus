import pickle
import pandas as pd

def main():
    df = pd.read_csv('./samples/sample_model_dataset.csv')
    df = add_primary_genre(df)
    df.to_csv('genre_cluster_preview.csv')

def add_primary_genre(df):
    """
    Given the model_dataset df (with genre as each column)
    add a column with genre cluster # and further manual processed 
    primary_genre str
    """
    df = df.copy()
    # TODO: save and load the pickled model in mongo 
    with open('./samples/cluster_model', 'rU') as h:
        model = pickle.loads(h.read())
    
    df_genre = df[[c for c in df.columns if 'genre' in c and c!='genre'] 
                  #+ ['release_price']
                  ]
    
    clusters = model.predict(df_genre.values)
    df['clusters'] = clusters
    
    primary_genres = []
    for k,v in df.iterrows():
        primary_genre = ''
        cluster = v['clusters']
        if cluster== 0:
            # this is the action/adventure cluster
            # some RPG lise Dragon Age Inquisition falls into this
            # creating a action_rpg category
            # similar handling for cluster 4 (rpg)
            if v['genre_rpg'] == 1:
                primary_genre = 'action_rpg'
            else:
                primary_genre = 'action/adventure'
        elif cluster==1:
            primary_genre = 'shooter(non_2d)'
        elif cluster == 2:
            # this is a "kitchen sink" cluster
            # so a bit more handling
            if v['genre_drivingracing'] == 1:
                primary_genre = 'racing'
            elif v['genre_sports'] == 1:
                primary_genre = 'sports'
            elif v['genre_puzzle'] == 1:
                primary_genre = 'puzzle'
            elif v['genre_simulation'] == 1:
                primary_genre = 'simulation'
            else:
                primary_genre = 'other'
        elif cluster == 3:
            # clear 2d platformer/shooter cluster
            primary_genre = 'platformer/2d_shooter'
        elif cluster == 4:
            # RPG cluster, we distinguish rpg with action rpg
            if v['genre_action'] == 1:
                primary_genre = 'action_rpg'
            else:
                primary_genre = 'rpg'
        elif cluster == 5:
            # this is a 1st person action/adventure cluster
            # don't care about the perspective
            primary_genre = 'action/adventure'
        elif cluster == 6:
            # action category with a few shooters sifted into
            # mark those to the non 2d shooter cluster
            if v['genre_shooter'] == 1:
                primary_genre = 'shooter(non_2d)'
            else:
                primary_genre = 'action/adventure'
        elif cluster == 7:
            primary_genre = 'strategy'
        primary_genres.append(primary_genre)
        
    df['primary_genre'] = primary_genres
    
    
    # generate a static file for eyeballing..
    """
    for col in [c for c in df.columns if 'genre' in c and c!='genre']:
        df[col] = df[col].apply(lambda d: col.replace('genre_', '') if d==1 else ',')
        
    df ['genres_clean'] = ''
    for col in [c for c in df.columns if 'genre' in c and c!='genre' and c!='genres_clean']:
        df ['genres_clean'] = df[col] + df ['genres_clean']
    df['genres_clean'] = df.genres_clean.apply(lambda d:d.replace(',,', '').strip(','))
    
    print 'generating ./samples/game_clusters_preview.csv for eyeballing....'
    df[['name','primary_genre', 'genre', 'genres_clean', 'release_price', 'clusters']].sort('primary_genre').to_csv('./samples/game_clusters_preview.csv')
    """
    
    return df

if __name__ == '__main__':
    main()
    
    