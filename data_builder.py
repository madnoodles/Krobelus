import csv
import json
from collections import defaultdict
game_ids_file = 'samples/game_ids_full.txt'
f = open(game_ids_file).read()
f = f.split('\n')
game_spot_name_to_ids = defaultdict(list)

for i in f[:-1]:
    i = i.split(', ')
    game_spot_name_to_ids[i[1]].append(i[0])


from os import listdir
price_file_path = 'samples/project_data/'
file_names = listdir(price_file_path)


def csv_to_dic(file):
    list = []
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            list.append(row)
    return list

game_spot_name_price_hist = {}
for game_spot_name in game_spot_name_to_ids.keys():
    for id in game_spot_name_to_ids[game_spot_name]:
        f_name = id+'.csv'
        if f_name in file_names:
            game_spot_name_price_hist[game_spot_name] = csv_to_dic(price_file_path + f_name)


game_spot_info_file = 'samples/games.csv'
info = csv_to_dic(game_spot_info_file)

game_spot_name_full_data = {}
for i in info:
    if i['name'] in game_spot_name_price_hist.keys():
        i['price_history'] = game_spot_name_price_hist[i['name']]
        game_spot_name_full_data[i['name']] = i
        
        
with open('final_data', 'w') as outfile:
    json.dump(game_spot_name_full_data, outfile)

