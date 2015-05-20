"""
Scrape game price history given ITAD game id list
"""

from datetime import datetime
import requests
import pandas as pd
import execjs # for executing javascript

game_ids = ['dragonageinquisition', 'tomclancyssplintercellblacklist']

for id in game_ids:
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

	df.to_csv('%s.csv' %id)
	print '-- generated csv'
