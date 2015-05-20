"""
Scraping game metadata from gamespot
"""

import urllib2
from bs4 import BeautifulSoup
import difflib
import re
url_base = "http://www.gamespot.com/new-games/?sort=score&game_filter_type%5Bplatform%5D=19&game_filter_type%5Bgenres%5D=&game_filter_type%5BminRating%5D=&game_filter_type%5BtimeFrame%5D=&game_filter_type%5BstartDate%5D=&game_filter_type%5BendDate%5D=&game_filter_type%5Btheme%5D=&game_filter_type%5Bregion%5D=&game_filter_type%5Bletter%5D=&page="
page = 1
starting_url = url_base + str(page) 
response = urllib2.urlopen(starting_url)
soup = BeautifulSoup(response)
domain = "http://www.gamespot.com"
all_page_num = [int((domain + link.get('href')).replace(url_base,'')) for link in soup.find_all('a',{'class':'btn'})]
page_num_max = max(all_page_num)
 
all_pages = [url_base+str(i) for i in range(1,page_num_max+1)]
 
 
all_game_links = []
 
for page in all_pages:
	soup = BeautifulSoup(urllib2.urlopen(page))
	game_links = [(domain + link.get('href')) for link in soup.find_all('a',{'class':'js-event-tracking'})]
	all_game_links += game_links
 
 
def retrive_game_info_by_link(link):
	response = urllib2.urlopen(link)
	soup = BeautifulSoup(response)
	r = soup.find("div", {"id": "object-stats-wrap"})
 
	#date published
	date_published = r.find('dd', {'class':'pod-objectStats-info__release'}).find('span').text	
	#system
	systems = [i.text for i in r.find_all('li', {'class':'system'})]
	#title
	name = r.find('a',  {'data-event-tracking':'Tracking|games_overview|GameStats|Title'}).find(text=True).split('\n')[0]
	#gs score
	score = float(re.sub("\D", "", r.find('div', {'class':'gs-score__cell'}).text))
	# UserReviewScore
	user_review_score = float(r.find('a',  {'data-event-tracking':'Tracking|games_overview|GameStats|UserReviewScore'}).text)
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
		"systems":systems,
		"date_published":date_published,
		"score":score,
		"user_review_score":user_review_score,
		"developer":developer,
		"publisher":publisher,
		"genre":genre,
		"theme":theme
	}
 
items = []
for i in all_game_links:
	try:
		items.append(retrive_game_info_by_link(i))
	except:
		continue
