import pandas as pd
import json

df = pd.read_csv('mappings/publishers.csv')

def generate_publisher_mapper(publishers_info):
	publishers = [i[1] for i in publishers_info.iterrows()]

	publisher_mapper = {}	
	for publisher_info in publishers:
		publishers_refined_info = {}
		publisher_name = publisher_info['publisher']
		publisher_type = publisher_info['type']
		if publisher_info['count'] == 1:
			publisher_name = 'small_publisher'
			publisher_type = 'small'

		if pd.isnull(publisher_info['publisher_standard']) == False:
			publisher_name = publisher_info['publisher_standard']
		publishers_refined_info['name'] = publisher_name
		publishers_refined_info['type'] = publisher_type
		publisher_mapper[publisher_info['publisher']] = publishers_refined_info
	return publisher_mapper

publisher_mapper = json.dumps(generate_publisher_mapper(df))

f= open('mappings/publisher_mapper.json','w')
f.write(publisher_mapper)
f.close()
