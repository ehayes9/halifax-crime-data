import requests
import json
import pandas as pd

def extract_values(obj, key):
	"""source: https://hackersandslackers.com/extract-data-from-complex-json-python """
	"""Pull all values of specified key from nested JSON."""
	arr = []

	def extract(obj, arr, key):
		"""Recursively search for values of key in JSON tree."""
		if isinstance(obj, dict):
			for k, v in obj.items():
				if isinstance(v, (dict, list)):
					extract(v, arr, key)
				elif k == key:
					arr.append(v)
		elif isinstance(obj, list):
			for item in obj:
				extract(item, arr, key)
		return arr

	results = extract(obj, arr, key)
	return results


if __name__ == "__main__":
	# get crime data from Halifax open data
	response = requests.get("https://opendata.arcgis.com/datasets/f6921c5b12e64d17b5cd173cafb23677_0.geojson")

	data = response.json()

	# convert data to a string
	data_string = json.dumps(data)
	# load json object as python dictionary 
	data_dict = json.loads(data_string)

	# create list of fields we want to extract
	column_names = ['OBJECTID','evt_rt','evt_rin','evt_date','location','zone','rucr','rucr_ext_d']

	#todo: write a loop for this process, add X & Y coordinates
	#pull data from the json object for each nested key using extract_values function

	# X = extract_values(data_dict,'X')	
	# Y = extract_values(data_dict,'Y')	
	OBJECTID = extract_values(data_dict,'OBJECTID')
	evt_rt = extract_values(data_dict,'evt_rt')	
	evt_rin = extract_values(data_dict,'evt_rin')	
	evt_date = extract_values(data_dict,'evt_date')	
	location = extract_values(data_dict,'location')	
	zone = extract_values(data_dict,'zone')	
	rucr= extract_values(data_dict,'rucr')	
	rucr_ext_d = extract_values(data_dict,'rucr_ext_d')	

	# create df by zipping lists together
	df_new = pd.DataFrame(list(zip(OBJECTID,evt_rt,evt_rin,evt_date,location,zone,rucr,rucr_ext_d)),columns=column_names)

	# will have to run this the first time you run the script to save the initial dataset
	# df_new.to_csv('data/crime_output.csv')

	# import existing dataframe 
	df_existing = pd.read_csv('data/crime_output.csv')
	
	# find max date of existing df
	max_date = df_existing['evt_date'].max()

	# query new DF to find records that aren't already in existing df
	new_records = df_new.query('evt_date > @max_date')

	# append new records to existing DF
	df_updated = df_existing.append(new_records,sort=True)







