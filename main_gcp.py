import requests
import json
import pandas as pd
from google.cloud import bigquery

client = bigquery.Client()

# TODO: update variables to match your project, update project, dataset & table name in query below 

TABLE_NAME = ""
DATASET_NAME = ""

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


def extract_hfx_crime_data(request):

	"""HTTP Cloud Function.
	Extracts Data from the HFX OpenData portal, and imports into BigQuery table
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.

		Response = 'SUCCESS' implies that the load was successful 
    """

	# get crime data from Halifax open data
	response = requests.get("https://opendata.arcgis.com/datasets/f6921c5b12e64d17b5cd173cafb23677_0.geojson")

	data = response.json()
	# convert data to a string
	data_string = json.dumps(data)
	# load json object as python dictionary 
	data_dict = json.loads(data_string)

	# create list of fields we want to extract
	column_names = ['OBJECTID','evt_rt','evt_rin','evt_date','location','zone','rucr','rucr_ext_d']

	#TODO: write a loop for this process, add X & Y coordinates
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
	df = pd.DataFrame(list(zip(OBJECTID,evt_rt,evt_rin,evt_date,location,zone,rucr,rucr_ext_d)),columns=column_names)

	""" perform cleaning functions """
	df.columns = df.columns.str.lower()
	df = df.apply(lambda x: x.astype(str).str.lower())

	# convert evt_date column to timestamp
	df['evt_date'] = pd.to_datetime(df['evt_date'])

	# rename columns to make them move intuitive
	df.rename(columns={'rucr_ext_d':'description',
		'evt_date':'date'},
		inplace=True)

	""" TODO: update project, dataset & table name in query
	find max objectID in existing BQ table to determine new records to append """

	query = """
    SELECT max(object_id) as object_id
    FROM `project_name.dataset_name.table_name`
    """

	query_job = client.query(query).result().to_dataframe()
	max_objectid = query_job['objectid'][0]

	"""query new DF to find records that aren't already in existing df """
	new_records = df.query('objectid > @max_objectid')	

	table_id = "{}.{}".format(DATASET_NAME, TABLE_NAME)
	method = 'append'

	job = client.load_table_from_dataframe(
		new_records, table_id, method
	)

	# Wait for async job to finish
	job.result()

	# TODO: Add Error handling here. The return message can be used to trigger other functions, 
	# for example - on FAILURE, send Slack notification

	return 'SUCCESS'


if __name__ == '__main__':

	## Use this to run locally (not necessary for cloud function)
	extract_hfx_crime_data(None)




