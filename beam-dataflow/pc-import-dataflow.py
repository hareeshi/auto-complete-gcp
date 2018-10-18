import datetime
import json
import re
import random

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from google.cloud.proto.datastore.v1 import entity_pb2
from googledatastore import helper as datastore_helper

from settings import PROJECT, BUCKET, INPUT_FILENAME

regex_replace = re.compile('[\W_]+')


class JSONtoDict(beam.DoFn):
	"""Converts line into dictionary"""
	def process(self, element):
		try:
			element = json.loads(element)
			product_name = element['name']
			sku = element['sku']
			name_lst = regex_replace.sub(' ', product_name.lower()).split(' ')
			name_lst = [x for x in name_lst if x and len(x) > 2]
			data = []
			for name_items in name_lst:
				sku = sku + random.randint(1,sku)
				data.append ({"idx": name_items, "name": product_name, "skupxy": sku})
			return data
		except Exception:
			pass

class CreateEntities(beam.DoFn):
	"""Creates Datastore entity"""
	def process(self, element):
		entity = entity_pb2.Entity()
		datastore_helper.add_key_path(entity.key, 'ProductsIndex',element['skupxy'])
		datastore_helper.add_properties(entity, element)
		return [entity]

def dataflow(run_local):
	if run_local:
		input_file_path = './products.json'
	else:
		input_file_path = 'gs://' + BUCKET + '/' + INPUT_FILENAME

	JOB_NAME = 'datastore-upload-{}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))

	pipeline_options = {
		'project': PROJECT,
		'staging_location': 'gs://' + BUCKET + '/staging',
		'runner': 'DataflowRunner',
		'job_name': JOB_NAME,
		'disk_size_gb': 100,
		'temp_location': 'gs://' + BUCKET + '/temp',
		'save_main_session': True
	}

	if run_local:
		pipeline_options['runner'] = 'DirectRunner'

	options = PipelineOptions.from_dictionary(pipeline_options)
	with beam.Pipeline(options=options) as p:

		(p | 'Reading input file' >> beam.io.textio.ReadFromText(input_file_path,strip_trailing_newlines=True)
		 | 'Converting from json to dict' >> beam.ParDo(JSONtoDict())
		 | 'Create entities' >> beam.ParDo(CreateEntities())
		 | 'Write entities into Datastore' >> WriteToDatastore(PROJECT)
		 )

if __name__ == '__main__':
	run_locally = False
	dataflow(run_locally)
