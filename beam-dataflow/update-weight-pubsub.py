
from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

from settings import PROJECT

def run(run_local):
 	options = {
		'project': PROJECT,
		'input_topic': 'projects/extended-signal-219515/topics/search-weights',
		'input_subscription': 'projects/extended-signal-219515/subscriptions/search-weights-subscription',
		'runner': 'DataflowRunner',
		'save_main_session': True
	}
	if run_local:
		options['runner'] = 'DirectRunner'

	pipeline_options = PipelineOptions.from_dictionary(options)
	pipeline_options.view_as(SetupOptions).save_main_session = True
	pipeline_options.view_as(StandardOptions).streaming = True
	p = beam.Pipeline(options=pipeline_options)
	messages = (p
                | beam.io.ReadFromPubSub(
                    subscription='projects/extended-signal-219515/subscriptions/search-weights-subscription')
                .with_output_types(bytes))
	lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
	result = p.run()
	result.wait_until_finish()

if __name__ == '__main__':
  	logging.getLogger().setLevel(logging.INFO)
	run_locally = True
	run(run_locally)
