"""A Apache Beam example workflow."""
#	python Ambrite.py --input input10.csv --output counts.txt

from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.pvalue import AsSingleton


import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def split_and_lower(line):
    '''Splits input by ',' and converts to lowercase.'''
    user = line.split(',')
    return [x.lower() for x in user]


def no_num_format(line):
    '''Remove all numbers.'''
    ans = ''
    for info in line:
        out = ''.join([i for i in info if not i.isdigit()])
        if out != '':
            ans += out
        ans += ','
    return ans


def format_output(line):
    '''Convert back to csv format.'''
    ans = line.split(',')
    return ans[1] + ',' + ans[2]


def run(argv=None, save_main_session=True):
    '''Main entry point; defines and runs the wordcount pipeline.'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input',
                        default='gs://dataflow-samples/shakespeare/kinglear.txt', help='Input file to process.')
    parser.add_argument('--output', dest='output', required=True,
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    # Read the text file[pattern] into a PCollection.
    lines = p | 'read' >> ReadFromText(known_args.input)

    # Write to .avro file

    class write_to_avro(beam.DoFn):
        def process(self, element):
            user = element.split(',')
            schema = avro.schema.parse(open("user.avsc", "rb").read())
            writer = DataFileWriter(
                open("users.avro", "wb"), DatumWriter(), schema)
            writer.append(
                {"name": str(user[0]), "password": str(user[1])})
            writer.close()

            # Display .avro file
            reader = DataFileReader(open("users.avro", "rb"), DatumReader())
            for user in reader:
                print user
            reader.close()

    processed_users = (lines | 'splits' >> beam.Map(split_and_lower)
                             | 'noNum' >> beam.Map(no_num_format)
                             | 'formatOut' >> beam.Map(format_output))

    processed_users | 'uniqueUser' >> beam.Distinct() | 'writeUnique' >> WriteToText(
        known_args.output, file_name_suffix='.csv')

    #--------------------------------AVRO_EXPERIMENT BEGIN --------------------------------#

    # avro_users = (processed_users | 'write_avro' >>
    #               beam.ParDo(write_to_avro()))
    avro_users = (processed_users | 'write_avro' >>
                  beam.ParDo(write_to_avro()))

    #--------------------------------AVRO_EXPERIMENT END --------------------------------#
    # avro_two = (processed_users | 'avro_out' >> beam.ParDo(write_to_avro()))

    result = p.run()
    result.wait_until_finish()

    # Do not query metrics when creating a template which doesn't run
    if (not hasattr(result, 'has_job')    # direct runner
            or result.has_job):               # not just a template creation
        empty_lines_filter = MetricsFilter().with_name('empty_lines')
        query_result = result.metrics().query(empty_lines_filter)
    if query_result['counters']:
        empty_lines_counter = query_result['counters'][0]
        logging.info('number of empty lines: %d', empty_lines_counter.result)

    word_lengths_filter = MetricsFilter().with_name('word_len_dist')
    query_result = result.metrics().query(word_lengths_filter)
    if query_result['distributions']:
        word_lengths_dist = query_result['distributions'][0]
        logging.info('average word length: %d', word_lengths_dist.result.mean)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
