from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google_replicate import exec_google_copy

try:
  unicode           # pylint: disable=unicode-builtin
except NameError:
  unicode = str

FILE_HEADERS = ['fileid', 'filename', 'size', 'hash', 'acl', 'project']
global_config ={"token_path": "./gdc-token.txt", "chunk_size_download": 2048000, "chunk_size_upload": 20971520}

class FileCopyingDoFn(beam.DoFn):
  """Process each line of input text into words."""

  def __init__(self):
    super(FileCopyingDoFn, self).__init__()
    self.words_counter = Metrics.counter(self.__class__, 'words')
    self.word_lengths_counter = Metrics.counter(self.__class__, 'word_lengths')
    self.word_lengths_dist = Metrics.distribution(
        self.__class__, 'word_len_dist')
    self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')

  def process(self, element):
    """Returns an iterator over the words of this element.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the element being processed
    Returns:
      The processed element.
    """
    text_line = element.strip()
    if not text_line:
      self.empty_line_counter.inc(1)
    words = text_line.split()
    for w in words:
      self.words_counter.inc()
      self.word_lengths_counter.inc(len(w))
      self.word_lengths_dist.update(len(w))
    fi = dict(zip(FILE_HEADERS, words))

    return [(fi.get('fileid',''), exec_google_copy(fi, global_config))]

def format_result(result):
    (uuid, success) = result
    return '%s: %d' % (uuid, success)

def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='./manifest',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> ReadFromText(known_args.input)
  result = (lines
            | 'copy' >> beam.ParDo(FileCopyingDoFn()))
  formated_result = result | 'format' >> beam.Map(format_result)
  formated_result | 'write' >> WriteToText(known_args.output)
  prog = p.run()
  prog.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
