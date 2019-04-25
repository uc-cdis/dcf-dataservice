from __future__ import absolute_import

import argparse
import logging
import timeit

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import json

from scripts.google_replicate import google_copy_wrapper
from scripts.utils import (
    get_ignored_files,
    build_object_dataset_gs,
    prepare_txt_manifest_google_dataflow
)
from scripts.settings import IGNORED_FILES, PROJECT_ACL

try:
    unicode  # pylint: disable=unicode-builtin
except NameError:
    unicode = str

FILE_HEADERS = ["id", "file_name", "size", "md5", "acl", "project_id"]


class PipePrepare(object):
    # class variable
    ignored_dict = get_ignored_files(IGNORED_FILES, "\t")


class FileCopyingDoFn(beam.DoFn):
    def __init__(self, config):
        super(FileCopyingDoFn, self).__init__()
        self.global_config = config

    def process(self, element):
        """Process each line of the manifest file.
        The element is a line of text.
        Args:
          element: the row being processed
        Returns:
          The outcome of the copying process. True/False means success/failure
        """
        text_line = element.strip()
        words = text_line.split()
        fi = dict(zip(FILE_HEADERS, words))
        fi["size"] = int(fi["size"])

        return [(fi, google_copy_wrapper(fi, PipePrepare.ignored_dict, self.global_config))]


def format_result(result):
    (fi, datalog) = result
    return "%s %s %d %s %s %s %s %s %s" % (
        fi.get("id"),
        fi.get("file_name"),
        int(fi.get("size")),
        fi.get("md5"),
        fi.get("acl"),
        fi.get("project_id"),
        datalog.copy_success,
        datalog.index_success,
        datalog.message,
    )


def run(argv=None):
    """Main entry point; defines and runs the copying pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="./scripts/test_data.txt",
        help="Input file to process.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        required=True,
        help="Output file to write results to.",
    )
    # config the upload and download in global_config dictioanry
    # {
    #     "chunk_size_download": 1024,
    #     "chunk_size_upload": 1024
    # }
    parser.add_argument(
        "--global_config", dest="global_config", help="global configuration"
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    global_config = {}
    if known_args.global_config:
        global_config = json.loads(known_args.global_config)
        if not global_config.get("log_bucket") or not global_config.get("release"):
            print("Either log bucket or release params is missing")
            return

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    copied_objects = build_object_dataset_gs(PROJECT_ACL)
    input_path = prepare_txt_manifest_google_dataflow(
        known_args.input,
        "./data_flow_input.txt",
        copied_objects,
        PROJECT_ACL,
        PipePrepare.ignored_dict
    )

    # Read the text file[pattern] into a PCollection.
    lines = p | "read" >> ReadFromText(
        file_pattern=input_path, skip_header_lines=1
    )
    result = lines | "copy" >> beam.ParDo(FileCopyingDoFn(global_config))
    formated_result = result | "format" >> beam.Map(format_result)
    formated_result | "write" >> WriteToText(known_args.output)
    prog = p.run()
    prog.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    start = timeit.default_timer()
    run()
    end = timeit.default_timer()
    print("Total time: {} seconds".format(end - start))
