import time
import subprocess
from multiprocessing import Pool, Manager
from functools import partial

#from cdislogging import get_logger
import logging as logger

from indexclient.client import IndexClient

import utils
from errors import UserError
from scripts.aws_replicate import build_object_dataset
from settings import PROJECT_ACL, IGNORED_FILES, INDEXD

logger.basicConfig(level=logger.INFO, format='%(asctime)s %(message)s')


def run(manifest_file, out_manifest):
    """
    start threads and log after they finish
    Args:
        thread_num(int): Number of threads/cores
        global_config(dict): a configuration
        job_name(str): job name
        manifest_file(str): the name of the manifest
    
    Returns:
        None

    """
    ignored_dict = utils.get_ignored_files(IGNORED_FILES)

    indexd_client = IndexClient(
        INDEXD["host"],
        INDEXD["version"],
        (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
    )

    if not ignored_dict:
        raise UserError(
            "Expecting non-empty IGNORED_FILES. Please check if ignored_files_manifest.csv is configured correctly!!!"
        )

    files = utils.get_fileinfo_list_from_gs_manifest(manifest_file)

       
    logger.info("scan all copied objects")
        copied_objects, _ = build_object_dataset(PROJECT_ACL, None)
    

    utils.write_csv("./tmp.csv", files)
    cmd = "gsutil cp ./tmp.csv {}".format(out_manifest)
    subprocess.Popen(cmd, shell=True).wait()
