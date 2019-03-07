import time
from multiprocessing import Pool, Manager
from functools import partial
from google_replicate import bucket_exists, blob_exists, JobInfo

#from cdislogging import get_logger
import logging as logger

from indexclient.client import IndexClient

import utils
from errors import UserError
from settings import PROJECT_ACL, IGNORED_FILES, INDEXD

logger.basicConfig(level=logger.INFO, format='%(asctime)s %(message)s')


def exec_validation(lock, ignored_dict, indexclient, jobinfo):
    """
    Validate gs url and indexd url

    Args:
        lock(SyncManageLock): lock for synchronization
        ignored_dict(dict): dictionary of 5aa objects with key is id and value containing
        gs url hash, size, etc.
        jobinfo(JobInfo): Job info object

    Returns:
        int: Number of files processed
    """

    for fi in jobinfo.files:

        fi["gs_url"], fi["indexd_url"] = None, None
        try:
            bucket_name = utils.get_google_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            logger.error(e)

        try:
            if fi["id"] in ignored_dict:
                object_key = utils.get_structured_object_key(fi["id"], ignored_dict)
                if blob_exists(bucket_name, object_key):
                    fi["gs_url"], fi["indexd_url"] = object_key, indexclient(fi.get("id", ""))
                if not fi["indexd_url"]:
                    logger.warn("{} is not indexed".format(fi["id"]))
                continue

            blob_name = fi.get("id") + "/" + fi.get("file_name")

            if blob_exists(bucket_name, blob_name):
                fi["gs_url"] = "gs://{}/{}".format(bucket_name, blob_name)
                fi["indexd_url"] = indexclient(fi.get("id", ""))
                if not fi["indexd_url"]:
                    logger.warn("{} is not indexed".format(fi["id"]))
            else:
                logger.warn("{} is not copied".format(fi["id"]))
        except Exception as e:
            logger.erro(e)
            time.sleep(120)

    lock.acquire()
    jobinfo.manager_ns.total_processed_files += len(jobinfo.files)
    lock.release()
    if jobinfo.manager_ns.total_processed_files % 100 == 0:
        logger.info(jobinfo.manager_ns.total_processed_files)

    return fi


def _read_data(manifest_file):
    results = []
    copying_files = utils.get_fileinfo_list_from_gs_manifest(manifest_file)
    for fi in copying_files:
        results.append({"id": fi["id"], "file_name": fi["file_name"]})
    return results


def run(thread_num, global_config, job_name, manifest_file, out_manifest):
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

    files = _read_data(manifest_file)

    manager = Manager()
    manager_ns = manager.Namespace()
    manager_ns.total_processed_files = 0
    lock = manager.Lock()

    jobInfos = []
    for fi in files:
        job = JobInfo(global_config, [fi], len(files), job_name, {}, manager_ns, bucket)
        jobInfos.append(job)

    # Make the Pool of workers
    pool = Pool(thread_num)

    part_func = partial(exec_validation, lock, ignored_dict, indexd_client)

    try:
        results = pool.map_async(part_func, jobInfos).get(9999999)
    except KeyboardInterrupt:
        pool.terminate()

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    lookup_dict = {}
    for r in results:
        lookup_dict[r["id"]] = r

    files = utils.get_fileinfo_list_from_gs_manifest(manifest_file)
    for fi in files:
        fi["gs_url", fi["indexd_url"] = None, None
        if fi["id"] in lookup_dict:
            fi["gs_url", fi["indexd_url"] = lookup_dict[fi["id"]]["gs_url"], lookup_dict[fi["id"]]["indexd_url"]

    utils.write_csv("./tmp.csv", files)
    cmd = "gsutil cp ./tmp.csv {}".format(out_manifest)
