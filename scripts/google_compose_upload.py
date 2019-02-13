from socket import error as SocketError
import errno
from multiprocessing import Pool, Manager
from multiprocessing.dummy import Pool as ThreadPool
import time
import timeit
import hashlib
import urllib2
import random
import urllib
import base64
import crcmod
import requests

import threading

import json

from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession

from cdislogging import get_logger
from indexclient.client import IndexClient

from settings import PROJECT_ACL, INDEXD, GDC_TOKEN
import utils
from utils import (
    get_fileinfo_list_from_csv_manifest,
    get_fileinfo_list_from_s3_manifest,
    generate_chunk_data_list,
)
from errors import UserError
from indexd_utils import update_url

logger = get_logger("GoogleReplication")

RETRIES_NUM = 10


def get_object_metadata(sess, bucket_name, blob_name):
    """
    get object metadata
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.quote(blob_name, safe="")
    )
    tries = 0
    while tries < RETRIES_NUM:
        try:
            res = sess.request(method="GET", url=url)
            if res.status_code == 200:
                return res
            else:
                tries += 1
        except Exception:
            tries += 1

    return res


def delete_object(sess, bucket_name, blob_name):
    """
    Delete object from cloud
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.quote(blob_name, safe="")
    )
    sess.request(method="DELETE", url=url)


def resumable_upload_chunk_to_gs(sess, chunk_data, bucket_name, key, part_number):

    object_part_name = key + "-" + str(part_number) if part_number else key

    res = get_object_metadata(sess, bucket_name, object_part_name)
    if res.status_code == 200 and int(res.json().get("size", "0")) == len(chunk_data):
        return res

    url = "https://www.googleapis.com/upload/storage/v1/b/{}/o?uploadType=resumable&name={}".format(
        bucket_name, object_part_name
    )
    headers = {
        "Content-Length": '0',
    }
    tries = 0
    while tries < RETRIES_NUM:
        res = sess.request(method="POST", url=url, headers=headers)
        if res.status_code == 200:
            break
        else:
            tries = tries + 1

    if tries == RETRIES_NUM:
        return res

    tries = 0
    while tries < RETRIES_NUM:
        res2 = sess.request(method="PUT", url=res.headers['Location'], data=chunk_data, headers={"Content-Length": str(len(chunk_data))})
        if res2.status_code not in {200, 201}:
            tries += 1
            continue

        meta_data_res = get_object_metadata(sess, bucket_name, object_part_name)
        chunk_crc32 = crcmod.predefined.Crc('crc-32c')
        chunk_crc32.update(chunk_data)

        if meta_data_res.json().get("crc32c", "") != base64.b64encode(chunk_crc32.digest()):
            tries += 1
        elif int(meta_data_res.json().get("size", "0")) != len(chunk_data):
            logger.warn("upload chunk fail. retries")
            tries += 1
        else:
            return res2

    return res2


def upload_chunk_to_gs(sess, chunk_data, bucket_name, key, part_number):
    """
    upload to create compose object.

    Args:
        sess(session): google client session
        chunk_data: chunk data
        bucket_name(str): bucket name
        key(str): key
        part_number(int): part number

    Return:
        http.Response
    """
    object_part_name = key + "-" + str(part_number) if part_number else key

    res = get_object_metadata(sess, bucket_name, object_part_name)
    if res.status_code == 200 and int(res.json().get("size", "0")) == len(chunk_data):
        return res

    url = "https://www.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}".format(
        bucket_name, object_part_name
    )
    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(chunk_data)),
    }
    tries = 0
    while tries < RETRIES_NUM:
        res = sess.request(method="POST", url=url, data=chunk_data, headers=headers)
        if res.status_code == 200:
            return res
        else:
            tries = tries + 1

    return res


def upload_compose_object_gs(sess, bucket_name, key, object_parts, data_size):
    """
    create compose object from object components
    the object component name is in the format of key-part_number

    Args:
        sess(session): google client session
        bucket_name(str): bucket name
        key(str): key
        object_parts(list(int)): list of object part number
        data_size(int): total data size of all object components

    Return:
        http.Response
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}/compose".format(
        bucket_name, urllib.quote(key, safe="")
    )
    payload = {"destination": {"contentType": "application/octet-stream"}}
    L = []
    for part in object_parts:
        L.append({"name": part})
    payload["sourceObjects"] = L

    headers = {
        "Host": "www.googleapis.com",
        "Content-Type": "application/json",
        "Content-Length": str(data_size),
    }
    retries = 0
    while retries < RETRIES_NUM:
        try:
            res = sess.request(
                method="POST", url=url, data=json.dumps(payload), headers=headers
            )
            if res.status_code in {200, 201}:
                meta_data = get_object_metadata(sess, bucket_name, key)
                if int(meta_data.json().get("size", "0")) != data_size:
                    logger.warn("upload compose fail")
                    retries += 1
                else:
                    return res
            else:
                retries += 1
        except Exception as e:
            logger.warn("Upload fail. Take a sleep and retry. Detail {}".format(e))
            time.sleep(10)
            retries += 1

    return res


def finish_compose_upload_gs(sess, bucket_name, key, chunk_sizes):
    """
    concaternate all object parts

    Args:
        sess(session): google client session
        bucket_name(str): bucket name
        key(str): key
        chuck_sizes(list(int)): list of chunk sizes

    Return:
        http.Response
    """
    def exec_compose_objects(objects, bucket_name, key):
        L = []
        total_size = 0
        for obj in objects:
            L.append(obj["key"])
            total_size += obj['size']

        tries = 0
        while tries < RETRIES_NUM:
            res = upload_compose_object_gs(sess, bucket_name, key, L, total_size)
            if res.status_code not in {200, 201}:
                tries += 1
            else:
                for obj in objects:
                    delete_object(sess, bucket_name, obj["key"])
                return {"key": key, "size": total_size}

        return {}

    def recursive_compose_objects(objects, bucket_name, key):
        if len(objects) < 32:
            return [exec_compose_objects(objects, bucket_name, key)]
        else:
            first = 0
            results = []
            while first < len(objects):
                last = min(len(objects) - 1, first + 31)
                new_key = objects[first]["key"] + "-1"
                results.append(exec_compose_objects(objects[first: last + 1], bucket_name, new_key))
                first = last + 1
            return recursive_compose_objects(results, bucket_name, key)

    objects = []

    for i in range(0, len(chunk_sizes)):
        objects.append({"key": key + "-" + str(i + 1), "size": chunk_sizes[i]})

    res = requests.Response()
    if {} in recursive_compose_objects(objects, bucket_name, key):
        res.status_code = 404
    else:
        res.status_code = 200

    return res


def stream_object_from_gdc_api(fi, target_bucket, global_config, endpoint=None):
    """
    Stream object from gdc api. In order to check the integrity, we need to compute md5 during streaming data from
    gdc api and compute its local crc32c since google only provides crc32c for multi-part uploaded object.

    Args:
        fi(dict): object info
        target_bucket(str): target bucket

    Returns:
        None
    """

    class ThreadControl(object):
        def __init__(self):
            self.mutexLock = threading.Lock()
            self.sig_update_turn = 1

    def _handler(chunk_info):
        tries = 0
        request_success = False

        chunk = None
        while tries < RETRIES_NUM and not request_success:
            try:
                req = urllib2.Request(
                    data_endpoint,
                    headers={
                        "X-Auth-Token": GDC_TOKEN,
                        "Range": "bytes={}-{}".format(
                            chunk_info["start"], chunk_info["end"]
                        ),
                    },
                )

                chunk = urllib2.urlopen(req).read()
                if len(chunk) == chunk_info["end"] - chunk_info["start"] + 1:
                    request_success = True

            except urllib2.HTTPError as e:
                logger.warn(
                    "Fail to open http connection to gdc api. Take a sleep and retry. Detail {}".format(
                        e
                    )
                )
                time.sleep(5)
                tries += 1
            except SocketError as e:
                if e.errno != errno.ECONNRESET:
                    logger.warn(
                        "Connection reset. Take a sleep and retry. Detail {}".format(e)
                    )
                    time.sleep(20)
                    tries += 1
            except Exception as e:
                logger.warn("Take a sleep and retry. Detail {}".format(e))
                time.sleep(10)
                tries += 1

        if tries == RETRIES_NUM:
            raise Exception(
                "Can not open http connection to gdc api {}".format(data_endpoint)
            )

        md5 = hashlib.md5(chunk).digest()

        part_number = chunk_info["part_number"]
        if chunk_info["start"] == 0 and chunk_info["end"] < chunk_data_size - 1:
            part_number = None

        res = resumable_upload_chunk_to_gs(
            sess,
            chunk_data=chunk,
            bucket_name=target_bucket,
            key=object_path,
            part_number=part_number,
        )

        if res.status_code != 200:
            raise Exception(
                "Can not upload chunk data of {} to {}".format(fi["id"], target_bucket)
            )

        # while thead_control.sig_update_turn != chunk_info["part_number"]:
        #     time.sleep(1)

        thead_control.mutexLock.acquire()
        # sig.update(chunk)
        # crc32c.update(chunk)
        thead_control.sig_update_turn += 1
        thead_control.mutexLock.release()

        if thead_control.sig_update_turn % 10 == 0 and not global_config.get("quite"):
            logger.info(
                "Streaming {}. Received {} MB".format(
                    fi["id"], thead_control.sig_update_turn * 1.0 / 1024 / 1024 * chunk_data_size
                )
            )

        return res.json(), chunk_info["part_number"], md5, len(chunk)

    thead_control = ThreadControl()
    thread_client = storage.Client()
    sess = AuthorizedSession(thread_client._credentials)

    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
    data_endpoint = endpoint or "https://api.gdc.cancer.gov/data/{}".format(
        fi.get("id")
    )

    sig = hashlib.md5()
    crc32c = crcmod.predefined.Crc('crc-32c')
    # prepare to compute local etag
    md5_digests = []

    chunk_data_size = global_config.get("data_chunk_size", 1024 * 1024 * 256)

    tasks = []
    for part_number, data_range in enumerate(
        generate_chunk_data_list(fi["size"], chunk_data_size)
    ):
        start, end = data_range
        tasks.append({"start": start, "end": end, "part_number": part_number + 1})

    pool = ThreadPool(global_config.get("multi_part_upload_threads", 10))
    results = pool.map(_handler, tasks)
    pool.close()
    pool.join()

    total_bytes_received = 0

    sorted_results = sorted(results, key=lambda x: x[1])

    chunk_sizes = []
    for res, part_number, md5, chunk_size in sorted_results:
        md5_digests.append(md5)
        total_bytes_received += chunk_size
        chunk_sizes.append(chunk_size)

    if len(sorted_results) > 1:
        finish_compose_upload_gs(
            sess=sess, bucket_name=target_bucket, key=object_path, chunk_sizes=chunk_sizes
        )

    sig_check_pass = validate_uploaded_data(
        fi, sess, target_bucket, sig, crc32c, sorted_results
    )

    if not sig_check_pass:
        delete_object(sess, target_bucket, object_path)
    else:
        logger.info(
            "successfully stream file {} to {}".format(object_path, target_bucket)
        )


def validate_uploaded_data(fi, sess, target_bucket, sig, crc32c, sorted_results):
    """
    validate uploaded data

    Args:
        fi(dict): file info
        sess(session): google client session
        target_bucket(str): aws bucket
        sig(sig): md5 of downloaded data from api
        sorted_results(list(object)): list of result returned by upload function

    Returns:
       bool: pass or not
    """

    md5_digests = []
    total_bytes_received = 0

    for res, part_number, md5, chunk_size in sorted_results:
        md5_digests.append(md5)
        total_bytes_received += chunk_size

    # compute local etag from list of md5s
    #etags = hashlib.md5(b"".join(md5_digests)).hexdigest() + "-" + str(len(md5_digests))
    object_path = "{}/{}".format(fi["id"], fi["file_name"])

    sig_check_pass = True

    if sig_check_pass:
        meta_data = get_object_metadata(sess, target_bucket, object_path)
        if meta_data.status_code != 200:
            return False

        if int(meta_data.json().get("size", "0")) != fi["size"]:
            logger.warn(
                "Can not stream the object {}. {} vs {}. Size does not match".format(fi.get("id"), int(meta_data.json().get("size", "0")), fi["size"])
            )
            sig_check_pass = False

    # if sig_check_pass:
    #     if meta_data.json().get("crc32c", "") != base64.b64encode(crc32c.digest()):
    #         logger.warn(
    #             "Can not stream the object {} to {}. crc32c check fails".format(
    #                 fi.get("id"), target_bucket
    #             )
    #         )
    #         sig_check_pass = False

    # if sig_check_pass:
    #     if sig.hexdigest() != fi.get("md5"):
    #         logger.warn(
    #             "Can not stream the object {}. md5 check fails".format(fi.get("id"))
    #         )
    #         sig_check_pass = False

    return sig_check_pass
