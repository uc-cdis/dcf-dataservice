from socket import error as SocketError
import errno
import time
import hashlib
import urllib2
import urllib
import base64
import crcmod
import requests
from Queue import Queue

import threading
from threading import Thread

import json

from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession

from cdislogging import get_logger

from scripts.settings import GDC_TOKEN

from scripts.utils import (
    generate_chunk_data_list,
)


logger = get_logger("GoogleReplication")

RETRIES_NUM = 10


class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception, e:
                logger.warn(e)
            finally:
                self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()


def get_object_metadata(sess, bucket_name, blob_name):
    """
    get object metadata
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.parse.quote(blob_name, safe="")
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
        bucket_name, urllib.parse.quote(blob_name, safe="")
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
        bucket_name, urllib.parse.quote(key, safe="")
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
        global_config(dict): configuration dictionary

    Returns:
        None
    """

    class ThreadControl(object):
        def __init__(self):
            self.mutexLock = threading.Lock()
            self.sig_update_turn = 1
            self.chunk_nums = 0
            self.let_exit = False
            self.chunk_sizes = []
    
    def _call_back(chunk_info, chunk):

        while thead_control.sig_update_turn != chunk_info["part_number"] and not thead_control.let_exit:
            time.sleep(3)
        if thead_control.let_exit:
            raise Exception("One of thread fails. Exit now!!!")
        
        thead_control.mutexLock.acquire()

        sig.update(chunk)
        crc32c.update(chunk)
        thead_control.sig_update_turn +=1
        thead_control.chunk_sizes.append(len(chunk))

        thead_control.mutexLock.release()

    def _handler(chunk_info):
        tries = 0
        request_success = False

        if thead_control.let_exit:
            raise Exception("One of thread fails. Exit now!!!")

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
            thead_control.mutexLock.acquire()
            thead_control.let_exit = True
            thead_control.mutexLock.release()

            raise Exception(
                "Can not open http connection to gdc api {}".format(data_endpoint)
            )

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
            thead_control.mutexLock.acquire()
            thead_control.let_exit = True
            thead_control.mutexLock.release()
            raise Exception(
                "Can not upload chunk data of {} to {}".format(fi["id"], target_bucket)
            )

        thead_control.mutexLock.acquire()
        thead_control.chunk_nums += 1
        thead_control.mutexLock.release()

        if thead_control.chunk_nums % 10 == 0 and not global_config.get("quiet"):
            logger.info(
                "Streamming {}. Received {} MB".format(
                    fi["id"], thead_control.chunk_nums * 1.0 / 1024 / 1024 * chunk_data_size
                )
            )
        _call_back(chunk_info, chunk)

    thead_control = ThreadControl()
    thread_client = storage.Client()
    sess = AuthorizedSession(thread_client._credentials)

    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
    data_endpoint = endpoint or "https://api.gdc.cancer.gov/data/{}".format(
        fi.get("id")
    )

    sig = hashlib.md5()
    crc32c = crcmod.predefined.Crc('crc-32c')

    chunk_data_size = global_config.get("data_chunk_size", 1024 * 1024 * 256)

    tasks = []
    for part_number, data_range in enumerate(
        generate_chunk_data_list(fi["size"], chunk_data_size)
    ):
        start, end = data_range
        tasks.append({"start": start, "end": end, "part_number": part_number + 1})

    pool = ThreadPool(global_config.get("multi_part_upload_threads", 10))

    for task in tasks:
        pool.add_task(_handler, task)
    pool.wait_completion()

    if thead_control.chunk_nums > 1:
        finish_compose_upload_gs(
            sess=sess, bucket_name=target_bucket, key=object_path, chunk_sizes=thead_control.chunk_sizes
        )

    sig_check_pass = validate_uploaded_data(
        fi, sess, target_bucket, sig, crc32c
    )

    if not sig_check_pass:
        delete_object(sess, target_bucket, object_path)
    else:
        logger.info(
            "successfully stream file {} to {}".format(object_path, target_bucket)
        )


def validate_uploaded_data(fi, sess, target_bucket, sig, crc32c):
    """
    validate uploaded data

    Args:
        fi(dict): file info
        sess(session): google client session
        target_bucket(str): aws bucket
        sig(sig): md5 of downloaded data from api

    Returns:
       bool: pass or not
    """

    object_path = "{}/{}".format(fi["id"], fi["file_name"])

    meta_data = get_object_metadata(sess, target_bucket, object_path)
    if meta_data.status_code != 200:
        return False

    if int(meta_data.json().get("size", "0")) != fi["size"]:
        logger.warn(
            "Can not stream the object {}. {} vs {}. Size does not match".format(fi.get("id"), int(meta_data.json().get("size", "0")), fi["size"])
        )
        return False

    if meta_data.json().get("crc32c", "") != base64.b64encode(crc32c.digest()):
        logger.warn(
            "Can not stream the object {} to {}. crc32c check fails".format(
                fi.get("id"), target_bucket
            )
        )
        return False

    if sig.hexdigest() != fi.get("md5"):
        logger.warn(
            "Can not stream the object {}. md5 check fails".format(fi.get("id"))
        )
        return False

    return True
