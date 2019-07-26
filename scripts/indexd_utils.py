from cdislogging import get_logger
from errors import APIError, UserError
import os
import utils
from settings import PROJECT_ACL
from urlparse import urlparse


logger = get_logger("IndexdUtils")


NAMESPACE = ""
if os.getenv("AUTH_NAMESPACE"):
    NAMESPACE = "/" + os.getenv("AUTH_NAMESPACE").strip("/")
    logger.info("using namespace {}".format(NAMESPACE))
else:
    logger.info("not using any auth namespace")


def _remove_changed_url(doc, url):
    """
    due to acl change, we need to update indexd to point to the right bucket
    """
    res1 = urlparse(url)
    modified = False
    for element in doc.urls:
        res2 = urlparse(element)
        if res1.scheme != res2.scheme:
            continue
        bucket1 = res1.netloc
        bucket2 = res2.netloc
        if {bucket1, bucket2} == {"ccle-open-access", "gdc-ccle-controlled"}:
            doc.urls.remove(element)
            doc.urls_metadata.pop(element, None)
            modified = True
            continue
        # Check if bucket1 and bucket2 are from the same project. 
        # gdc_project_phs_controlled and gdc_project_phs_open are from the same project since
        # the prefixes are the same
        bucket1 = bucket1[:-5] if bucket1.endswith("-open") else bucket1[:-11]
        bucket2 = bucket2[:-5] if bucket2.endswith("-open") else bucket2[:-11]
        if bucket1 == bucket2:
            doc.urls.remove(element)
            doc.urls_metadata.pop(element, None)
            modified = True

    return doc, modified


def update_url(fi, indexclient, provider="s3", url=None):
    """
    update a record to indexd
    Args:
        fi(dict): file info
    Returns:
        bool: if record is created/updated or not
    """
    if url is None:
        try:
            if provider == "s3":
                bucket_name = utils.get_aws_bucket_name(fi, PROJECT_ACL)
            else:
                bucket_name = utils.get_google_bucket_name(fi, PROJECT_ACL)
            object_key = "{}/{}".format(fi.get("id"), fi.get("file_name"))
        except UserError as e:
            raise APIError(
                "Can not get the bucket name of the record with uuid {}. Detail {}".format(
                    fi.get("id", ""), e
                )
            )
        url = "{}://{}/{}".format(provider, bucket_name, object_key)

    if fi.get("acl") in {"[u'open']", "['open']"}:
        acl = ["*"]
        authz = ["/open"]
    else:
        acl = [
            ace.strip().replace("u'", "").replace("'", "")
            for ace in fi.get("acl", "").strip()[1:-1].split(",")
        ]
        for ace in acl:
            if not ace.startswith("phs"):
                raise Exception('Only "open" and "phs[...]" ACLs are allowed. Got ACL "{}"'.format(ace))
        authz = [
            "{}/programs/{}".format(NAMESPACE, ace)
            for ace in acl
        ]

    try:
        doc = indexclient.get(fi.get("id", ""))

        # record already exists: patch it
        if doc is not None:
            need_update = False
            if url not in doc.urls:
                doc, _ = _remove_changed_url(doc, url)
                doc.urls.append(url)
                need_update = True

            if set(doc.acl) != set(acl):
                doc.acl = acl
                need_update = True

            if set(doc.authz) != set(authz):
                doc.authz = authz
                need_update = True

            if need_update:
                doc.patch()
            return doc is not None
    except Exception as e:
        # Don't break for any reason
        raise APIError(
            "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                fi.get("id", ""), e
            )
        )

    # record does not already exist: create it
    urls = ["https://api.gdc.cancer.gov/data/{}".format(fi.get("id", "")), url]
    try:
        doc = indexclient.create(
            did=fi.get("id"),
            hashes={"md5": fi.get("md5")},
            size=fi.get("size", 0),
            acl=acl,
            authz=authz,
            urls=urls,
        )
        return doc is not None
    except Exception as e:
        # Don't break for any reason
        raise APIError(
            "INDEX_CLIENT: Can not create the record with uuid {}. Detail {}".format(
                fi.get("id", ""), e
            )
        )


def remove_url_from_indexd_record(uuid, urls, indexclient):
    """
    remove url from indexd record

    Args:
        uuid(str): did
        urls(list): list of urls
        indexclient(IndexClient): indexd client
    """
    doc = indexclient.get(uuid)
    if doc is not None:
        for url in urls:
            if url in doc.urls:
                doc.urls.remove(url)
            if url in doc.urls_metadata:
                del doc.urls_metadata[url]
        try:
            doc.patch()
        except Exception as e:
            raise APIError(
                "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                    uuid, e
                )
            )
