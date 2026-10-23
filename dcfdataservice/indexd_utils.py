import os
from urllib.parse import urlparse
import logging as logger
from retry import retry

from dcfdataservice import utils
from dcfdataservice.errors import APIError, UserError
from dcfdataservice.settings import PROJECT_ACL

logger.basicConfig(level=logger.INFO, format="%(asctime)s %(message)s")

NAMESPACE = ""
if os.getenv("AUTH_NAMESPACE"):
    NAMESPACE = "/" + os.getenv("AUTH_NAMESPACE").strip("/")
    logger.info("using namespace {}".format(NAMESPACE))
else:
    logger.warning("not using any auth namespace")

try:
    from dcfdataservice.settings import (
        REDACT_URL_VALUE,
        REDACT_AUTHZ_VALUE,
        REDACT_ACL_VALUE,
        REDACT_FILE_NAME_VALUE,
        REDACT_URL_METADATA_VALUE,
        REDACT_DESCRIPTION,
    )
except:
    logger.warning("Redact field value undefined in settings. Using default value")


def _remove_changed_url(doc, url):
    """
    Sometime a file's acl is changed, the service need to move that file
    from public to controlled bucket or vice versa) and update indexd.
    To update indexd, the service goes through all the url and determine if
    it is the one need to be removed (the one points to the bucket for the same
    project but different acl) and remove it

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


@retry(APIError, tries=10, delay=2)
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
                raise Exception(
                    'Only "open" and "phs[...]" ACLs are allowed. Got ACL "{}"'.format(
                        ace
                    )
                )
        authz = ["{}/programs/{}".format(NAMESPACE, ace) for ace in acl]

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


@retry(APIError, tries=10, delay=2)
def delete_record_from_indexd(uuid, indexclient):
    """
    remove url from indexd record

    Args:
        uuid(str): did
        indexclient(IndexClient): indexd client
    """
    try:
        doc = indexclient.get(uuid)
        if doc:
            doc.delete()
    except Exception as e:
        raise APIError(
            "INDEX_CLIENT: Can not delete the record with uuid {}. Detail {}".format(
                uuid, e
            )
        )


@retry(APIError, tries=10, delay=2)
def redact_info_from_indexd(uuid, indexclient):
    """
    redact info from indexd record

    Args:
        uuid(str): did
        indexclient(IndexClient): indexd client
    """
    try:
        doc = indexclient.get(uuid)
        if doc:
            doc.urls = REDACT_URL_VALUE or []
            doc.authz = REDACT_AUTHZ_VALUE or []
            doc.acl = REDACT_ACL_VALUE or []
            doc.file_name = REDACT_FILE_NAME_VALUE or ""
            doc.urls_metadata = REDACT_URL_METADATA_VALUE or {}
            doc.description = (
                REDACT_DESCRIPTION
                or "The file information for this GUID has been removed at stakeholder request"
            )
            try:
                doc.patch()
            except Exception as e:
                raise APIError(
                    "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                        uuid, e
                    )
                )
        else:
            logger.warning("%s not found in indexd", uuid)
    except Exception as e:
        raise APIError(
            "INDEX_CLIENT: Can not redact info from the record with uuid {}. Detail {}".format(
                uuid, e
            )
        )
