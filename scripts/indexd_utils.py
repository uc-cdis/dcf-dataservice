from errors import APIError
import utils
from settings import PROJECT_ACL


def update_url(fi, indexclient, provider="s3"):
    """
    update a record to indexd
    Args:
        fi(dict): file info
    Returns:
        None
    """

    if provider == "s3":
        bucket_name = utils.get_aws_bucket_name(fi, PROJECT_ACL)
    else:
        bucket_name = utils.get_google_bucket_name(fi, PROJECT_ACL)
    s3_object_name = "{}/{}".format(provider, fi.get("id"), fi.get("file_name"))

    url = "{}://{}/{}".format(provider, bucket_name, s3_object_name)

    try:
        doc = indexclient.get(fi.get("id", ""))

        if doc is not None:
            need_update = False
            if url not in doc.urls:
                doc.urls.append(url)
                need_update = True

            acl = (
                ["*"]
                if fi.get("acl") in {"[u'open']", "['open']"}
                else fi.get("acl")[1:-1].split(",")
            )
            if doc.acl != acl:
                doc.acl = acl
                need_update = True

            if need_update:
                doc.patch()
            return doc is not None
    except Exception as e:
        raise APIError(
            "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                fi.get("id", ""), e
            )
        )

    urls = ["https://api.gdc.cancer.gov/data/{}".format(fi.get("id", "")), url]
    acl = (
        ["*"]
        if fi.get("acl") in {"[u'open']", "['open']"}
        else fi.get("acl")[1:-1].split(",")
    )
    try:
        doc = indexclient.create(
            did=fi.get("id"),
            hashes={"md5": fi.get("md5")},
            size=fi.get("size", 0),
            acl=acl,
            urls=urls,
        )
        return doc is not None
    except Exception as e:
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
