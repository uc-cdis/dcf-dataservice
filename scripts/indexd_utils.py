from errors import APIError
from utils import get_bucket_name
from settings import PROJECT_MAP


def update_url(fi, indexclient, provider="s3"):
    """
    update a record to indexd
    Args:
        fi(dict): file info
    Returns:
        None
    """

    s3_bucket_name = get_bucket_name(fi, PROJECT_MAP)
    s3_object_name = "{}/{}".format(provider, fi.get("id"), fi.get("filename"))

    url = "{}://{}/{}".format(provider, s3_bucket_name, s3_object_name)

    try:
        doc = indexclient.get(fi.get("id", ""))
        if doc is not None:

            if url not in doc.urls:
                doc.urls.append(url)
                doc.patch()
            return doc is not None
    except Exception as e:
        raise APIError(
            "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                fi.get("id", ""), e
            )
        )

    urls = ["https://api.gdc.cancer.gov/data/{}".format(fi.get("id", "")), url]

    try:
        doc = indexclient.create(
            did=fi.get("id"),
            hashes={"md5": fi.get("md5")},
            size=fi.get("size", 0),
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
            doc.urls.remove(url)
            if url in doc.urls_metadata:
                del doc.urls_metadata[url]
        try:
            doc.patch()
        except Exception as e:
            raise APIError(
                "INDEX_CLIENT: Can not update the record with uuid {}. Detail {}".format(
                    uuid, e.message
                )
            )
