import base64
import re
import argparse

# def Base64ToHexHash(base64_hash):
#    return hexlify(base64.decodestring(base64_hash.strip('\n"\'')))


def get_bucket_name(fi, PROJECT_MAP):
    """
    TCGA-open -> gdc-tcga-phs000178-open
    TCGA-controled -> gdc-tcga-phs000178-controled
    TARGET-controled -> gdc-target-phs000218-controlled
    """
    bucketname = ""
    project = fi.get("project", "")
    if fi.get("acl", "") == "*" or fi.get("acl", "") == "open":
        bucketname = "gdc-" + PROJECT_MAP.get(project, "") + "-open"
    else:
        bucketname = "gdc-" + PROJECT_MAP.get(project, "") + "-controlled"
    return bucketname


def extract_md5_from_text(text):
    """
    extract base-128 hash from string
    Args:
        text(str): string output from gsutil command
    Returns:
        hash(str) or None
    """
    m = re.search("[0-9,a-f]{32}", text.lower())
    if m:
        return m.group(0)
    else:
        return None


def get_headers(manifest_file):
    try:
        with open(manifest_file, "r") as f:
            content = f.readlines()
            if len(content) <= 1:
                return []
            return content[0].replace("\r", "").replace("\n", "").split("\t")

    except IOError:
        print("File {} is not existed".format(manifest_file))
    return []


def get_fileinfo_list_from_manifest(manifest_file):
    """
    get list of dictionaries from manifest file.
    Temporaly ignore this function until the officical manifest file come out
    [
        {
            'did':'11111111111111111',
            'filename': 'abc.bam',
            'size': 1,
            'hash': '1223344543t34mt43tb43ofh',
            'acl': 'tcga',
            'project': 'TCGA'
        },
    ]
    """
    l = []
    try:
        with open(manifest_file, "r") as f:
            content = f.readlines()
            if len(content) <= 1:
                return l, []
            headers = content[0].replace("\r", "").replace("\n", "").split("\t")
            for row in content[1:]:
                dictionary = dict()
                values = row.replace("\r", "").replace("\n", "").split("\t")
                dictionary = dict(zip(headers, values))
                dictionary["size"] = int(dictionary["size"])
                l.append(dictionary)

            return l, headers

    except IOError:
        print("File {} is not existed".format(manifest_file))
    return l, []


def split_manifest_file(manifest_file, file_nums=1):
    headers = get_headers(manifest_file)
    rows = get_fileinfo_list_from_manifest(manifest_file)
    nrow_in_subfile = len(rows) / file_nums
    file_index = 0
    for file_index in xrange(0, file_nums):
        sub_rows = []
        if file_index < file_nums - 1:
            sub_rows = rows[
                file_index * nrow_in_subfile : (file_index + 1) * nrow_in_subfile
            ]
        else:
            sub_rows = rows[file_index * nrow_in_subfile :]

        new_filename = "{}_{}".format(manifest_file, file_index)
        with open(new_filename, "w") as writer:
            writer.write(
                "{}\t{}\t{}\t{}\t{}\t{}".format(
                    headers[0],
                    headers[1],
                    headers[2],
                    headers[3],
                    headers[4],
                    headers[5],
                )
            )
            for row in sub_rows:
                writer.write(
                    "{}\t{}\t{}\t{}\t{}\t{}".format(
                        row.get(headers[0]),
                        row.get(headers[1]),
                        row.get(headers[2]),
                        row.get(headers[3]),
                        row.get(headers[4]),
                        row.get(headers[5]),
                    )
                )
