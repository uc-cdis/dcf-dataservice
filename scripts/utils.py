import re
import boto3
import csv

from errors import UserError

# def Base64ToHexHash(base64_hash):
#    return hexlify(base64.decodestring(base64_hash.strip('\n"\'')))


def get_aws_bucket_name(fi, PROJECT_ACL):
    try:
        project_info = PROJECT_ACL[fi.get("project_id")]
    except KeyError:
        raise UserError("PROJECT_ACL does not have {} key".format(fi.get("project_id")))
    return project_info["aws_bucket_prefix"] + (
        "-open" if fi.get("acl") in {"[u'open']", "['open']", "*"} else "-controlled"
    )


def get_google_bucket_name(fi, PROJECT_ACL):
    try:
        project_info = PROJECT_ACL[fi.get("project_id")]
    except KeyError:
        raise UserError("PROJECT_ACL does not have {} key".format(fi.get("project_id")))
    return project_info["gs_bucket_prefix"] + (
        "-open" if fi.get("acl") in {"[u'open']", "['open']", "*"} else "-controlled"
    )


def get_google_bucket_name_auto(fi, project_acl):
    project_info = project_acl[fi.get("project_id")]

    if project_info["access_control_level"] == "program":
        return "gdc-{}-{}-{}".format(
            project_info["program_name"].lower(),
            project_info["program_phsid"].lower(),
            "open" if fi.get("acl") in {"[u'open']", "['open']"} else "controlled",
        )
    if project_info["access_control_level"] == "project":
        return "gdc-{}-{}-{}".format(
            project_info["program_id"].lower(),
            project_info["project_phsid"].lower(),
            "open" if fi.get("acl") in {"[u'open']", "['open']"} else "controlled",
        )
    return None


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
    """
    Get headers of the manifest file
    """
    try:
        with open(manifest_file, "r") as f:
            content = f.readlines()
            if len(content) <= 1:
                return []
            return content[0].replace("\r", "").replace("\n", "").split("\t")

    except IOError:
        print("File {} is not existed".format(manifest_file))
    return []


def get_fileinfo_list_from_s3_manifest(url_manifeat):
    """
    Get the manifest from s3
    pass to get_fileinfo_list_from_manifest to get 
    list of file info dictionary (size, md5, etc.)
    """

    s3 = boto3.resource("s3")
    from urlparse import urlparse

    out = urlparse(url_manifeat)
    s3.meta.client.download_file(out.netloc, out.path[1:], out.path[1:])
    return get_fileinfo_list_from_csv_manifest(out.path[1:])


def get_fileinfo_list_from_manifest(manifest_file):
    """
    get list of dictionaries from manifest file.
    Temporaly ignore this function until the officical manifest file comes out
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

    return l


def get_fileinfo_list_from_csv_manifest(manifest_file):
    """
    get file info from csv manifest
    """
    files = []
    with open(manifest_file, "rt") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=";")
        for row in csvReader:
            row["size"] = int(row["size"])
            files.append(row)

    return files


def exec_files_grouping(files):
    """
    Group files into multiple groups according to the target buckets.
    All files in the same group should be copied/deleted to/from the same bucket
    """
    project_acl_set = set()
    for fi in files:
        if fi.get("project_id"):
            project_acl_set.add(fi.get("project_id") + fi.get("acl"))

    file_grp = dict()
    key = 0
    while len(project_acl_set) > 0:
        project_acl = project_acl_set.pop()
        same_project_files = []
        for fi in files:
            if fi.get("project_id") + fi.get("acl") == project_acl:
                same_project_files.append(fi)
        if len(same_project_files) > 0:
            if key in file_grp:
                file_grp[key].append(same_project_files)
            else:
                file_grp[key] = same_project_files
            key = key + 1
    return file_grp
