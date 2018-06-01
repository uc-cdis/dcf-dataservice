from binascii import unhexlify, hexlify
import base64
import re

def Base64ToHexHash(base64_hash):
    return hexlify(base64.decodestring(base64_hash.strip('\n"\'')))

#st = Base64ToHexHash('YXg/2Z/PYhMRgRXqxJBjpg==')


def check_bucket_is_exists(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    return bucket.exists()

def get_bucket_name(fi, PROJECT_MAP):
    """
    TCGA-open -> gdc-tcga-phs000178-open
    TCGA-controled -> gdc-tcga-phs000178-controled
    TARGET-controled -> gdc-target-phs000218-controlled
    """
    bucketname = ''
    project = fi.get('project', '')
    if fi.get('acl', '') == "*":
        bucketname = 'gdc-' + PROJECT_MAP.get(project, '') + "-open"
    else:
        bucketname = 'gdc-' + PROJECT_MAP.get(project, '') + "-controlled"
    return bucketname

def extract_md5_from_text(text):
    """
    extract base-128 hash from string
    Args:
        text(str): string output from gsutil command
    Returns:
        hash(str) or None
    """
    m = re.search('[0-9,a-f]{32}', text.lower())
    if m:
        return m.group(0)
    else:
        return None

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
        with open(manifest_file,'r') as f:
            content = f.readlines()
            if len(content) <= 1:
                return l
            headers = content[0].replace("\r","").replace("\n","").split('\t')
            for row in content[1:]:
                dictionary = dict()
                values = row.replace("\r","").replace("\n","").split('\t')
                dictionary = dict(zip(headers, values))
                dictionary['size'] = int(dictionary['size'])
                l.append(dictionary)

            return l

    except IOError as e:
        print("File {} is not existed".format(manifest_file))
    return l

