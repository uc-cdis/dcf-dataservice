import random
import uuid


from dcfdataservice.utils import merge_prev_manifest


def file_list_generator(n_records, available_versions=["1.0", "2.0", "3.0"]):
    """
    Create a list of dictionary records for testing

    Args:
        n_records (int): number of records to create
        available_versions (list): list of versions of release to pick from.
    """

    file_lists = []

    for i in range(n_records):
        release = random.choice(available_versions)
        record = {
            "id": str(uuid.uuid4()),
            "file_name": f"file_{i}",
            "md5": f"1809hnd02jpjdoiadjhoiasjo{i}",
            "size": 1234,
            "state": "released",
            "project_id": "myProject123",
            "baseid": str(uuid.uuid4()),
            "version": 1,
            "release": release,
            "acl": "open",
            "type": "data",
            "deletereason": "",
            "indexd_url": "[s3://bucket/url/file]",
            "case_submitter_ids": "id123",
        }
        file_lists.append(record)
    return file_lists


def test_files_no_intersection_manifest():
    """
    Test file with release with no overlap
    """
    prev_release_count = 10
    current_release_count = 20

    prev_release = file_list_generator(prev_release_count)
    current_release = file_list_generator(current_release_count, ["4.0"])

    final_list = merge_prev_manifest(prev_release, current_release)

    assert len(final_list) == prev_release_count + current_release_count


def test_files_intersection_manifest():
    """
    Test the final list is merged and make sure it updates with the latest information
    """

    prev_release_count = 10
    current_release_count = 20
    intersect_release_count = 7

    prev_release = file_list_generator(prev_release_count)
    current_release = file_list_generator(current_release_count, ["4.0"])

    intersect_release = []
    for i in prev_release[:intersect_release_count]:
        # changing the acl for these data in new release
        i["acl"] = "controlled"
        intersect_release.append(i)

    current_release = current_release + intersect_release

    final_list = merge_prev_manifest(prev_release, current_release)

    assert len(final_list) == prev_release_count + current_release_count

    acl_controlled_count = 0

    for j in final_list:
        if j["acl"] == "controlled":
            acl_controlled_count += 1

    assert acl_controlled_count == intersect_release_count
