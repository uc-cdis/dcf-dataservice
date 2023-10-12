import os
import ast
import csv

from typing import Union
from cdislogging import get_logger

logging = get_logger("__name__")


def _get_directory_files(directory: str):
    """
    List all file paths in a directory.
    Args:
        directory(str):
            Path of the directory containing the input manifests. All of the manifests contained in directory are assumed to be in a delimiter-separated values (DSV) format, and that there are no other non-DSV files in directory.
    Returns:
        List of files in directory
    """

    logging.info(f"Iterating over files in {directory} directory")

    files = []
    if os.path.exists(directory):
        directory_list = os.listdir(directory)
    else:
        raise NotADirectoryError()
    if directory_list:
        for file in sorted(directory_list):
            files.append(os.path.join(directory, file))

        files.sort()
    else:
        raise RuntimeError(f"Directory {directory} is empty")
    return files


def _delimiter(file_name: str):
    """
    Determine delimiter of DSV file from filename, non-DSV files not accepted.
    Args:
        file_name(str):
            Filename or file path
    Returns:
        DSV file delimiter
    """

    ext = file_name.lower()[-3:]
    delimiter = ""
    if ext == "tsv":
        delimiter = "\t"
    elif "csv":
        delimiter = ","
    elif "txt":
        delimiter = "."
    else:
        raise ValueError(
            "Please check you are providing file with an appropriate extension - TSV, CSV, or TXT. Filenames without an extension will error."
        )

    return delimiter


def get_headers(file_name: str):
    """
    Get headers from DSV file
    Args:
        file_name(str):
            Filename or file path
    Returns:
        List of headers
    """

    logging.info(f"Collecting headers from {file_name}...")

    delimiter = _delimiter(file_name)
    headers = []
    if delimiter == ".":
        file = open(file_name, "r")
        lines = file.readlines()
        headers.append(lines[0])
    else:
        with open(file_name) as process_file:
            file_reader = csv.reader(process_file, delimiter=delimiter)
            headers = next(file_reader)

    return headers


def file_to_list(file_name: str, skip_headers=False):
    """
    Collect records from DSV file
    Args:
        file_name(str):
            Filename or file path
        skip_headers(bool):
            True skip, False include in final list
    Returns:
        List of lists (rows) from input file
    """

    logging.info(f"Collecting records from {file_name}...")

    delimiter = _delimiter(file_name)
    records = []
    if delimiter == ".":
        file = open(file_name, "r")
        lines = file.readlines()
        if skip_headers:
            lines.pop(0)
        for line in lines:
            records.append(line)
    else:
        with open(file_name) as process_file:
            file_reader = csv.reader(process_file, delimiter=delimiter)
            if skip_headers:
                next(file_reader)
            for row in file_reader:
                values = []
                for column in row:
                    if "[" in column:
                        values.append(ast.literal_eval(column))
                    else:
                        values.append(column)
                records.append(values)

    return records


def file_to_listdict(file_name: str):
    """
    Collect records from DSV file
    Args:
        file_name(str):
            Filename or file path
    Returns:
        List of dictionaries from input file, with header:row pairs
    """

    logging.info(f"Collecting records from {file_name}...")

    delimiter = _delimiter(file_name)
    records = []

    if delimiter == "txt":
        file = open(file_name, "r")
        lines = file.readlines()

        # TODO finish this section
    else:
        with open(file_name) as process_file:
            file_reader = csv.reader(process_file, delimiter=delimiter)
            headers = next(file_reader)
            for row in file_reader:
                values = []
                for column in row:
                    if "[" in column:
                        values.append(ast.literal_eval(column))
                    else:
                        values.append(column)
                records.append(dict(zip(headers, values)))

    return records


def resort_list_columns(
    reordered_headers: list,
    file_name: str,
):
    """
    Move columns in list to match header order
    Args:
        reordered_headers(list):
            ""
        file_name(str):
            Filename or file path
    Returns:
        List of lists (rows) from input file, with modified order based on given headers
    """

    original_headers = get_headers(file_name)
    if sorted(reordered_headers) == sorted(original_headers):
        relational_indices = []
        for header in reordered_headers:
            relational_indices.append(original_headers.index(header))

        file_list = file_to_list(file_name, True)
        reordered_file_list = []
        for row in file_list:
            reordered_row = []
            for column in relational_indices:
                reordered_row.append(row[column])
            reordered_file_list.append(reordered_row)

        reordered_file_list.insert(0, reordered_headers)
        return reordered_file_list

    else:
        raise AssertionError("Columns do not have matching headers")


def write(records: list, output_file_name: str):
    """
    Write records to DSV file
    Args:
        records(list):
            List of rows, each row a list itself (list of lists)
        output_file_name(str):
            Filename or file path, expect file extension in the filename to determine file type
    Returns:
        None
    """

    logging.info(f"Writing to {output_file_name}...")

    delimiter = _delimiter(output_file_name)
    with open(output_file_name, "w") as output:
        if delimiter == ".":
            output.writelines(records)
        else:
            output_writer = csv.writer(output, delimiter=delimiter)
            for record in records:
                output_writer.writerow(record)


def convert_type(file_name: str, new_type: str, skip_headers=False):
    """
    Convert file to different extension type
    Args:
        file_name(str):
            Filename or file path
        new_type(str):
            Name of DSV type file is being converted to
        skip_headers(bool):
            True skip, False include in final list
    Returns:
        None
    """

    logging.info(f"Converting {file_name} to {new_type}...")

    records = file_to_list(file_name, skip_headers)
    new_file_name = f"{file_name[:-3:]}{new_type.lower()}"
    write(records, new_file_name)


def merge_files(files: Union[str, list], output_file_name=None, has_headers=False):
    """
    Merge multiple files into one file, files must have same headers if present
    Args:
        files(str | list):
            List of filepaths, or string directory path
        output_file_name(str):
            Name of file to be output
        has_headers(bool):
            True when headers present
    Returns:
        None
    """

    logging.info(f"Merging files: {files}...")

    if type(files) is str:
        files = _get_directory_files(files)

    if len(files) > 1:
        if has_headers:
            headers = get_headers(files[0])
            for pos in range(1, len(files)):
                other_headers = get_headers(files[pos])
                if headers != other_headers:
                    raise Exception(
                        "Headers and header position must match, cannot merge files"
                    )

        all_lists = []
        for file in files:
            all_lists.append(file_to_list(file, has_headers))
        all_records = [item for list_ in all_lists for item in list_]

        if has_headers:
            all_records.insert(0, headers)

        if output_file_name is None:
            output_file_name = "merged_files.tsv"
        write(all_records, output_file_name)
    else:
        logging.info("Must be more than one file to merge, did nothing")


def split_by_headers(file_name: str):
    """
    Split records by header and write into separate files
    Args:
        file_name(str):
            Filename or file path
    Returns:
        None
    """
    logging.info(f"Splitting {file_name}...")

    records = file_to_list(file_name, True)
    headers = get_headers(file_name)
    all_chunks = []
    for i in range(len(headers)):
        chunk = []
        chunk.append(headers[i])
        for record in records:
            chunk.append(record[i])
        all_chunks.append(chunk)

    for i in range(len(all_chunks)):
        ext = file_name[-3::]
        output_file_name = f"{file_name[:-4:]}_{headers[i]}.{ext}"
        write(all_chunks[i], output_file_name)


def chunk(
    file_name: str,
    chunk_size: int,
    has_headers=False,
):
    """
    Chunk records into files of input size
    Args:
        file_name(str):
            Filename or file path
        chunk_size(int):
            Size of desired segment
        has_headers(bool):
            True when headers present
    Returns:
        None
    """
    logging.info(f"Chunking records into {chunk_size} size...")

    records = file_to_list(file_name, has_headers)
    all_chunks = []
    chunk = []
    marker = 0
    for record in records:
        if marker != chunk_size:
            chunk.append(record)
            marker += 1
        else:
            all_chunks.append(chunk)
            chunk = []
            chunk.append(record)
            marker = 1
    if len(chunk) > 0:
        all_chunks.append(chunk)

    if has_headers:
        headers = get_headers(file_name)
    for i in range(len(all_chunks)):
        if has_headers:
            all_chunks[i].insert(0, headers)
        ext = file_name[-3::]
        output_file_name = f"{file_name[:-4:]}_{i}.{ext}"
        write(all_chunks[i], output_file_name)


def file_difference(
    file_one: str, file_two: str, has_headers=False, strict=False, large_unique_key=None
):
    """
    Take difference between two files, files must have matching column headers
    Args:
        file_one(str):
            Filename or file path
        file_two(str):
            Filename or file path
        has_headers(bool):
            True when headers present
        strict(bool):
            When true, will look for exact match; else, will match lower and uppercase strings and match array of different order
        large_unique_key(str):
            str represtation of unique id column for record, files must have headers
    Returns:
        List of lists, returns rows that contain at least one difference
    """

    logging.info(f"Taking differences between {file_one} & {file_two}...")

    if has_headers:
        file_one_headers = get_headers(file_one)
        file_two_headers = get_headers(file_two)

        if file_one_headers == file_two_headers:
            if not large_unique_key:
                list_one = file_to_list(file_one, has_headers)
                list_two = file_to_list(file_two, has_headers)
            else:
                list_one = file_to_listdict(file_one)
                list_two = file_to_listdict(file_two)
        elif sorted(file_one_headers) == sorted(file_two_headers):
            if not large_unique_key:
                list_one = file_to_list(file_one, has_headers)
                list_two = resort_list_columns(file_one_headers, file_two).pop(0)
            else:
                list_one = file_to_listdict(file_one)
                list_two = file_to_listdict(file_two)
        else:
            raise AssertionError("Columns do not have matching headers")

    if not strict:
        for row in list_one:
            values = []
            for column in row:
                if isinstance(column, str):
                    values.append(column.lower())
                else:
                    values.append(sorted(column))
        for row in list_two:
            values = []
            for column in row:
                if isinstance(column, str):
                    values.append(column.lower())
                else:
                    values.append(sorted(column))

    if not large_unique_key:
        return [row for row in list_one if row not in list_two] + [
            row for row in list_two if row not in list_one
        ]
    else:
        dict_one = {}
        for record in list_one:
            key = record[large_unique_key]
            values = []
            for val in record.values():
                values.append(val)
            dict_one[key] = values

        dict_two = {}
        for record in list_two:
            key = record[large_unique_key]
            values = []
            for val in record.values():
                values.append(val)
            dict_two[key] = values

        return [dict_one[key] for key in dict_one if key not in dict_two] + [
            dict_two[key] for key in dict_two if key not in dict_one
        ]


def file_intersection(file_one: str, file_two: str, has_headers=False, strict=False):
    """
    Take intersection between two files, files must have matching column headers
    Args:
        file_one(str):
            Filename or file path
        file_two(str):
            Filename or file path
        has_headers(bool):
            True when headers present
        strict(bool):
            When true, will look for exact match; else, will match lower and uppercase strings and match array of different order
    Returns:
        List of lists, returns rows that contain matches between all columns
    """

    logging.info(f"Taking intersection between {file_one} & {file_two}...")

    list_one = file_to_list(file_one, has_headers)

    if has_headers:
        file_one_headers = get_headers(file_one)
        file_two_headers = get_headers(file_two)

        if file_one_headers == file_two_headers:
            list_one = file_to_list(file_one, has_headers)
            list_two = file_to_list(file_two, has_headers)
        elif sorted(file_one_headers) == sorted(file_two_headers):
            list_one = file_to_list(file_one, has_headers)
            list_two = resort_list_columns(file_one_headers, file_two).pop(0)
        else:
            raise AssertionError("Columns do not have matching headers")

    if not strict:
        for row in list_one:
            values = []
            for column in row:
                if isinstance(column, str):
                    values.append(column.lower())
                else:
                    values.append(sorted(column))
        for row in list_two:
            values = []
            for column in row:
                if isinstance(column, str):
                    values.append(column.lower())
                else:
                    values.append(sorted(column))

    return [row for row in list_one if row in list_two]


# TODO
# def file_subset(
#     file_name: str,
#     criteria: str,
#     column: str,
#     has_headers=False
# ):
#     """
#     Find all records that match a given criteria within a specified column
#     Args:
#         file_name(str):
#             Filename or file path
#         criteria(str):
#             Expression to match (for example: >5, ==3, ==this_guid)
#         column(str | int):
#             If has_headers, str name of column to search for criteria; else, int index of column
#         has_headers(bool):
#             True when headers present
#     Returns:
#         List of lists, returns rows that match criteria
#     """
#     logging.info(f"Finding subset of records that match {criteria} for {column}...")

#     records = file_to_list(file_name, has_headers)
#     subset = []
#     # do stuff here

#     return subset
