import _io
import csv
import datetime
from dataprocessing.minioConnect import MyMinio
import time
epoch = datetime.datetime.utcfromtimestamp(0)


def make_2d_list(lst: list, k: int):
    """
    Transform a list of integer to a list of lists where the inner lists are of size k
    Note that len(lst) should be divisible by k
    :param lst: the list to restructure
    :param k: the integer as described above
    :return: The new 2D list
    """
    rows = []
    for i in range(0, len(lst), k):
        rows.append(lst[i: i + k])
    return rows


def test_files_validity(minioClient: MyMinio, csv_files: list, image_files: list, bucket_name: str):
    """
    Check if the source data files are valid. Check if csv files contain three columns.
    Check if csv files contain values for each of the three columns.
    """

    assert (len(csv_files) >= len(image_files), "There are missing csv files. "
                                                "There are more image files than csv.")

    for csv_file in csv_files:

        contents = minioClient.read_csv_file_from_minio(bucket_name, csv_file[0])

        rows = make_2d_list(contents, 3)

        assert (len([row for row in rows]) == 2)

        for row in rows:
            assert (len(row) == 3)

        assert (rows[0][0].strip(), 'first_name', "No 'first_name' column")
        assert (rows[0][1].strip(), 'last_name', "No 'last_name' column")
        assert (rows[0][2].strip(), 'birthts', "No 'birthts' column")

        for item in rows[1]:
            assert (len(item.strip()) != 0, f"Empty field in file {csv_file[0]}")


def millis_to_age(birthts: float):
    """
    Transform from timestamp ms to age in years
    :param birthts: the timestamp ms
    :return:
    """
    now = time.time() * 1000
    return (now - birthts) / (3.154 * 10 ** 10)


def dict_to_list(user_info: dict):
    return [user_info['user_id'], user_info['first_name'], user_info['last_name'],
            user_info['birthts'], user_info['img_path']]


def extract_csv(file: _io.TextIOWrapper):
    """
    Extract the information about a specific user from 'file' which contains
    first_name, last_name, birthts
    :param file: the info file of a user
    :return: A dictionary with the keys: ['first_name', 'last_name', 'birthts']
    that contains the info about a user
    """

    columns = []
    user_info = {}
    for row in csv.reader(file, delimiter=','):
        if not columns:
            columns = row
            continue
        for i in range(len(row)):
            user_info[columns[i].strip()] = row[i].strip()
    return user_info

