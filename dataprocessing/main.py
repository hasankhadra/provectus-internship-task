import csv
import os
from dataprocessing.utils.helpers import dict_to_list, millis_to_age, make_2d_list, test_files_validity
from dataprocessing.minioConnect import MyMinio
from minio.error import S3Error
from dataprocessing.postgresConnect import MyPgConnect


class DataProcessing:

    def __init__(self, src_data_path: str, processed_data_path: str):
        """
        Initialize the instance with the files' paths that the data processing will be applied to
        :param src_data_path: the path of the source data where the information
        will be retrieved
        :param processed_data_path: the path of the processed data where the results
        will be stored
        """
        self.src_data_path = src_data_path
        self.processed_data_path = processed_data_path

        self.minioClient = MyMinio('minio-access-key', 'minio-secret-key')
        self.pg_instance = MyPgConnect(dbname='internship', user='postgres',
                                       password='postgres', host='db')

    def check_id(self, user_id: str):
        """
        Check if the user with id 'user_id' already exist in the 'self.processed_data_path', and overwrite it.
        :param user_id: the id of the user that we will check if it exists in the results file.
        """

        try:
            contents = self.minioClient.read_csv_file_from_minio(self.processed_data_path, 'output.csv')
            rows = make_2d_list(contents, 5)
        except S3Error:
            return

        to_write = [row for row in rows if row[0] != user_id]

        write_obj = open("temp.csv", 'w+')
        csv.writer(write_obj).writerows(to_write)

        write_obj.close()

        self.minioClient.minioClient.remove_object(self.processed_data_path, 'output.csv')
        self.minioClient.minioClient.fput_object(self.processed_data_path, 'output.csv', 'temp.csv')
        os.remove('temp.csv')

    def get_data(self, **filters):
        """
        Retrieves the users' processed data from 'self.processed_data_path' path after applying
        the given 'filters'.
        :param filters: a dictionary that contains the filters to apply to the data.
        Possible keys for 'filters':
        'is_image_exists' which specifies if the users must have an image or not
        'min_age' lowerbound of the age of the users
        'max_age' upperbound of the age of the users

        :return: the processed and filtered data in JSON format:
        {
            user_id1: {
                first_name: 'example_first_name',
                last_name: 'example_last_name',
                birthts: 'example_birthts',
                img_path: 'example_img_path'
            },
            user_id2:{
                ...
            }
            ,
            ...
        }
        """

        data = self.pg_instance.get_users_data('users')
        data = make_2d_list(data, 5)

        if filters['is_image_exists'] != -1:
            if filters['is_image_exists'] == 'True':
                data = [row for row in data if row[4] != "No image found"]
            elif filters['is_image_exists'] == 'False':
                data = [row for row in data if row[4] == "No image found"]

        if filters['min_age'] != -1:
            minimum_age = float(filters['min_age'])
            data = [row for row in data if float(millis_to_age(float(row[3]))) >= minimum_age]

        if filters['max_age'] != -1:
            maximum_age = float(filters['max_age'])
            data = [row for row in data if float(millis_to_age(float(row[3]))) <= maximum_age]

        final_data = {
            row[0]: {
                'first_name': row[1],
                'last_name': row[2],
                'birthts': row[3],
                'img_path': row[4],
            } for row in data
        }

        return final_data

    def get_files(self):
        """
        Get the names of the csv and image files from the 'self.src_data_path' path
        :return: two lists of files' paths and names of the format (csv_files, image_files)
        csv_files format: [(file_path, file_name)]
        image_files format: [(file_path, file_name)]
        """

        files = self.minioClient.get_files_in_bucket(self.src_data_path)

        csv_files = [(file, file[:file.find('.')])
                     for file in files if file.endswith('.csv')]

        image_files = [(f"{self.src_data_path}/{file}", file[:file.find('.')])
                       for file in files if not file.endswith('.csv')]

        return csv_files, image_files

    def add_data(self, user_data: dict):
        """
        Add the data of a user to 'processed_data_path' file.

        :param user_data: a dict of the data of the user.
        'user_data' keys are: [user_id, first_name, last_name, birthts, img_path]
        """
        self.check_id(user_data['user_id'])

        try:
            contents = self.minioClient.read_csv_file_from_minio(self.processed_data_path, 'output.csv')
            to_write = make_2d_list(contents, 5)
        except S3Error:
            to_write = []

        with open('temp.csv', 'w+') as writefile:
            csv_writer = csv.writer(writefile)

            if len(to_write) == 0:
                csv_writer.writerow(['user_id', 'first_name', 'last_name', 'birthts', 'img_path'])

            user_data = dict_to_list(user_data)
            to_write.append(user_data)

            csv_writer.writerows(to_write)

            writefile.close()

            self.minioClient.minioClient.remove_object(self.processed_data_path, 'output.csv')
            self.minioClient.minioClient.fput_object(self.processed_data_path, 'output.csv', 'temp.csv')
            os.remove('temp.csv')

    def process_data(self):
        """
        Process the data from 'self.src_data_path' and add the results to 'self.processed_data_path'.
        """
        csv_files, image_files = self.get_files()

        try:
            test_files_validity(self.minioClient, csv_files, image_files, self.src_data_path)
        except Exception as e:
            print(e)
            return

        for csv_file, user_id in csv_files:

            contents = self.minioClient.read_csv_file_from_minio(self.src_data_path, csv_file)

            user_info = {
                'user_id': user_id,
                'first_name': contents[3],
                'last_name': contents[4],
                'birthts': contents[5]
            }

            try:
                image_path = [image[0] for image in image_files if image[1] == user_id][0]
                user_info['img_path'] = image_path
            except:  # Check if the user does not have an image file
                user_info['img_path'] = "No image found"

            self.add_data(user_info)

        contents = self.minioClient.read_csv_file_from_minio(self.processed_data_path, 'output.csv')[5:]

        self.pg_instance.insert_users('users', contents)

