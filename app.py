from flask import Flask, jsonify, request
from dataprocessing.main import DataProcessing
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from dataprocessing.minioConnect import MyMinio
from dataprocessing.postgresConnect import MyPgConnect
import os
from decouple import config

SRC_DATA_PATH = 'srcdata'
PROCESSED_DATA_PATH = 'processeddata'


MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", config("MINIO_ACCESS_KEY"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", config("MINIO_SECRET_KEY"))

POSTGRES_DB = os.getenv("POSTGRES_DB", config("POSTGRES_DB"))
POSTGRES_USER = os.getenv("POSTGRES_USER", config("POSTGRES_USER"))
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", config("POSTGRES_PASSWORD"))
POSTGRES_HOST = os.getenv("POSTGRES_HOST", config("POSTGRES_HOST"))

FLASK_RUN_HOST = os.getenv("FLASK_RUN_HOST", config("FLASK_RUN_HOST"))
FLASK_RUN_PORT = int(os.getenv("FLASK_RUN_PORT", config("FLASK_RUN_PORT")))

app = Flask(__name__)

data_processor = DataProcessing(SRC_DATA_PATH, PROCESSED_DATA_PATH)


def process_data():
    """
    function to automate the process of processing the data
    """
    data_processor.process_data()


scheduler = BackgroundScheduler()

# Periodically process the data each 10 minutes
scheduler.add_job(process_data, 'interval', seconds=600)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())


@app.route("/data", methods=['POST'])
def process_data():
    data_processor.process_data()
    return jsonify("Data has been processed!")


@app.route("/data", methods=['GET'])
def get_data():
    # Read the filters
    is_image_exists = request.args.get('is_image_exists', -1)
    if is_image_exists != -1:
        is_image_exists = is_image_exists.lower()
    min_age_years = request.args.get('min_age', -1)
    max_age_years = request.args.get('max_age', -1)

    data = data_processor.get_data(is_image_exists=is_image_exists, min_age=min_age_years, max_age=max_age_years)
    return jsonify(data)


if __name__ == "__main__":

    # Connect to minio
    myMinioClient = MyMinio(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    # Create the two main buckets
    myMinioClient.create_bucket(SRC_DATA_PATH)
    myMinioClient.create_bucket(PROCESSED_DATA_PATH)

    # Create the output file
    try:
        myMinioClient.read_csv_file_from_minio(PROCESSED_DATA_PATH, 'output.csv')
    except:
        header = "user_id,first_name,last_name,birthts,img_path"
        myMinioClient.add_file(PROCESSED_DATA_PATH, 'output.csv', header)

    # connect to pgadmin
    pg_instance = MyPgConnect(dbname='internship', user=POSTGRES_USER,
                              password=POSTGRES_PASSWORD, host=POSTGRES_HOST)

    # create the main table
    pg_instance.create_table_users()

    # run the flask app
    app.run(host=FLASK_RUN_HOST, port=FLASK_RUN_PORT)
