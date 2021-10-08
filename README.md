# provectus-internship-task

## Table of contents
1. [ Prerequisites. ](#prereq)
2. [ Description. ](#desc)
3. [ Installation and Running. ](#install)
4. [ Extra Notes. ](#usage)

<a name="prereq"></a>
### 1. Prerequisites
- Python 3.7 or greater
- Docker 19.03 or greater
- Git 2.28 or greater
- Postgres 13 or greater
- Docker-compose 1.26 or greater

To install `docker-compose` visit the following [link](https://docs.docker.com/compose/install/).


<a name="desc"></a>
### 2. Description
In this project we implemented a dockerized service to process users data, which is equavilent to **Level 3*. It extracts the users data `(first_name, last_name, birthts)` from `minio` and finds the image path `img_path` 
for each user (if any) and then stores the intermediate results in `minio`. Finally all processed data is then migrated to `postgres` database. The service 
periodically processes the data inside `minio/srcdata`. You can interact with the service with a `flask` server that works on two endpoints
- POST http://localhost:5000/data - Manually run data processing in `minio/src_data`
- GET http://localhost:5000/data - retrieves all records from `postgres` DB in JSON format:
```
{
  user_id1: {
      first_name: 'example_first_name',
      last_name: 'example_last_name',
      birthts: 'example_birthts',
      img_path: 'example_img_path'
  },
  user_id2: {
      ...
  },
  ...
}
```
You can apply the following filters for the retrieved data:
- `is_image_exists`=True/False
  - if True, it retrieves the records of all users that *have* an image.
  - if False, it retrieves the records of all users that * do not have* an image.
  - if it was not specified, it retrieves the records of all users in the database.
- `min_age`=MIN_AGE - if specified, it retrieves the records of all users that has a minimum age of `MIN_AGE`
- `max_age`=MAX_AGE - if specified, it retrieves the records of all users that has a maximum age of `MAX_AGE`

To apply a filter you can proved them as tags in the url, for example: [http://localhost:5000/data?min_age=35&max_age=40](http://localhost/data?min_age=35&max_age=40)
returns the records of all users that age is between 35 and 40.

<a name="install"></a>
### 3. Installation and Running
Clone this repo to your local machine. `cd` to the directory of the repo `provectus-internship-task` and run:
```
sudo docker-compose up --build -d
```
After you run the docker-compose. You need to give access to minio and pgadmin directories. On the same terminal type `Ctrl+C` and then run:
```
sudo chmod 777 minio/
sudo chmod 777 pgadmin/
```
Now you need to restart the `docker-compose` containers you ran before. On the same terminal run:
```
sudo docker-compose down
sudo docker-compose up --build -d
```
Now the service is up and running on [http://localhost:5000/](http://localhost:5000/).


<a name="usage"></a>
### 4. Extra Notes

After you run the `docker-compose`, to manually add data to `srcdata` you need to `cd` to `provectus-internship-task/minio` and run the following command 
to give access to `srcdata` so you can paste your data there:
```
sudo chmod 777 srcdata/
```


