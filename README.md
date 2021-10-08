# provectus-internship-task

## Table of contents
1. [ Data Processing Task - Level 3. ](#level3)
2. [ Coding Tasks for Data Engineers. ](#codingtasks)

<a name="level3"></a>
## 1. Data Processing Task - Level 3

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
In this project we implemented a dockerized service to process users data, which is equavilent to **Level 3**. It extracts the users data `(first_name, last_name, birthts)` from `minio` and finds the image path `img_path` 
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

<a name="codingtasks"></a>
## 2. Coding Tasks for Data Engineers

### SQL
1. 
```
SELECT users.id as id
FROM users
LEFT JOIN departments
ON users.id = departments.user_id
WHERE departments.user_id is NULL OR departments.department_id != 1;
```

2.
```
SELECT last_name 
FROM user
GROUP BY last_name
HAVING COUNT(last_name) > 1;
```

3.
```
SELECT MAX(user.username) AS username, MAX(salary.salary) AS salary
FROM user, salary
WHERE user.id = salary.user_id AND salary.salary NOT IN (
	SELECT MAX(salary)
	FROM salary
);
```
Note that we wrote `MAX(user.username)` instead of `user.username` because if the `ONLY_FULL_GROUP_BY` is disabled 
in the server configuration it will give an error. However, if the `ONLY_FULL_GROUP_BY` is enabled, then the `MAX` keyword
can be removed and the query will work just fine.

### Algorithms and Data Structures
1.
```
def count_connections(list1: list, list2: list) -> int:
    cnt = {}

    # fill in the cnt dictionary where cnt[i] contains
    # the number of times the value i exists in list1
    for i in list1:
        if cnt.get(i) is None:
            cnt[i] = 1
        else:
            cnt[i] += 1

    count = 0

    # for each value i in list2, we sum the number of times
    # this value has occurred in list1
    for i in list2:
        count += cnt[i] if not cnt.get(i) is None else 0

    return count
```
2.
```
def longest_non_repeating_substring(s: str) -> int:

    # the smallest valid start of a beginning of the substring
    start = 0
    ans = 0

    # contains the last index of each character in the string
    last_index = {}

    for i in range(len(s)):

        # if this letter is unique till index i
        if not last_index.get(s[i]) is None:
            start = max(start, last_index[s[i]] + 1)

        # update the answer
        ans = max(ans, i - start + 1)

        # update the last seen index of character s[i]
        last_index[s[i]] = i

    return ans
```
3.
```
def index_of_target(nums: list, target: int):
    l, r = 0, len(nums) - 1

    # The smallest integer in nums greater or equal to target
    upper_bound = r + 1

    # use binary search to locate the index
    while l <= r:

        mid = (l + r) // 2

        if nums[mid] == target:
            return mid
        elif nums[mid] > target:
            upper_bound = mid
            r = mid - 1
        else:
            l = mid + 1

    return upper_bound
```
### Linux Shell

1. List processes listening on ports 80 and 443 - solution: `lsof -i :443 & lsof -i :80`
![ports](ports.png)
Note that the list of processes was very big and couldn't fit in one screen shot.
2. List process environment variables by given PID - solution: ``sudo cat /proc/`pgrep 'the process name'`/environ``
![environ](enviorn.png)
3. 
