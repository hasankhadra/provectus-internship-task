import psycopg2


class MyPgConnect:

    def __init__(self, dbname: str, user: str, password: str, host: str):
        """
        Stores the connection credintials to connect to the database
        :param dbname: the name of the database
        :param user: the username credential
        :param password: the password
        :param host: the host of the database
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def init(self):
        """
        Initiates the connection with the database 'dbname'
        :return: (conn, crsr) -> (the connection object, the cursor to perform commands)
        """
        conn = psycopg2.connect(dbname=self.dbname, user=self.user,
                                password=self.password, host=self.host)
        return conn, conn.cursor()

    def delete_table(self, table_name: str):
        """
        Deletes a table from the database
        :param table_name: the table_name
        """
        conn, crsr = self.init()

        crsr.execute(f"DROP TABLE {table_name};")
        conn.commit()
        crsr.close()
        conn.close()

    def create_table_users(self):
        """
        Creates the table users that we will use to migrate our data from output.csv to.
        """
        conn, crsr = self.init()

        crsr.execute("""
            CREATE TABLE IF NOT EXISTS users(
                id SERIAL PRIMARY KEY NOT NULL,
                user_id varchar (20) NOT NULL,
                first_name varchar (30) NOT NULL,
                last_name varchar (30) NOT NULL,
                birthdate varchar (30) NOT NULL,
                img_path varchar (250) NOT NULL
            );
        """)

        conn.commit()
        crsr.close()
        conn.close()

    def clear_table(self, table_name: str):
        conn, crsr = self.init()
        crsr.execute(f"TRUNCATE TABLE {table_name };")

        conn.commit()
        crsr.close()
        conn.close()

    def _insert_rows(self, table_name: str, users: list):
        """
        Uploads the given list of users into the 'users' table
        :param users: a list of the users to be uploaded to the 'users' table

        NOTE:
        the list users should contain all the information about the users right after each other
        without having each user in a separate list.
        For example: users = [user_id1, first_name1, last_name1, birthdate1, img_path1, user_id2, first_name2, .....]

        Required columns: (user_id, first_name, last_name, birthdate, img_path)
        """
        conn, crsr = self.init()

        values = "(" + "%s ," * 5 + ")"
        values = values[:-3] + values[-1] + ","
        values = values * (len(users) // 5)
        values = values[:-1]

        crsr.execute(
            f"INSERT INTO {table_name} (user_id, first_name, last_name, birthdate, img_path) VALUES {values}"
            , users)

        conn.commit()
        crsr.close()
        conn.close()

    def _get_ids(self, table_name: str):
        """
        Retrieves all the ids of the users inside the 'table_name' table
        :param table_name: the name of the table
        :return: a list of the ids of the users inside the table
        """
        conn, crsr = self.init()

        crsr.execute(f"SELECT user_id FROM {table_name};")
        ids = crsr.fetchall()

        conn.commit()
        conn.close()
        crsr.close()

        return [id[0] for id in ids]

    def delete_rows_by_ids(self, table_name: str, ids: list):
        """
        Deletes rows from the table table_name that has an id in 'ids'
        :param table_name: the name of the table
        :param ids: the ids of the rows we want to delete from the table
        """
        condition = f" user_id = %s OR" * len(ids)
        condition = condition[:-3]

        conn, crsr = self.init()

        crsr.execute(f"DELETE FROM {table_name} WHERE {condition}", ids)

        conn.commit()
        conn.close()
        crsr.close()

    def get_users_data(self, table_name: str):
        """
        Retrieves all the data in the table table_name
        :param table_name: the name of the table
        :return: a list that contains all the data in the table

        NOTE:
        the returned list will contain all the information about the users right after each other
        without having each user in a separate list.
        For example: returned_list = [user_id1, first_name1, last_name1, birthdate1, img_path1, user_id2, first_name2, .....]
        """
        conn, crsr = self.init()

        crsr.execute(f"SELECT user_id, first_name, last_name, birthdate, img_path FROM {table_name};")
        data = crsr.fetchall()

        conn.commit()
        crsr.close()
        conn.close()

        returned_list = []
        for row in data:
            returned_list.extend(row)
        return returned_list

    def insert_users(self, table_name: str, users: list):
        """
        Process users data before inserting them into the DB. Checks for already existing
        users and delete their old records. Then insert all rows as a batch into the DB.
        :param table_name: the name of the table to insert the data in
        :param users: a list of the users to be uploaded to the 'users' table

        NOTE:
        the list users should contain all the information about the users right after each other
        without having each user in a separate list.
        For example: users = [user_id1, first_name1, last_name1, birthdate1, img_path1, user_id2, first_name2, .....]
        """
        db_users_ids = self._get_ids(table_name)

        existing_users_ids = []
        for i in range(0, len(users), 5):
            if users[i] in db_users_ids:
                existing_users_ids.append(users[i])

        if len(existing_users_ids) != 0:
            self.delete_rows_by_ids(table_name, existing_users_ids)
        if len(users) != 0:
            self._insert_rows('users', users)

