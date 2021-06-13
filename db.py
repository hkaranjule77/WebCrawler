import configparser
import time
from datetime import datetime, timedelta
from mysql import connector
from mysql.connector import errorcode


class CrawlerDBHandler:
    def __init__(self):
        """ Initializes CrawlerDBHandler with parameters and detects the connection is live. """
        self.config = {}
        self.connector = None
        self.cursor = None
        self.read_config()
        self.DB_NAME = self.config['db_name']
        self.TABLE_NAME = self.config['table_name']
        self.USERNAME = self.config['username']
        self.PASSWORD = self.config['password']
        self.HOST = self.config['host_type']
        self.connect()

    def close(self):
        """ Closes a connection with the database. """
        if self.connector is not None:
            self.connector.close()
        else:
            raise ValueError(" Connector value is not initialize. ")

    def connect(self):
        """
            Makes connection with MySQL Database server.
            If check is set to True,
                it detects by making connection and closes it.
            Else if check is set to False,
                it makes a connection and also initializes a cursor.
        """
        try:
            self.connector = connector.connect(
                pool_size=5,
                host=self.HOST,
                user=self.USERNAME,
                password=self.PASSWORD,
                database=self.DB_NAME,
                autocommit=True
            )
            self.cursor = self.connector.cursor(dictionary=True)
            print("Database Connected:", self.DB_NAME)
        except connector.Error as err:
            print(err.errno, end=": ")
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                # unknown database error
                print(f"DATABASE {self.DB_NAME} does not exist.")
                self.create_db()
            elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                # access denied user
                print("Database Username and/or Password is wrong.")
            elif err.errno == errorcode.ER_HOSTNAME:
                # unknown host
                print("Incorrect host name.")
            elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                print("Database server is down.")
            else:
                print("Unknown Error while connecting.")

    def create_db(self):
        """ Creates a New Database crawler if does not exist. """
        self.connector = connector.connect(
            user=self.USERNAME,
            password=self.PASSWORD
        )
        add_db = f"CREATE DATABASE IF NOT EXISTS {self.DB_NAME};"
        self.cursor = self.connector.cursor()
        self.cursor.execute(add_db)
        print("New Database created:", self.DB_NAME)
        self.connector.database = self.DB_NAME
        self.create_table()
        self.connector.close()

    def create_table(self):
        """ Creates Table links if does not exists. """
        add_table = f'''
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME}(
                id INT PRIMARY KEY AUTO_INCREMENT, 
                link VARCHAR(1023) NOT NULL UNIQUE, 
                src_link VARCHAR(1023) NOT NULL, 
                is_crawled TINYINT(1) NOT NULL DEFAULT 0, 
                last_crawl_dt DATETIME, 
                response_status VARCHAR(4), 
                content_type VARCHAR(255), 
                content_len INT, 
                file_path VARCHAR(1023) UNIQUE, 
                created_at DATETIME NOT NULL
            )
            CHARSET=latin1;
        '''
        self.connect()
        self.close()
        if self.execute(add_table):
            print("New Table created:", self.TABLE_NAME)
            self.connect()

    def execute(self, query, values=None, fetch=False):
        """ Executes query with cursor by creating connection.

            Parameters:
                values (list/tuple): Parameters in case of insert query.
                fetch (bool): Is this a select query?.

            Output:
                If fetch is set to true then
                    it returns a boolean value and list of experiments.
                Else then
                    it returns a boolean value based on success/failure.
            """
        if self.connector is not None:
            if self.cursor is not None:
                try:
                    if values is not None:
                        self.cursor.execute(query, values)
                    else:
                        self.cursor.execute(query)
                    if fetch:
                        return self.cursor.fetchall()
                    return True
                except connector.Error as err:
                    if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                        print("Table does not exist:", self.TABLE_NAME)
                        self.create_table()
                    elif err.errno == errorcode.ER_DUP_ENTRY:
                        # print("link is repeated")
                        pass
                    else:
                        print(err.errno, ":", sep="", end=" ")
                        print("While executing query")
            else:
                raise TypeError("Cursor is not initialized.")
        else:
            raise TypeError("Connector is not initialized.")
        if fetch:
            return False, []
        return False

    def get_unvisited(self):
        """ Retrieves unvisited links from the database. """
        get_unvisit = f"SELECT * FROM {self.TABLE_NAME} WHERE is_crawled=0;"
        result = self.execute(query=get_unvisit, fetch=True)
        return list(result)

    def get_unrefreshed(self, from_dt=datetime.now() - timedelta(days=1)):
        """ Fetchs unrefreshed data which aren't updated recently. """
        get_unrefreshed = f'''SELECT * FROM {self.TABLE_NAME} 
        WHERE last_crawl_dt > {from_dt.strftime("%Y-%-m-%-d %H:%M:%S")};'''
        result = self.execute(query=get_unrefreshed, fetch=True)
        return list(result)

    def insert_unvisited(self, link, src_link):
        """ Inserts unvisited links in database with current datetime. """
        insert_link = "INSERT INTO {}(link, src_link, created_at) VALUES(%s, %s, %s);".format(self.TABLE_NAME)
        new_link = (link, src_link, datetime.now())
        # print("Insert query:", insert_link, new_link)
        return self.execute(query=insert_link, values=new_link)

    def read_config(self):
        """ Reads parameters for database configuration. """
        config_parser = configparser.ConfigParser()
        config_parser.read("config.cfg")
        for k, v in config_parser.items(section="database"):
            self.config.update({k: v})

    def row_count(self):
        """ Returns row count of links table. """
        result = self.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME};", fetch=True)
        # print("row_count:", result)
        # indexes at end of statement [row_index][column name]
        print("row count result:", result)
        row = result[0]
        row_count = result[0]['COUNT(*)']
        return row_count

    def update_visit(self, id, resp_status, content_type, content_len, file_path):
        """ Updates a link if visited. """
        update_link = f'''
        UPDATE {self.TABLE_NAME} 
        SET is_crawled=%s,
            response_status=%s, 
            last_crawl_dt=%s,
            content_type=%s,
            content_len=%s,
            file_path=%s
        WHERE id=%s;
        '''
        visit_info = (True, resp_status, datetime.now(), content_type, content_len, file_path, id)
        self.execute(query=update_link, values=visit_info)
