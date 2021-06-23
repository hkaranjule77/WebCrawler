import configparser

from datetime import datetime, timedelta
from mysql import connector
from mysql.connector import errorcode
from threading import Lock


class CrawlerDBHandler:
    def __init__(self):
        """
        Initializes CrawlerDBHandler with parameters and detects the connection is live.
        """
        # Database configuration values
        self.config = self.__class__.read_config()
        self.DB_NAME = self.config['db_name']
        self.TABLE_NAME = self.config['table_name']
        self.USERNAME = self.config['username']
        self.PASSWORD = self.config['password']
        self.HOST = self.config['host_type']
        self.MAX_ROW_LIMIT = self.config['max_row_read']
        del self.config
        # connectors
        self.connector = None
        self.cursor = None
        # thread synchronization with lock
        self.lock = Lock()
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
            If check is set to True,con_count=10
                it detects by making connection and closes it.
            Else if check is set to False,
                it makes a connection and also initializes a cursor.
        """
        try:
            self.connector = connector.connect(
                host=self.HOST,
                user=self.USERNAME,
                password=self.PASSWORD,
                database=self.DB_NAME,
                autocommit=True
            )
            self.cursor = self.connector.cursor(dictionary=True)
        except connector.Error as err:
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
                print("Please start/reboot database server and then restart the program.")
                exit()
            else:
                print("DB ERROR", err.errno, end=": ")
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
                is_crawled TINYINT NOT NULL DEFAULT 0, 
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
        if self.execute(add_table):
            print("New Table created:", self.TABLE_NAME)

    def execute(self, query, values=None, fetch=False):
        """ Executes query with cursor by creating connection.

            Parameters:
                query (str): SQL query for execution.
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
                        self.lock.acquire()
                        self.cursor.execute(query, values)
                        self.lock.release()
                        return True
                    elif fetch:
                        self.lock.acquire()
                        self.cursor.execute(query)
                        result = self.cursor.fetchall()
                        self.lock.release()
                        return result
                    else:
                        self.lock.acquire()
                        self.cursor.execute(query)
                        self.lock.release()
                        return True
                except connector.Error as err:
                    self.lock.release()
                    if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                        print("Table does not exist:", self.TABLE_NAME)
                        self.create_table()
                    elif err.errno == errorcode.ER_DUP_ENTRY:
                        # print("link is repeated")
                        pass
                    elif err.errno == errorcode.ER_NO_SUCH_TABLE:               # error code 1146
                        self.create_table()
                    elif err.errno == errorcode.CR_SERVER_LOST:
                        self.connect()
                        if values is not None:
                            self.execute(query, values=values)
                        elif fetch:
                            self.execute(query, fetch=True)
                            return self.cursor.fetchall()
                        else:
                            self.execute(query)
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
        get_unvisit = f"SELECT * FROM {self.TABLE_NAME} WHERE is_crawled=0 LIMIT {self.MAX_ROW_LIMIT};"
        result = self.execute(query=get_unvisit, fetch=True)
        return list(result)

    def get_unrefreshed(self, from_dt=datetime.now() - timedelta(days=1)):
        """
        Fetches unrefreshed data which aren't updated recently.

        Parameters:
             from_dt(datetime): Links which have last_crawled later than datetime of "from_dt" parameter.
                                By default, it's set for one day before the time of execution.
        """
        get_unrefreshed = f'''SELECT * FROM {self.TABLE_NAME} 
        WHERE last_crawl_dt <= "{from_dt.strftime("%Y-%-m-%-d %H:%M:%S")}" 
        LIMIT {self.MAX_ROW_LIMIT};'''
        result = self.execute(query=get_unrefreshed, fetch=True)
        return list(result)

    def insert_unvisited(self, link, src_link):
        """ Inserts unvisited links in database with current datetime. """
        insert_link = "INSERT INTO {}(link, src_link, created_at) VALUES(%s, %s, %s);".format(self.TABLE_NAME)
        new_link = (link, src_link, datetime.now())
        # print("Insert query:", insert_link, new_link)
        return self.execute(query=insert_link, values=new_link)

    def print_connect(self):
        """ Prints connect message. """
        if self.connector is not None:
            print("Database connected:", self.DB_NAME)
        else:
            print("h")

    @staticmethod
    def read_config():
        """
        Reads parameters for database configuration.

        Returns:
            config(dict): A configurations name as key and it's value.
        """
        config_parser = configparser.ConfigParser()
        config_parser.read("config.cfg")
        config = {}
        for k, v in config_parser.items(section="database"):
            config.update({k: v})
        return config

    def row_count(self):
        """
        Returns row count of links table.

        Returns:
            row_count(int): Number of row in database table of hyperlinks.
        """
        result = self.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME};", fetch=True)
        # print("row_count:", result)
        if type(result[0]) == bool:
            return 0
        # indexes at end of statement [row_index][column name]
        row = result[0]
        row_count = row['COUNT(*)']
        return row_count

    def update_visit(self, row_id, resp_status, content_type=None, content_len=None, file_path=None):
        """
        Updates values of database for a visited link.

        Parameters:
             row_id(int):           Primary key of a hyperlink row for which values are to be updated in the database.
             resp_status(int):  Value of response for a visited link Eg. 200 for Successful visit.
             content_type(str): Value of Content Type mentioned in the response.
             content_len(int):  Number of bytes of data present in the webpage response.
             file_path(str):    File Path where webpage is stored on the machine/server.
        """
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
        visit_info = (True, resp_status, datetime.now(), content_type, content_len, file_path, row_id)
        self.execute(query=update_link, values=visit_info)
