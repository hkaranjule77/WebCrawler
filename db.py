from datetime import datetime, timedelta
from mysql import connector
from mysql.connector import errorcode


class CrawlerDBHandler:
    def __init__(self, db_name, table_name, usrname, pswd, host):
        ''' Initializes CrawlerDBHandler with parameters and detects the connection is live. '''
        self.DB_NAME = db_name
        self.TABLE_NAME = table_name
        self.USERNAME = usrname
        self.PASSWORD = pswd
        self.HOST = host
        self.connect()
        self.create_table()


    def close(self):
        ''' Closes a connection with the database. '''
        if self.connector:
            self.connector.close()
        else:
            raise ValueError(" Connector value is not initialize. ")
        
        
    def connect(self):
        ''' 
            Makes connection with MySQL Database server.
            If check is set to True,
                it detects by making connection and closes it.
            Else if check is set to False,
                it makes a connection and also initializes a cursor. 
        '''
        try:
            self.connector = connector.connect(
                host=self.HOST,
                user=self.USERNAME,
                password=self.PASSWORD,
                database=self.DB_NAME
            )
            self.cursor = self.connector.cursor()
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
        ''' Creates a New Database crawler if does not exist. '''
        self.connector = connector.connect(
            user = self.USERNAME,
            password = self.PASSWORD
        )
        add_db = f"CREATE DATABASE IF NOT EXISTS {self.DB_NAME};"
        self.cursor = self.connector.cursor()
        self.cursor.execute(add_db)
        print("New Database created:", self.DB_NAME)
        self.connector.database = self.DB_NAME
        self.create_table()
        self.connector.close()


    def create_table(self):
        ''' Creates Table links if does not exists. '''
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
        if self.execute(add_table):
            print("New Table created:", self.TABLE_NAME)
        

    def execute(self, query, params=None, fetch=False):
        ''' Executes query with cursor by creating connection. 

            Parameters:
                params (list/tuple): Parameters in case of insert query.
                fetch (bool): Is this a select query?.
            
            Output:
                If fetch is set to true then
                    it returns a boolean value and list of experiments.
                Else then
                    it returns a boolean value based on success/failure.
            '''
        if self.connector:
            if self.cursor:
                try:
                    if params:
                        self.cursor.execute(query, params)
                    else:
                        self.cursor.execute(query)
                    if fetch:
                        return self.cursor.fetchall()
                    self.connector.close()
                    return True
                except connector.Error as err:
                    print(err.errno, ":", sep="")
                    if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                        print("Table does not exist:", self.TABLE_NAME)
                        self.create_table()
                    else:
                        print("While executing query")
            else:
                raise TypeError("Cursor is not initialized.")
        else:
            raise TypeError("Connector is not initialized.")
        if fetch:
            return False, []
        return False


    def get_unvisited(self):
        ''' Retrieves unvisited links from the database. '''
        get_unvisit = f"SELECT * FROM {self.TABLE_NAME} WHERE is_crawled=0;"
        success, result = self.execute(query=get_unvisit, fetch=True)
        if success:
            return result

    
    def get_unrefreshed(self, from=datetime.now()-timedelta(days=1)):
        get_unrefresh = f'''SELECT * FROM {self.TABLE_NAME} 
        WHERE last_crawl_dt > {from.strftime("%Y-%-m-%-d %H:%M:%S")};'''
        success, result = self.execute(query=get_unrefresh, fetch=True)
        if success:
            return result


    def insert_unvisited(self, link, src_link):
        ''' Inserts unvisited links in database with current datetime. '''
        insert_link = f"INSERT INTO {self.TABLE_NAME}(link, src_link, created_at) VALUES('%s', '%s', '%s');"
        new_link = (link, src_link, datetime.now())
        return self.execute(query=insert_link, params=new_link)


    def update_visit(self, id, resp_status, content_type, content_len, file_path):
        ''' Updates a link if visited. '''
        update_link = f'''
        UPDATE {self.TABLE_NAME} 
        SET response_status=%s, 
            last_crawl_at=%s,
            content_type=%s,
            content_len=%s,
            file_path=%s
        WHERE id=%s;
        '''
        visit_info = (resp_status, datetime.now(), content_type, content_len, file_path)
        self.execute(query=update_link, params=visit_info)