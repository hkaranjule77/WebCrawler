# global imports
import bs4
import configparser
import os
import random
import re
import requests
import string
import time

# relative local imports
import db


class WebCrawler:
    def __init__(self):
        """ Initializes configuration of Web Crawler. """
        self.config = self.__class__.__read_config()
        self.db_handler = db.CrawlerDBHandler()
        self.save_limit_reached = False
        self.timeout = self.config['request_timeout']

    def __add_base_url(self):
        """ Adds the starting url into database from configuration file. """
        if self.db_handler.row_count() == 0:
            link = self.config['base_url']
            self.db_handler.insert_unvisited(link, 'NA')

    def __create_html_dir(self):
        """ Creates directory for storing html files. """
        try:
            os.makedirs(self.config["html_dir_name"])
        except FileExistsError:
            pass

    @staticmethod
    def __read_config():
        """ Reads Configurations parameters into a dictionary. """
        config = {}
        config_parser = configparser.ConfigParser()
        config_parser.read("config.cfg")
        for k, v in config_parser.items("setting"):
            try:
                v = int(v)
            except ValueError:
                pass
            config.update({k: v})
        return config

    def add_new_links(self, html_text, html_url):
        """
        Finds new links and adds them into the database.

        Parameters:
            html_text(str): text content of html page.
            html_url(str): visited url from where html page was downloaded.

        Returns:
            None
        """
        # print("Insert query:", html_url)
        if not self.save_limit_reached:
            new_links = WebCrawler.scrape(html_text, html_url)
            for new_link in new_links:
                row_count = self.db_handler.row_count()
                if row_count < self.config["max_link_limit"]:
                    self.db_handler.insert_unvisited(
                        link=new_link, src_link=html_url
                    )
                else:
                    self.save_limit_reached = True
                    print("Maximum limit reached")
                    break

    def get_page(self, link_row):
        """
        Downloads a html webpage from provided hyperlink.

        Parameters:
            link_row(dict): a row from table links with column name as keys.

        Returns:
            response(requests.models.Response): If successfully downloads a webpage
            None: If any failure occurs during download.
        """
        try:
            link = link_row['link']
        except ValueError:
            return False
        try:
            response = requests.get(link, timeout=self.timeout, headers={"Accept-Encoding": None})
            print("visited:", link)
            return response
        except requests.exceptions.MissingSchema:
            # print("MissingSchema", link)
            self.db_handler.update_visit(row_id=link_row['id'], resp_status=404)
        except requests.exceptions.InvalidSchema:
            # print("InvalidSchema", link)
            self.db_handler.update_visit(row_id=link_row['id'], resp_status=404)
        except requests.exceptions.ConnectionError:
            # print("ConnectionError:", link)
            time.sleep(10)                                 # to avoid infinite looping in case of network failure
            self.db_handler.update_visit(row_id=link_row['id'], resp_status=502)
        except requests.exceptions.TooManyRedirects:
            # print("Too Many Redirects:", link)
            self.db_handler.update_visit(row_id=link_row['id'], resp_status=502)
        except requests.exceptions.Timeout:
            # print("Timeout:", link)
            self.db_handler.update_visit(row_id=link_row['id'], resp_status=408)
        return None

    @staticmethod
    def get_valid_link(link, src_link):
        """
        Checks if link is valid or not.

        Parameters:
            link(str): a unvalidated link.
            src_link(str): a hyperlink of webpage from which above link was extracted.

        Returns:
            link(str): a validated/corrected link
        """
        if link.startswith("/") or link.startswith("./") or link.startswith("../"):
            # converts relative to absolute link
            link = requests.compat.urljoin(src_link, link)
            return link
        has_proto = False
        protocols = ["https://", "http://"]
        for proto in protocols:
            if link.startswith(proto):
                has_proto = True
        has_domain = re.search(pattern=r"[\w]\.[\w]", string=link)
        if has_proto and has_domain:
            return link
        else:
            return None

    def setup(self):
        """ It does one time work of setup if not done. """
        # initialization methods
        self.__add_base_url()
        self.__create_html_dir()
        self.db_handler.print_connect()

    @staticmethod
    def save_page(html_dir, page_text, file_path=None):
        """
        It uses specified file_path or else generates random name to save a webpage.

        Parameters:
            html_dir(str): A path or name of directory where html page should be stored.
            page_text(str): A text content of html webpage.
            file_path(str): A file path if a html file is already stored for the currently visited link.

        Returns:
            file_path(str): A file path where html content is stored. For storing it in the database.
        """
        # generates random name
        if file_path is None:
            while True:
                file_name = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
                file_path = os.path.join(html_dir, file_name)
                if not os.path.exists(file_path):
                    break
        # saves a webpage
        html_page = open(file_path, 'w')
        html_page.write(page_text)
        html_page.close()
        return file_path

    @staticmethod
    def scrape(webpage, src_link):
        """
        Scrapes list of valid links from provided webpage and returns it.

        Parameters:
            webpage(str): A text content of downloaded html page.
            src_link(str): A text of hyperlink from which a webpage is download.

        Returns:
            links(list): A list of valid scraped links from the passed webpage.
        """
        links = []
        soup = bs4.BeautifulSoup(webpage, "html.parser")
        for a_tag in soup.find_all("a"):
            # relative
            try:
                link = a_tag["href"]
            except KeyError:
                continue
            link = WebCrawler.get_valid_link(link, src_link)
            if link is not None:
                links.append(link)
        return links

    def update_row(self, row_id, response, file_path=None):
        """
        Saves updated data of link in corresponding row of database.

        Parameters:
            row_id(int): A primary key of the row at which links is stored in the database table.
            response(requests.models.Response): A GET Response value of the URL visited by requests.
            file_path(str): A path of the file where currently downloaded webpage is stored.

        Returns:
             None
        """
        if response.status_code == 200:
            content_type = response.headers['Content-Type']
            try:
                content_len = response.headers['Content-Length']
            except KeyError:
                content_len = len(response.content)
            self.db_handler.update_visit(row_id=row_id,
                                         resp_status=response.status_code,
                                         content_type=content_type,
                                         content_len=content_len,
                                         file_path=file_path)
        else:
            self.db_handler.update_visit(row_id=row_id,
                                         resp_status=response.status_code)

    def visit(self, **link_row):
        """
        Performs each and every steps required for scraping and crawling. This method is made such that it needs to be
        separately called for each hyperlink.

        Parameters:
            link_row(kwargs): A Dictionary of row value from a database table of hyperlinks.

        Returns:
            None
        """
        # print("Crawling:", link_row["link"])
        response = self.get_page(link_row)
        if response is not None and response.status_code == 200:
            if not self.save_limit_reached:
                self.add_new_links(response.text, response.url)
            if link_row['is_crawled']:
                try:
                    file_path = WebCrawler.save_page(
                        self.config['html_page_dir'],
                        response.text,
                        file_path=link_row['file_path']
                    )
                except KeyError:
                    file_path = WebCrawler.save_page(self.config['html_dir_name'], response.text)
            else:
                file_path = WebCrawler.save_page(self.config['html_dir_name'], response.text)
            self.update_row(link_row["id"], response, file_path)
        elif response is not None:
            self.db_handler.update_visit(link_row["id"], resp_status=response.status_code)
        else:
            self.db_handler.update_visit(link_row["id"], resp_status=204)
