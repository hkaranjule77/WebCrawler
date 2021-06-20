# global imports

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import bs4
import configparser
import os
import random
import re
import requests
import string
import threading
import time

# relative local imports
import db
import logger


class Lock:
    def __init__(self):
        self.is_locked = False

    def acquire(self):
        if self.is_locked:
            while self.is_locked:
                pass
        self.is_locked = True

    def release(self):
        self.is_locked = False


class WebCrawler:
    def __init__(self):
        """ Initializes configuration and starts crawls. """
        # TODO: create log of error links or when it's invalid also log it.
        # TODO: update files instead of creating new file.
        self.db_handler = db.CrawlerDBHandler()
        self.links = []
        self.logger = logger.Logger()
        self.save_limit_reached = False
        self.unvisited_links = []
        self.unrefreshed_links = []
        self.__read_config()
        self.__add_base_url()
        self.create_html_dir()
        self.timeout = self.config['request_timeout']

    def __add_base_url(self):
        """ Adds the starting url into database from configuration file. """
        if self.db_handler.row_count() == 0:
            link = self.config['base_url']
            self.db_handler.insert_unvisited(link, 'NA')

    def visit(self, link_row):
        """ Executes steps required for scraping. """
        # unvisited
        # print("Crawling:", link_row["link"])
        response = self.get_page(link_row["link"])
        if response is not False:
            if not self.save_limit_reached:
                self.add_new_links(response.text, response.url)
            if link_row['is_crawled']:
                file_path = WebCrawler.save_page(
                    self.config['html_page_dir'],
                    response.text,
                    file_path=link_row['file_path']
                )
            else:
                file_path = WebCrawler.save_page(self.config['html_page_dir'], response.text)
            self.update_row(link_row["id"], response, file_path)

    def crawl(self):
        """ Main method of a WebCrawler object. """
        self.fetch_links()
        print("Links to be visited:", len(self.unvisited_links), len(self.unrefreshed_links))
        # Multi-threading
        while True:
            threads = ThreadPoolExecutor(max_workers=self.config['parallel_thread_count'])
            threads.map(self.visit, self.unvisited_links)
            threads.map(self.visit, self.unrefreshed_links)
            threads.shutdown()
            while threading.active_count() != 1:
                print("threads", threading.active_count())
                time.sleep(10)
            self.fetch_links()
            if len(self.unrefreshed_links) == 0 and len(self.unvisited_links) == 0:
                print("All links crawled.")
                time.sleep(self.config['sleep_time'])
            '''
        # Single-threading
        while True:
            while len(self.unvisited_links) > 0:
                if len(self.unvisited_links) != 0:
                    link = self.unvisited_links.pop()
                elif len(self.unrefreshed_links) != 0:
                    link = self.unrefreshed_links.pop()
                else:
                    break
                self.visit(link)
            self.fetch_links()
            
            # self.threads.map(self.__new_visit, self.unvisited_links)
            
            if len(self.unvisited_links) == 0 and len(self.unrefreshed_links) == 0:
                print("All links crawled")
                time.sleep(5)
            '''

    def __read_config(self):
        """ Reads Configurations into a params dictionary. """
        self.config = {}
        config_parser = configparser.ConfigParser()
        config_parser.read("config.cfg")
        for k, v in config_parser.items("setting"):
            try:
                v = int(v)
            except ValueError:
                pass
            self.config.update({k: v})

    def __refresh_visit(self, link_row):
        """ Revisits links and updates database with new data. """
        # print("refresh:", link_row['link'])
        response = self.get_page(link_row["link"])
        if response is not False:
            self.add_new_links(response.text, response.url)

            self.update_row(link_row["id"], response, file_path)

    def add_new_links(self, html_text, html_url):
        """ Finds and adds new links into the database. """
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

    def create_html_dir(self):
        """ Creates directory for storing html files. """
        try:
            os.makedirs(self.config["html_page_dir"])
        except FileExistsError:
            pass

    def fetch_links(self):
        """ Updates unvisited  links firsts if those are done.
            Then unrefreshed links are updated. """
        self.unvisited_links = self.db_handler.get_uncrawled()
        refresh_after = int(self.config["link_refresh_after_hrs"])
        self.unrefreshed_links = self.db_handler.get_unrefreshed(datetime.now() - timedelta(hours=refresh_after))

    def get_page(self, link):
        """ Downloads a webpage from provided hyperlink. """
        try:
            response = requests.get(link, timeout=self.timeout, headers={"Accept-Encoding": None})
            print("visited:", link)
            return response
        except requests.exceptions.MissingSchema:
            print("MissingSchema", link)
            self.logger.log("missing_schema", link)
        except requests.exceptions.InvalidSchema:
            print("InvalidSchema", link)
            self.logger.log("invalid_schema", link)
        except requests.exceptions.ConnectionError:
            print("ConnectionError:", link)
            self.logger.log("connection_error", link)
        except requests.exceptions.TooManyRedirects:
            print("Too Many Redirects:", link)
            self.logger.log("too_many_redirect", link)
        except requests.exceptions.Timeout:
            print("Timeout:", link)
            self.logger.log("timeout.log", link)
        return False

    @staticmethod
    def get_valid_link(link, src_link):
        """ Checks if link is valid or not. """
        if link.startswith("/"):
            # converts relative to absolute
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
            log_obj = logger.Logger()
            log_obj.log('rejected', link)
            del log_obj
            return None

    @staticmethod
    def save_page(html_dir, page_text, file_path=None):
        """ It uses specified file_path or else generates random name to save a webpage. """
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
        """ Scrapes and returns list of links from provided webpage. """
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

    def update_row(self, row_id, response, file_path):
        """ Saves updated data of link in corresponding row of database. """
        resp_status = response.status_code
        content_type = response.headers['Content-Type']
        try:
            content_len = response.headers['Content-Length']
        except KeyError:
            content_len = len(response.content)
        self.db_handler.update_visit(id=row_id,
                                     resp_status=resp_status,
                                     content_type=content_type,
                                     content_len=content_len,
                                     file_path=file_path
                                     )