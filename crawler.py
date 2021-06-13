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
import time

# relative local imports
import db


def save_page(html_dir, page_text, file_name=None):
    """ Generates random name and saves webpage. """
    while True:
        try:
            file_name = ''.join(random.choices(string.ascii_letters + string.digits, k=12))
            file_path = os.path.join(html_dir, file_name)
            if not os.path.exists(file_path):
                html_page = open(file_path, 'w')
                html_page.write(page_text)
                html_page.close()
                return file_path
        except FileExistsError:
            continue


class WebCrawler:
    def __init__(self):
        """ Initializes configuration and starts crawls. """
        # TODO: create log of error links or when it's invalid also log it.
        # TODO: update files instead of creating new file.
        self.links = []
        self.unvisited_links = []
        self.unrefreshed_links = []
        self.db_handler = db.CrawlerDBHandler()
        self.save_limit_reached = False

        self.__read_config()
        self.__add_base_url()
        self.create_html_dir()
        self.__main()

    def __add_base_url(self):
        if self.db_handler.row_count() == 0:
            link = self.config['base_url']
            self.db_handler.insert_unvisited(link, 'NA')

    def __crawl(self):
        """ Executes steps required for scraping. """
        if not self.save_limit_reached:
            # unvisited
            if len(self.unvisited_links) > 0:
                link_row = self.unvisited_links.pop(0)
        else:
            # refresh
            if len(self.unrefreshed_links) > 0:
                link_row = self.unrefreshed_links.pop(0)
            else:
                return
        print("Crawling:", link_row["link"])
        response = self.get_page(link_row["link"])
        if response:
            self.add_new_links(response.text, response.url)
            self.save(link_row["id"], response)

    def __main(self):
        """ Main method of a WebCrawler object. """
        while True:
            self.update_links()
            '''
            # Multi-threading
            threadpool = ThreadPoolExecutor(max_workers=5)
            threadpool.map(self.__crawl)
            '''
            # Single-threading
            print("Links to be visited:", len(self.unvisited_links), len(self.unrefreshed_links))
            # print(self.unvisited_links)
            while (len(self.unvisited_links) > 0 and not self.save_limit_reached) or len(self.unrefreshed_links) > 0:
                self.__crawl()
            print("All links crawled")
            time.sleep(5)

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

    def add_new_links(self, html_text, html_url):
        """ Finds and adds new links into the database. """
        print("Insert query:", html_url)
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

    def get_page(self, link):
        """ Downloads a webpage from provided hyperlink. """
        timeout = self.config['request_timeout']
        try:
            response = requests.get(link, timeout=timeout, headers={"Accept-Encoding": None})
            print("visited:", link)
            return response
        except requests.exceptions.MissingSchema:
            print("MissingSchema", link)
            WebCrawler.log_link(link, "missing_schema.log")
        except requests.exceptions.InvalidSchema:
            print("InvalidSchema", link)
            WebCrawler.log_link(link, "invalid_schema.log")
        except requests.exceptions.ConnectionError:
            print("ConnectionError:", link)
            WebCrawler.log_link(link, "connection_error.log")
        except requests.exceptions.TooManyRedirects:
            print("Too Many Redirects:", link)
            WebCrawler.log_link(link, "too_many_redirect.log")
        except requests.exceptions.Timeout:
            print("Timeout:", link)
            WebCrawler.log_link(link, "timeout.log")
        return False

    @staticmethod
    def get_valid_link(link, src_link):
        """ Checks if link is valid or not. """
        if link.startswith("/"):
            # converts relative to absoulte
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
        elif has_domain:
            return "https://" + link
        else:
            return None

    @staticmethod
    def log_link(link, filename):
        """ Logs hyperlink, crawling time into file of specified filename. """
        try:
            file = open(filename, "a")
        except FileExistsError:
            file = open(filename, "w")
        file.write(link + "\t" + str(datetime.now()) + "\n")
        file.close()

    def save(self, row_id, response):
        """ Saves webpage and updates visited data on database. """
        page_path = save_page(self.config['html_page_dir'], response.text)
        resp_status = response.status_code
        content_type = response.headers['Content-Type']
        try:
            content_len = response.headers['Content-Length']
        except KeyError:
            content_len = len(response.content)
        file_path = page_path
        self.db_handler.update_visit(id=row_id,
                                     resp_status=resp_status,
                                     content_type=content_type,
                                     content_len=content_len,
                                     file_path=file_path
                                     )

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

    def update_links(self):
        """ Updates unvisited  links firsts if those are done.
            Then unrefreshed links are updated. """
        self.unvisited_links = self.db_handler.get_unvisited()

        if len(self.unvisited_links) == 0:
            refresh_after = int(self.config["link_refresh_in_hrs"])
            self.unrefreshed_links = self.db_handler.get_unrefreshed(
                datetime.now() - timedelta(hours=refresh_after)
            )
