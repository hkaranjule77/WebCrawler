# global imports
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
from . import db

'''
def log(linkset, filename):
    file = open(filename, "w")
    for link in linkset:
        file.write(link+"\n")
    file.close()


cfgparser = configparser.ConfigParser()
cfgparser.read("config.cfg")

parameters = {}
for sect in cfgparser.sections():
    for k, v in cfgparser.items(sect):
        parameters.update({k: v})

#print(parameters)
unvisited_links = set()
visited_links = set()
link = parameters["base_url"]
invalid_links = set()
while True:
    if len(visited_links)<5000:
        if link not in visited_links:
            try:
                response = requests.get(link)
            except requests.exceptions.MissingSchema:
                print("MissingSchema", link, response.status_code, "visited:", len(visited_links))
                log_link(link,"missing_schema.log", response.status_code)
                link = unvisited_links.pop()
            except requests.exceptions.InvalidSchema:
                print("InvalidSchema", link, response.status_code, "visited:", len(visited_links))
                log_link(link,"invalid_schema.log", response.status_code)
                invalid_links.add(link)
                link = unvisited_links.pop()
            except requests.exceptions.ConnectionError:
                print("ConnectionError", link, response.status_code, "visited:", len(visited_links))
                log_link(link,"connection_error.log", response.status_code)
            except requests.exceptions.TooManyRedirects:
                print("too many redirects")
                log_link(link, "conn")
            except requests.exceptions.Timeout:
                print("")
            soup = bs4.BeautifulSoup(response.text, "html.parser")
            for a_tag in soup.find_all("a"):
                try:
                    tmp_link = requests.compat.urljoin(response.url, a_tag["href"])
                    if tmp_link:
                        unvisited_links.add(tmp_link)
                    else:
                        invalid_links.add(tmp_link)
                except KeyError:
                    continue
            #print(response.text)
            print(len(visited_links)+1, response.url, response.status_code)
            visited_links.add(link)
        link = unvisited_links.pop().strip()
    else:
        break

log(visited_links, "visited.log")
log(invalid_links, "invalid.log")
log(unvisited_links, "unvisited.log")
'''

class WebCrawler:
    def __init__(self):
        ''' Initializes configuration and starts crawls. '''
        self.read_config()
        self.db_handler = db.CrawlerDBHandler()
        self.create_html_dir()
        self.save_limit_reached = True
        self.__crawl()


    def __crawl(self):
        ''' Executes steps required for scraping. '''
        while True:
            if not self.save_limit_reached:
                # unvisited
                if len(self.unvisited_links)>0:
                    links = self.unvisited_links.pop(0)
                else:
                    self.update_links()
            else:
                # refresh
                if len(self.unrefreshed_links) > 0:
                    link = self.unrefreshed_links.pop(0)
                else:
                    self.update_links()

            response = WebCrawler.get_page(links["link"])
            if response:
                    self.add_new_links(response.text, response.url)
                    self.save(links["id"], response)


    def add_new_links(self, html_text, html_url):
        ''' Finds and adds new links into the database. '''
        new_links = WebCrawler.scrape(html_text, html_url)
        for new_link in new_links:
            row_count = self.db_handler.row_count()
            if row_count < self.config["max_link_limit"]:
                self.db_handler.insert_unvisited(
                    link=new_link, src_link=html_url
                )
            else:
                self.save_limit_reached = True
                break


    def create_html_dir(self):
        ''' Creates directory for storing html files. '''
        try:
            os.makedirs(self.config["html_page_dir"])
        except FileExistsError:
            pass


    @staticmethod
    def get_page(link):
        ''' Downloads a webpage from provided hyperlink. '''
        try:
            response = requests.get(link, headers={"Accept-Encoding": None})
            return response
        except requests.exceptions.MissingSchema:
            print("MissingSchema", link)
            WebCrawler.log_link(link,"missing_schema.log")
        except requests.exceptions.InvalidSchema:
            print("InvalidSchema", link)
            WebCrawler.log_link(link,"invalid_schema.log")
        except requests.exceptions.ConnectionError:
            print("ConnectionError:", link)
            WebCrawler.log_link(link,"connection_error.log")
        except requests.exceptions.TooManyRedirects:
            print("Too Many Redirects:", link)
            WebCrawler.log_link(link, "too_many_redirect.log")
        except requests.exceptions.Timeout:
            print("Timeout:", link)
            WebCrawler.log_link(link, "timeout.log")
        return False


    @staticmethod
    def is_valid(link):
        ''' Checks if link is valid or not. '''
        has_proto = False
        protocols = ["https", "http"]
        for proto in protocols:
            if link.startswith(proto):
                has_proto = True
        
        has_domain = re.search(pattern=r"[\w]\.[\w]",string=link )

        if has_proto and has_domain:
            return link
        elif has_domain:
            return "https://"+link
        else:
            return None


    @staticmethod
    def log_link(link, filename):
        ''' Logs hyperlink, crawling time into file of specified filename. ''' 
        try:
            file = open(filename, "a")
        except FileExistsError:
            file = open(filename, "w")
        file.write(link + "\t" + str(datetime.now()) + "\n" )
        file.close()


    def read_config(self):
        ''' Reads Configurations into a params dictionary. '''
        self.config = {}
        config_parser = configparser.ConfigParser()
        config_parser.read("config.cfg")
        for k, v in config_parser.items("database"):
            self.config.update({k: v})


    def save(self, row_id, response):
        ''' Saves webpage and updates visited data on database. '''
        page_path = self.save_page(self.config['html_page_dir'], response.text)
        self.db_handler.update_visit(id=row_id, 
            resp_status=response.status_code,
            content_type=response.header['Content-Type'],
            content_len=response.header['Content-Length'],
            file_path=page_path
        )
  

    def save_page(self, html_dir, page_text):
        ''' Generates random name and saves webpage. '''
        while True:
            try:
                file_name = ''.join(random.choice(string.ascii_letters + string.digits, k=12))
                file_path = os.path.join(html_dir, file_name)
                if not os.path.exists(file_path):
                    html_page = open(file_path)
                    html_page.write(page_text)
                    html_page.close()
                    return file_path
            except FileExistsError:
                continue


    @staticmethod
    def scrape(webpage, visited_link):
        ''' Scrapes and returns list of links from provided webpage. '''
        links = []
        soup = bs4.BeautifulSoup(webpage, "html.parser")
        for a_tag in soup.find_all("a"):
            # relative url
            if a_tag["href"].startswith("/"):
                # converts relative to absoulte
                link = requests.compat.url_join(visited_link, a_tag["href"])
                links.append(link)
            elif WebCrawler.is_valid(a_tag["href"]):
                links.append(link)

    def update_links(self):
        ''' Updates unvisited  links firsts if those are done.
            Then unrefreshed links are updated. '''
        self.unvisited_links = self.db_handler.get_unvisited()
        if len(self.unvisited_links) == 0:
            self.save_limit_reached = True
            self.refresh_after = int(self.config["link_refersh_in_hrs"])
            self.unrefreshed_links = self.db_handler.get_unrefreshed(
                datetime.now() - timedelta(hours=self.refresh_after)
            )