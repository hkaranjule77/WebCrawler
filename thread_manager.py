# standard python package
import configparser
import threading
import time

# local packages
from crawler import WebCrawler
from fetcher import LinkFetcher


class ThreadManager:
    def __init__(self):
        """ Constructor for Thread Manager of Web Crawler. """
        self.config = ThreadManager.__read_config()
        self.crawler_list = list()
        self.fetcher = LinkFetcher()
        self.thread_count = self.config["parallel_thread_count"] + 1             # extra thread is main thread
        self.sleep_interval = self.config["sleep_interval"]

    @staticmethod
    def __read_config():
        """ Reads Configuration values from file. """
        config = dict()
        config_parser = configparser.ConfigParser()
        config_parser.read("config.cfg")
        for key, val in config_parser.items('manager'):
            try:
                config.update({key: int(val)})
            except ValueError:
                config.update({key: val})
        return config

    def execute(self):
        """ Main method for execution of thread management. """
        for count in range(self.thread_count):
            crawler = WebCrawler()
            self.crawler_list.append(crawler)
        # print(len(self.crawler_list))
        curr_index = 0                                              # index for crawler objects
        while True:
            while threading.active_count() < self.thread_count and self.fetcher.count() != 0:
                crawler = self.crawler_list[curr_index]             # selects one of the crawler
                link = self.fetcher.pop()                           # takes one link from fetcher
                self.fetcher.release_lock()                         # release lock of fetcher
                if link is not None:
                    # print(threading.active_count(), link['id'], link['link'])
                    thread = threading.Thread(target=crawler.visit, kwargs=link)
                    thread.start()
                    curr_index += 1                                 # for selection of next crawler
                    if curr_index >= self.thread_count:
                        curr_index = 0
            if threading.active_count() == self.thread_count:
                time.sleep(0.005)
            if self.fetcher.count() == 0:
                # to stop repeated visit of same link by many threads
                while threading.active_count() != 1:    # waits until all links popped by fetcher are visited
                    time.sleep(1)                       # by all threads to avoid repeated visit of same link.
                # after execution of previous link is done, fetch new links from database into fetcheer
                self.fetcher.refresh()
                if self.fetcher.count() == 0:
                    print("All links crawled.")
                    time.sleep(self.sleep_interval)
