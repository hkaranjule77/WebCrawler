import threading
import configparser
from crawler import WebCrawler


class ThreadManager:
    def __init__(self):
        self.thread_count = get_thread_count()
        self.crawler_list = []
        self.thread_list = []

    def execute(self):
        for count in range(self.thread_count):
            crawler = WebCrawler()
            threading.Thread(target=crawler.crawl)
        '''
        while True:
            thread_list = threading.Thread(target=)
            if threading.active_count() < self.thread_count:
                pass
        '''

    def get_thread_count():
        config_parser = configparser.ConfigParser()
        config_parser.read("config.cfg")
        for key, val in config_parser.items("setting"):
            if key == 'parallel_thread_count':
                return val
        raise KeyError
