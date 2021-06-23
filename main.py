# Local Imports
from crawler import WebCrawler
from thread_manager import ThreadManager


# to carry out some initialization tasks
crawler = WebCrawler()
crawler.setup()
del crawler

# Multi-threading crawling
crawl_threads = ThreadManager()
crawl_threads.execute()
