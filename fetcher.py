import configparser
from datetime import datetime, timedelta
from threading import Lock


from db import CrawlerDBHandler


class LinkFetcher:
    """
    It works as fetcher and distributor for links among various crawling threads.
    """
    def __init__(self):
        """ Initializer for LinkFetcher object. """
        self.config = LinkFetcher.read_config()
        self.refresh_after = self.config['link_refresh_after_hrs']
        self.links = list()
        self.lock = Lock()
        self.db_handler = CrawlerDBHandler()
        del self.config

    def count(self):
        """ Returns the count of links currently available in list. """
        return len(self.links)

    def pop(self):
        """
        Returns a link tuple at 0th index.
        NOTE: Don't forget to call release_lock method after calling this method as it acquires lock. If lock is not
        released then execution may can end up in racing state.
        """
        self.lock.acquire(timeout=5)
        if len(self.links) == 0:
            return None
        return self.links.pop()

    @staticmethod
    def read_config():
        """
        staticmethod: Reads configuration required for link fetcher and return it in form of list.

        Returns:
              config(dict): A configuration name as key and it's values.
        """
        config = dict()
        config_parser = configparser.ConfigParser()
        config_parser.read('config.cfg')
        for key, val in config_parser.items("fetcher"):
            try:
                config.update({key: int(val)})
            except ValueError:
                config.update({key: val})
        return config

    def refresh(self):
        """ Refreshes or updates new links in the list from database. """
        unvisited = self.db_handler.get_unvisited()
        self.links.extend(unvisited)
        unrefreshed = self.db_handler.get_unrefreshed(
            from_dt=datetime.now() - timedelta(hours=self.refresh_after)
        )
        self.links.extend(unrefreshed)

    def release_lock(self):
        """
        This method must be called after pop method to release the acquired lock. Else, execution may get stand still
        (single-thread) or in racing condition(multi-thread).
        """
        self.lock.release()
