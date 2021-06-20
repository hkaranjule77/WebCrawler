from threading import Lock
from .db import CrawlerDBHandler


class LinkFetcher:
    def __init__(self):
        self.links = list()
        self.lock = Lock()
        self.db_handler = CrawlerDBHandler()

    def pop(self):
        """
        Returns a link tuple at 0th index.
        NOTE: Don't forget to call release_lock method after calling this method as it acquires lock. If lock is not
        released then execution may can end up in racing state.
        """
        self.lock.acquire(timeout=5)
        if len(self.links) == 0:
            unvisited = self.db_handler.get_unvisited()
            self.links.extend(unvisited)
            unrefreshed = self.db_handler.get_unrefreshed()
            self.links.extend(unrefreshed)
            if len(self.links) == 0:
                time.sleep()
        return self.links.pop()

    def release_lock(self):
        """
        This method must be called after pop method to release the acquired lock. Else, execution may get stand still
        (single-thread) or in racing condition(multi-thread).
        """
        self.lock.release()
