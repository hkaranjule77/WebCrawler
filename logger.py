import os

from datetime import datetime, time


class Logger:
    def __init__(self):
        """ Initializer for Logger"""
        self.LOG_DIR = 'logs'
        self.__create_dir()
        # for frequency calculation
        self.start_interval = time()
        self.link_count = 0
        self.is_block = False

    def __create_dir(self):
        """ Creates 'log' directory if not present. """
        if not os.path.exists(self.LOG_DIR):
            os.mkdir(self.LOG_DIR)

    def log(self, log_type, link):
        if self.is_blocked() or link.strip() == '':
            return
        if link is not None:
            if log_type is not None:
                log_file = os.path.join(self.LOG_DIR, log_type + '.log')
            else:
                log_file = os.path.join(self.LOG_DIR, 'unknown.log')
            try:
                log_file = open(log_file, 'a')
            except FileNotFoundError:
                log_file = open(log_file, 'w')
            log_file.write(str(datetime.now()) + "\t" + link + "\n")
            log_file.close()
        else:
            raise ValueError('link value is not passed.')

    def is_blocked(self):
        """ Checks frequency of logging if it's really high then it stops logging temporarily. """
        self.link_count += 1
        freq = self.link_count / (time() - self.start_interval)
        if freq > 5.0:
            self.is_block = True
            return True
        else:
            self.is_block = False
        if not self.is_block:
            if time() - self.start_interval > 3:
                self.start_interval = time()
        return True
