import requests
import logging.config
import configparser
import re
import time
import sys
import billiard

from pymongo import MongoClient
from bs4 import BeautifulSoup, SoupStrainer

from queue import Queue
from threading import Thread
from billiard.context import Process
from billiard import cpu_count


class Worker(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try:
                func(*args, **kwargs)
            except Exception as e:
                print(e)
            finally:
                self.tasks.task_done()


class ThreadPool:
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kwargs):
        self.tasks.put((func, args, kwargs))

    def map(self, func, args_list):
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        self.tasks.join()


class ScrapeQuestionsByTag:

    def __init__(self, base_url, total_pages, num_threads=100, time_bw_requests=1):
        self.base_url = base_url
        self.tags = []
        self.total_pages = total_pages
        self.num_threads = num_threads
        self.time_bw_requests = time_bw_requests

        self.client = MongoClient('localhost', 27017)
        self.db = self.client['WEB_SCRAPER']
        self.col = self.db['LINKS_STACK']

        logging.config.fileConfig('LOG_CONFIG.ini')

    def find_tags(self, page_num):
        time.sleep(self.time_bw_requests)
        response = requests.get(
            self.base_url + '?page=' + str(page_num) + '&tab=popular')
        logging.info(f'\nOn page {page_num}\n')
        soups = BeautifulSoup(response.content, 'lxml')
        question_tags = soups.find_all('a', attrs={'class': 'post-tag'})
        for tag in question_tags:
            self.tags.append(tag)

            doc = {
                'page_num': page_num,
                'tag_link': str(tag),
                'tag_name': tag.string
            }
            self.col.insert_one(doc)

            logging.info(f'Tag found - {tag.string}')

    def find_all_tags(self):
        pool = ThreadPool(self.num_threads)
        pool.map(self.find_tags, self.total_pages)
        pool.wait_completion()


if __name__ == "__main__":
    start_time = time.time()

    scrape = ScrapeQuestionsByTag(
        "https://stackoverflow.com/tags", range(1528, 1625), 125, 10)
    scrape.find_all_tags()

    print("exec_time:", time.time() - start_time)
