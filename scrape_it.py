import requests

import logging
import configparser
import pprint
import re

from pymongo import MongoClient
from bs4 import BeautifulSoup, SoupStrainer

import time
import socket
import logging
import socks
import billiard

from os import path
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


config = configparser.ConfigParser()
config.read("CONFIG.ini")

logging.basicConfig(level=int(config['LOGGING']
                              ['LEVEL']), filename=config['LOGGING']['FILENAME'],
                    filemode=config['LOGGING']['FILEMODE'],
                    format='[ %(asctime)s - %(process)d - %(levelname)s ] %(message)s')

client = MongoClient(config['MONGODB']['CLIENT_IP'],
                     int(config['MONGODB']['CLIENT_PORT']))
db = client[config['MONGODB']['DB_NAME']]
link_doc = db[config['MONGODB']['COLLECTION_NAME']]

counter = 0


def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element.encode('utf-8'))):
        return False
    return True


def traverse_links(url_to_traverse):

    global counter

    if len(url_to_traverse) < 10:
        return

    response = requests.get(url_to_traverse)
    soups = BeautifulSoup(response.content, "lxml")
    data = soups.findAll(text=True)

    links = []
    for soup in soups.findAll('a'):
        try:
            current_link = soup.get('href')
            if "http" in current_link[:5]:
                links.append(current_link)
                counter += 1
                logging.info("Link Appended - {0}".format(current_link))
                logging.info("Counter = {0}".format(counter))
        except:
            logging.error(
                f'Error fetching links from {url_to_traverse}', exc_info=True)

    doc = {
        'title': soups.title.string,
        'links': links,
        'text': " ".join(list(map(lambda x: x.strip("\n\r "), (list(filter(visible, data))))))
    }

    link_doc_id = link_doc.insert_one(doc)
    logging.info(
        f'Inserted Successfuly from {url_to_traverse} with ID: {link_doc_id}')

    for link in links:
        traverse_links(link)

    # pool = ThreadPool(100)
    # pool.map(traverse_links, links)
    # pool.wait_completion()


traverse_links(input("Enter the initial link:"))

for links in link_doc.find({}):
    pprint.pprint(links)
