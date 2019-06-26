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

    def __init__(self, base_url: str, total_pages: list, pages_per_tag: list,
                 num_threads: int = 100, time_bw_requests: int = 1):
        self.base_url = base_url
        self.tags = []
        self.total_pages = total_pages
        self.num_threads = num_threads
        self.time_bw_requests = time_bw_requests
        self.pages_per_tag = pages_per_tag

        self.client = MongoClient('localhost', 27017)
        self.db = self.client['WEB_SCRAPER']
        self.col = self.db['LINKS_STACK_QUESTIONS']

        self.session = requests.session()
        self.set_proxies('socks5h://localhost:9050')

        logging.config.fileConfig('LOG_CONFIG.ini')

    def get_base_url(self):
        return self.base_url

    def set_base_url(self, base_url):
        self.base_url = base_url

    def get_tags(self):
        return self.tags

    def set_tags(self, tags):
        self.tags = tags

    def set_proxies(self, proxy):
        self.session.proxies['http'] = proxy
        self.session.proxies['https'] = proxy

    def find_tags(self, page_num: int):
        time.sleep(self.time_bw_requests)

        response = self.session.get(
            self.base_url + 'tags?page=' + str(page_num) + '&tab=popular')

        logging.info(f'\nOn page {page_num}\n')

        soups = BeautifulSoup(response.content, 'lxml')

        question_tags = soups.find_all('a', attrs={'class': 'post-tag'})
        for tag in question_tags:
            self.tags.append(tag.string)

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

    def find_questions(self, page_num_and_tag: str):
        time.sleep(self.time_bw_requests)

        page_num = page_num_and_tag.split('::')[0]
        tag = page_num_and_tag.split('::')[1]

        current_tag = list(self.col.find({'tag_name': tag}))
        logging.info('Current Tag - {0}'.format(type(current_tag[0]['_id'])))

        response = self.session.get(self.base_url + 'questions/tagged/' +
                                    tag + '?sort=frequent&page=' + page_num + '&pagesize=50')

        logging.info(f'On page {page_num}')
        soups = BeautifulSoup(response.content, 'lxml')

        question_summaries = soups.find_all(
            'div', attrs={'class': 'question-summary'})

        question_count = 0
        questions_per_page = []
        for question_summary in question_summaries:
            summary = question_summary.find('div', attrs={'class': 'summary'})

            sep = '*'*10
            logging.info(sep + 'Question ' + str(question_count) + sep)
            question_count += 1

            question_text = summary.h3.a.text
            logging.info(f'Question text - {question_text}')

            question_link = summary.h3.a.attrs['href']
            logging.info(f'Question link - {question_link}')

            question_excerpt = summary.find(attrs={'class': 'excerpt'}).text
            logging.info(f'Question excerpt found!')

            question_votes = question_summary.find(
                attrs={'class': 'vote-count-post'}).text
            logging.info(f'Question votes - {question_votes}')

            try:
                question_accepted = True
                question_answers = question_summary.find(
                    attrs={'class': 'status answered-accepted'}).strong.text
            except:
                question_accepted = False
                question_answers = question_summary.find(
                    attrs={'class': 'status answered'}).strong.text

            logging.info(f'Question answers - {question_answers}')
            logging.info(f'Question accepted - {question_accepted}')

            question_views = question_summary.find(
                attrs={'class': 'views'}).attrs['title'].split()[0]
            logging.info(f'Question views - {question_views}')

            question_details = {
                'question_link': question_link,
                'question_text': question_text,
                'question_excerpt': question_excerpt,
                'question_votes': question_votes,
                'question_accepted': question_accepted,
                'question_answers': question_answers
            }

            questions_per_page.append(question_details)

        self.col.update_one(
            {
                "_id": current_tag[0]["_id"]
            },
            {
                "$push": {
                    "questions": {
                        "$each": questions_per_page
                    }
                }
            }
        )

    def find_all_questions(self, tag):
        pool = ThreadPool(self.num_threads)
        logging.info("Pool initiated!")
        self.pages_per_tag = list(
            map(lambda x: str(x)+"::"+tag, self.pages_per_tag)
        )
        pool.map(self.find_questions, self.pages_per_tag)
        logging.info("Pool mapped! {0}".format(self.pages_per_tag))
        pool.wait_completion()
        logging.info("Pool completed!")

    def find_all_questions_of_all_tags(self):
        # TODO: Add Multiprocessing
        pass


if __name__ == "__main__":
    start_time = time.time()

    scrape = ScrapeQuestionsByTag(
        "https://stackoverflow.com/", range(10), range(3, 2831))
    scrape.find_all_questions('javascript')

    print("exec_time:", time.time() - start_time)
