from argparse import ArgumentParser
from bs4 import BeautifulSoup
from datetime import datetime
from elasticsearch import Elasticsearch

import json
import logging
import math
import os
import requests
import signal
import sys
import time


INDEX_NAME = 'archive'
KEYS = ['title', 'content', 'original']
TAGS_BLACKLIST = ['script', 'style']

COMPARISON_AMOUNT = 10
TERM_AMOUNT = 10

DEFAULT_COSINE_THRESHOLD = 0.9
DEFAULT_CRAWL_INTERVAL = 1


class LoggingFilter(logging.Filter):
    def filter(self, record):
        return record.levelno <= logging.INFO


class ShutdownHandler(object):
    """
    Handles shutdown by setting the flag kill_now to True once it receives an abortion or termination signal.
    Can be used to shutdown gracefully and save any remaining data to file before exiting.
    """

    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGABRT, self.exit_gracefully)
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        """
        Set the kill_now flag to True, to indicate the application should exit.
        """
        self.kill_now = True


class Indexer(object):
    """
    Performs the bulk of the work. Retrieves documents, measures them for relevance and
    indexes them if they are considered relevant.
    """

    def __init__(self, elastic_search, logger, threshold):
        self.es = elastic_search
        self.logger = logger
        self.threshold = threshold

    @staticmethod
    def cosine_similarity(query_terms, other_terms):
        """
        Computes the cosine similarity between two vectors.

        :param query_terms: term vector of the query document
        :param other_terms: term vector of the other document
        :return: the cosine similarity between the vectors
        """

        sum_a = sum_b = sum_ab = 0

        for label in set(query_terms) | set(other_terms):
            sum_a += query_terms.get(label, 0) ** 2
            sum_b += other_terms.get(label, 0) ** 2
            sum_ab += query_terms.get(label, 0) * other_terms.get(label, 0)

        if sum_ab == 0:
            return 0
        return float(sum_ab) / (sum_a ** 0.5 * sum_b ** 0.5)

    @staticmethod
    def extract_termvectors(tv_object, amount=None):
        """
        Extracts the actual term vectors (with tf-idf values) from the Elasticsearch result object

        :param tv_object: term vectors object as returned by Elasticsearch
        :param amount: the amount of term vectors to use (keeps the ones with highest tf-idf). Default: all (None)
        :return: a dictionary with the terms as key and their tf-idf as values
        """

        num_docs = tv_object['term_vectors']['content']['field_statistics']['doc_count']
        vectors = tv_object['term_vectors']['content']['terms']

        result = {}

        for key in vectors:
            tf = vectors[key]['term_freq']

            freq = vectors[key].get('doc_freq', 1)
            idf = 1 + math.log(num_docs / freq)

            result[key] = tf * idf**2

        if amount is not None:
            best_terms = sorted(result.items(), key=lambda pair: pair[1])[:amount]
            return dict(best_terms)

        return result

    def is_relevant(self, document):
        """
        Verifies whether the document is relevant. Does so by:
        - executing a MLT query with the query document against Elasticsearch to find the 10 most similar documents
        - extracting the term vectors of each of the result documents
        - extracting the term vectors of the query document
        - computing the cosine similarity between the query term vector and all result term vectors
        - comparing the lowest cosine similarity to the specified threshold

        :param document: the document of which we want to determine whether it is relevant
        :return: whether the given document is relevant to the user's interests
        """
        query = {
            'bool': {
                # Compares the document to the Elasticsearch index
                'must': {
                    'more_like_this': {
                        'fields': ['title', 'content'],
                        'like': document['content'],
                    }
                },
                # Makes sure the field 'original' does not exist, i.e. the document
                # was archived through normal usage, not through crawling
                'must_not': {
                    'exists': {
                        'field': 'original'
                    }
                }
            }
        }

        results = self.es.search(index=INDEX_NAME, body={'query': query}, size=COMPARISON_AMOUNT)

        most_similar = []

        # Extract term vectors of all result documents
        for result in results['hits']['hits'][:COMPARISON_AMOUNT]:
            res = self.es.termvectors(index='archive', doc_type='response', id=result['_id'], fields='content',
                                      offsets=False, positions=False, term_statistics=True)

            most_similar.append(self.extract_termvectors(res, TERM_AMOUNT))

        # Extract term vector of query document
        doc_tv = self.extract_termvectors(self.es.termvectors(index='archive', doc_type='response',
                                                              body={'doc': {'content': document['content']}},
                                                              offsets=False, positions=False, term_statistics=True),
                                          TERM_AMOUNT)

        similarities = [self.cosine_similarity(doc_tv, tv) for tv in most_similar]
        return min(similarities) > self.threshold

    def index(self, document):
        """
        Indexes the specified relevant document into Elasticsearch

        :param document: the document to index
        """

        body = {key: value for key, value in document.items() if key in KEYS}
        body['request'] = [{
            'uri': document['url'],
            'date': datetime.now(),
        }]

        self.es.index(index=INDEX_NAME, doc_type='response', body=body)

    def handle(self, entry):
        """
        Handles the entry chosen by the topical crawler. Performs the following actions:
        - retrieves the HTTP headers to assert the document is an HTML file
        - retrieves the document itself
        - strips any script, style or invisible tags from the document, so only visible text remains
        - extracts the title from the document
        - checks if the document is relevant
        - if it is relevant, indexes the document

        :param entry: the entry to handle
        """

        r = requests.head(entry['url'])

        if 'text/html' in r.headers['Content-Type']:
            r = requests.get(entry['url'])
            soup = BeautifulSoup(r.content, features='html.parser')
            [tag.extract() for tag in soup.findAll(TAGS_BLACKLIST)]
            content = soup.get_text(' ', strip=True)

            entry['content'] = content
            entry['title'] = soup.title.string if soup.title is not None else entry['url']

            if self.is_relevant(entry):
                self.logger.info('Response at {} is relevant!'.format(entry['url']))
                self.index(entry)
            else:
                self.logger.info('Response at {} is irrelevant!'.format(entry['url']))
        else:
            self.logger.warning('Found non-HTML response at {}'.format(entry['url']))


class BackgroundCrawler(object):
    """
    Represents the topical crawler, i.e. the component that decides on the crawl ordering.
    """

    def __init__(self, elastic_search, indexer, urls_path, interval):
        self.es = elastic_search
        self.indexer = indexer
        self.urls_path = urls_path
        self.interval = interval

    def get_existing_urls(self):
        """
        Creates a generator with all URLs currently present in the Elasticsearch index.
        Is meant to make sure URLs are not processed more than once.
        Uses pagination to prevent requests from becoming too large.

        :return: a generator with all URLs that are already present
        """
        _from = 0
        _size = 100

        results = self.es.search(index=INDEX_NAME, body={
            '_source': 'request.uri',
            'from': _from, 'size': _size
        })['hits']['hits']

        while results:
            for result in results:
                for request in result['_source'].get('request', []):
                    yield request['uri']
            _from += _size
            results = self.es.search(index=INDEX_NAME, body={
                '_source': 'requests.uri',
                'from': _from, 'size': _size
            })['hits']['hits']

    def get_best_entry(self):
        if not os.path.exists(self.urls_path):
            return None

        existing_urls = set(self.get_existing_urls())

        with open(self.urls_path) as _file:
            entries = [entry for entry in json.load(_file) if entry['url'] not in existing_urls]

        if not entries:
            return None

        # Constructs a list of queries for bulk search
        # Queries consist of the context provided alongside the document URL
        queries = ['{}\n{}'.format(
                       json.dumps({'index': INDEX_NAME}),
                       json.dumps({
                           'query': {
                               'common': {
                                   'content': {
                                       'query': entry['context'],
                                       'cutoff_frequency': 0.01,
                                    }
                               }
                           },
                           'size': COMPARISON_AMOUNT
                       })
                   ) for entry in entries]

        result = self.es.msearch(index=INDEX_NAME, body='\n'.join(queries))

        # Assign the highest Elasticsearch score to each entry
        for entry, res in zip(entries, result['responses']):
            entry['score'] = res['hits']['max_score']

        entries.sort(key=lambda entry: entry['score'])
        best_entry = entries.pop()

        # Replace the list of entries such that the chosen entry is no longer present
        with open(self.urls_path, 'w') as _file:
            json.dump(entries, _file)

        return best_entry

    def execute(self):
        """
        Executes one iteration of the crawler lifecycle, by finding the best entry and
        passing it on to the indexer
        """

        best_entry = self.get_best_entry()

        if best_entry is None:
            return

        self.indexer.handle(best_entry)

    def run(self):
        """ Initiates the crawler. """
        shutdown_handler = ShutdownHandler()
        while not shutdown_handler.kill_now:
            self.execute()
            time.sleep(self.interval)


def init_logger():
    """
    Constructs a logger instance with proper formatting and output to stdout and stderr.

    :return: the constructed logger instance
    """
    bc_logger = logging.getLogger('background_crawler')

    bc_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    log_filter = LoggingFilter()

    debug_log = logging.StreamHandler(sys.stdout)
    debug_log.setLevel(logging.DEBUG)
    debug_log.setFormatter(formatter)
    debug_log.addFilter(log_filter)

    error_log = logging.StreamHandler(sys.stderr)
    error_log.setLevel(logging.WARNING)
    error_log.setFormatter(formatter)

    bc_logger.addHandler(debug_log)
    bc_logger.addHandler(error_log)

    return bc_logger


if __name__ == '__main__':
    parser = ArgumentParser(description='Runs a background crawler that retrieves and indexes relevant documents.')
    parser.add_argument('urls_path', help='The file containing the urls to crawl')

    parser.add_argument('-t', '--threshold', type=float, default=DEFAULT_COSINE_THRESHOLD,
                        help='The cosine threshold above which a document is marked as relevant.')
    parser.add_argument('-i', '--interval', type=int, default=DEFAULT_CRAWL_INTERVAL,
                        help='The interval between crawl intervals (in seconds).')

    args = parser.parse_args()

    logger = init_logger()

    es = Elasticsearch()

    indexer = Indexer(es, logger, args.threshold)
    bc = BackgroundCrawler(es, indexer, args.urls_path, args.interval)

    try:
        bc.run()
    except Exception as e:
        logger.exception(e)
