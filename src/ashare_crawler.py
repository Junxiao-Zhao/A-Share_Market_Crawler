import logging
import threading
from time import time
from datetime import datetime
from typing import Dict, Tuple, List
from queue import Queue, PriorityQueue, Empty
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tqdm import tqdm

from src.utils import get, write, multi_queues, each_task

PBAR_LOCK = threading.Lock()


class AShareCrawler:
    """Download A-Share Market stocks' history"""

    def __init__(self, req_info: Dict[str, str], save_fp: str,
                 dates: Tuple[datetime, datetime]) -> None:
        """Constructor

        :param req_info: a dict of urls and params
        :param save_fp: the save path
        :param dates: [start, end)
        """

        self.req_info = req_info
        self.save_fp = save_fp
        self.dates = dates

        self.queues = multi_queues(PriorityQueue(), Queue(), Queue())
        self.pbar = tqdm(total=0, unit='stock', desc='Saving')

    def assign_tasks(self, num_crawler: int, max_retry: int = 3) -> None:
        """Start the thread pool and submit tasks

        :param num_crawler: the number of crawler threads
        :param max_retry: the limit of retries
        """

        logging.getLogger(__name__).info('Thread Pool starts %d threads...',
                                         num_crawler + 1)

        with ThreadPoolExecutor(max_workers=num_crawler + 1) as pool:
            for _ in range(num_crawler):
                pool.submit(self.do_task, max_retry=max_retry)
            pool.submit(self.write_result)

        logging.getLogger(__name__).info('%d/%d stocks\' data are retrieved.',
                                         self.pbar.n, self.pbar.total)
        self.pbar.close()

    def do_task(self, max_retry: int = 3) -> None:
        """Do tasks in the crawler queue

        :param max_retry: the limit of retries
        """

        thread_id = threading.get_ident()
        self.queues.other.put(thread_id)
        logging.getLogger(__name__).info('Crawler Thread %d starts...',
                                         thread_id)

        while True:
            try:
                prior, task = self.queues.crawler.get(timeout=30)
                result = task.func(*task.args)
                task.tries += 1

                if result is None:

                    if task.tries >= max_retry:  # exceeds max retries
                        logging.getLogger(__name__).error(
                            '%s(%s) fails %d times. Stop retrying!',
                            task.func.__name__, str(task.args), max_retry)
                        continue

                    logging.getLogger(
                        __name__).warning(  # continue with lower prior
                            '%s(%s) fails %d times. Continue retrying...',
                            task.func.__name__, str(task.args), task.tries)

                    prior += 2
                    self.queues.crawler.put([prior, task])
                    continue

                if task.func.__name__ == 'get_list':

                    if not result:  # end of pages
                        continue

                    with PBAR_LOCK:
                        self.pbar.total += len(result)
                        self.pbar.refresh()

                    _ = [  # get stocks' history
                        self.queues.crawler.put(
                            [0, each_task(self.get_stock, [stk_id], 0)])
                        for stk_id in result
                    ]
                    self.queues.crawler.put(  # get next page
                        [1, each_task(self.get_list, [task.args[0] + 1], 0)])

                else:  # get_stock
                    self.queues.writer.put(
                        (result, self.save_fp % task.args[0][2:]))

            except Empty:
                break

            except Exception as exc:
                logging.getLogger(__name__).exception(exc)
                logging.getLogger(__name__).error('%s(%s)', task.func.__name__,
                                                  str(task.args))

        self.queues.other.get()
        logging.getLogger(__name__).info('Crawler Thread %d stopped.',
                                         thread_id)

    def write_result(self) -> None:
        """Write results"""

        thread_id = threading.get_ident()
        logging.getLogger(__name__).info('Writer Thread %d starts...',
                                         thread_id)

        while True:
            try:
                result, save_fp = self.queues.writer.get(timeout=120)

                if result.empty:  # no data
                    with PBAR_LOCK:
                        self.pbar.total -= 1
                        self.pbar.refresh()
                else:
                    if write(result, save_fp):  # write success
                        with PBAR_LOCK:
                            self.pbar.update(1)

            except Empty:
                if self.queues.other.empty():  # all crawlers stopped
                    break

            except Exception as exc:
                logging.getLogger(__name__).exception(exc)

        logging.getLogger(__name__).info('Writer Thread %d stopped', thread_id)

    def get_stock(self, stock_id: str) -> pd.DataFrame | None:
        """Get specified stock's history

        :param stock_id: a specified stock
        :return: a dataframe of its history
        """

        params = self.req_info['stock_params']
        params['secid'] = stock_id
        params['_'] = round(time())

        content = get(url=self.req_info['stock_url'], params=params)

        if content is None:
            return None

        try:
            data_df = pd.DataFrame(
                [each.split(',') for each in content['data']['klines']])
            data_df.columns = self.req_info['stock_cols']
            data_df['日期'] = pd.to_datetime(data_df['日期'])
            data_df = data_df[[(data_df['日期'] >= self.dates[0]) &
                               (data_df['日期'] < self.dates[1])]]
            return data_df

        except KeyError as key_err:
            logging.getLogger(__name__).exception(key_err, exc_info=False)

        return None

    def get_list(self, page_num: int) -> List[str] | None:
        """Get a list of stock codes on the specified page

        :param page_num: a page number
        :return: a list of stock codes
        """

        params = self.req_info['list_params']
        params['pn'] = page_num
        params['_'] = round(time())

        content = get(url=self.req_info['list_url'], params=params)

        if content is None:
            return None

        if not content['data']:  # end of list
            return []

        stock_ls = [
            '{}.{}'.format(each['f13'], each['f12'])
            for each in content['data']['diff']
        ]

        return stock_ls
