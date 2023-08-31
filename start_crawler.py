import json
import logging
import logging.config
import configparser
import argparse
from datetime import datetime

from src import AShareCrawler

arg_parser = argparse.ArgumentParser(
    description='Download stocks\' data in the given range')
arg_parser.add_argument('-s',
                        '--start',
                        nargs=1,
                        type=str,
                        required=True,
                        help='Start date (include) in format %Y%m%d')
arg_parser.add_argument('-e',
                        '--end',
                        nargs=1,
                        type=str,
                        required=True,
                        help='End date (exclude) in format %Y%m%d')


def main() -> None:
    """Start the crawler"""

    config = configparser.RawConfigParser()
    config.read('./config/main.cfg')
    logging.config.fileConfig(config.get('log', 'log_cfg'))
    args = arg_parser.parse_args()

    req_info = {
        'stock_url': config.get('urls', 'stock_url'),
        'list_url': config.get('urls', 'list_url'),
        'stock_params': json.loads(config.get('params', 'stock_params')),
        'list_params': json.loads(config.get('params', 'list_params')),
        'stock_cols': config.get('others', 'stock_cols').split(',')
    }
    start = datetime.strptime(args.start[0], '%Y%m%d')
    end = datetime.strptime(args.end[0], '%Y%m%d')

    logging.getLogger(__name__).info('Retrieving data of stocks in [%s, %s)',
                                     start.date(), end.date())

    ashr_crwl = AShareCrawler(req_info, config.get('paths', 'save_fp'),
                              (start, end))
    ashr_crwl.assign_tasks(int(config.get('others', 'num_crawler')))


main()
