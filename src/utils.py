import re
import os
import json
import logging
from typing import Dict, Any
from collections import namedtuple

import requests
import pandas as pd
from fake_useragent import FakeUserAgent
from requests.exceptions import RequestException

FKUA = FakeUserAgent()
SESSION = requests.session()

multi_queues = namedtuple('MultiQueues', ['crawler', 'writer', 'other'])
each_task = namedtuple('Task', ['func', 'args', 'tries'])


def get(**kwargs) -> Dict[str, Any] | None:
    """Get the content from the url

    :param kwargs: the kwargs for requests.get (except headers)
    :return: a dict of contents
    """

    try:
        header = {'User-Agent': FKUA.random}
        req = SESSION.get(**kwargs, headers=header)

        req.raise_for_status()

        content = req.content.decode()
        content = re.findall(r'jQuery\d+_\d+\((.+)\)', content)[0]
        content = json.loads(content)

        return content

    except RequestException as req_exc:
        logging.getLogger(__name__).exception(req_exc, exc_info=False)

    return None


def write(result_df: pd.DataFrame, save_fp: str) -> bool:
    """Write the results

    :param result_df: a result dataframe
    :param save_fp: the save path
    :return: True for success, False for failure
    """

    try:
        folder_fp = os.path.dirname(save_fp)
        os.makedirs(folder_fp, exist_ok=True)

        if os.path.exists(save_fp):  # append to existing
            result_df.to_csv(save_fp,
                             mode='a',
                             header=False,
                             index=False,
                             encoding='utf-8-sig',
                             date_format='%Y-%m-%d')
        else:  # create new
            result_df.to_csv(save_fp,
                             mode='w',
                             header=True,
                             index=False,
                             encoding='utf-8-sig',
                             date_format='%Y-%m-%d')

        return True

    except OSError as os_err:
        logging.getLogger(__name__).exception(os_err)

    return False
