# !/usr/bin/python3
# Auto run crawler scripts.

import logging
import json
from configparser import ConfigParser

from bot_lashinbang_crawler import main as lashinbang_main
from bot_mercari_crawler import main as mercari_main
from bot_yahoo_crawler import main as yahoo_main


# Read config from an ini file.
def read_config(path='./config.ini') -> dict:
    c = ConfigParser()
    c.read(path, encoding='utf-8')
    js = json.loads(c.get('KEYWORDS', 'keywords'))
    config = {
        'db_path': c.get('PATH', 'db_path'),
        'log_path': c.get('PATH', 'log_path'),
        'lashinbang': js['lashinbang'],
        'mercari': js['mercari'],
        'yahoo': js['yahoo']
    }
    return config


def main():
    config = read_config()
    logging.basicConfig(filename=config['log_path'], format='%(asctime)s - \
        %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger(__name__)
    message = lashinbang_main(
            config['lashinbang'], 
            config['db_path']
        )['message']
    message += mercari_main(
            config['mercari'],
            config['db_path']
        )['message']
    message += yahoo_main(
            config['yahoo'],
            config['db_path']
        )['message']
    return ''.join(message)


if __name__ == '__main__':
    main()