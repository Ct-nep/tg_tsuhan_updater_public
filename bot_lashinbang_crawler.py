# !/usr/bin/python3

import logging
import time
import json
import re
from collections import namedtuple
from concurrent import futures

import requests

from bot_classutil import CrawlerBase, CrawlerManager


MAX_TIMEOUT = 15
MAX_PAGE_LIMIT = 12

# logging settings are specified in main script.
logger = logging.getLogger(__name__)


class LashinbangCrawler(CrawlerBase):

    url = 'https://lashinbang-f-s.snva.jp/'
    url_age = 'https://shop.lashinbang.com/age_check'
    url_ref = 'https://shop.lashinbang.com/'

    def __init__(self, threads=5):
        super().__init__(threads)
        self._s.headers.update({'Referer': self.url_ref})

    def pass_age_check(self, timeout=MAX_TIMEOUT):
        r = self._s.get(self.url_age, timeout=timeout)
        r.raise_for_status()
        logger.info('Age check passed.')

    def get_one(self, keyword: str, page: int, timeout=MAX_TIMEOUT) -> str:
        '''Get one page given specific keyword and page number. Return
        trimmed text result (json-like) without further parse.'''
        if page > MAX_PAGE_LIMIT:
            logger.warning(f'Too many pages for keyword {keyword}.')
            self.error_count += 1
            raise ValueError(f'Requesting page {page} for {keyword} '\
                f'while maximum acceptable number is {MAX_PAGE_LIMIT}.')
        params = self._get_params(keyword, page)
        try:
            r = self._s.get(self.url, params=params, timeout=timeout)
            r.raise_for_status()
        except requests.HTTPError as exc:
            logger.error(f'Cannot get page for keyword {keyword}, '\
                f'page {page}. Exc message: {exc}')
            self.error_count += 1
            raise
        # Result is in json format, but with residual heads and tails.
        # Clear them to be ready for parsing.
        return r.text[9: -2]

    def get_many(self, keywords: list) -> dict:
        '''Get all possible pages for each keyword, return a dict object
        with json-like structure.
        Example: {keyword : {pages : int, page_no : str, ...}, ...}'''
        logger.info(f'Start getting {len(keywords)} word(s).')
        # First pass the age check to avoid unexpected block.
        self.pass_age_check()
        todo = {self.executor.submit(self.get_one, kw, 1) : kw \
            for kw in keywords}
        result = {}
        for future in futures.as_completed(todo):
            if not future.exception():
                text = future.result()
                pages = min(self._get_page_count(text), MAX_PAGE_LIMIT)
            else:
                text, pages = None, 1
            result[todo[future]] = {'pages' : pages, 1 : text}
        todo = {self.executor.submit(self.get_one, kw, i + 2) : (kw, i + 2) \
            for kw in keywords for i in range(result[kw]['pages'] - 1)}
        for future in futures.as_completed(todo):
            kw, page = todo[future]
            result[kw][page] = future.result() \
                if not future.exception() else None
        return result

    @staticmethod
    def _get_params(keyword: str, page: int) -> dict:
        offset = (page - 1) * 100
        params = {
            'q': keyword,
            'searchbox[]': keyword,
            's6o': 1,
            'pl': 1,
            'sort': 'Number18,Score',
            'limit': 100,       # Number of items in one query
            'o': offset,      # Offset, technically choosing pages
            'n6l': 1,
            'callback': 'callback',
            'controller': 'lashinbang_front',
            '_': round(time.time() * 1000)      # Time stamp
        }
        return params

    @staticmethod
    def _get_page_count(text: str) -> int:
        regex = re.compile(r'(?<="last_page":)\d+')
        res = next(regex.finditer(text))
        page_count = int(res.group())
        return page_count


class LashinbangManager(CrawlerManager):

    Item = namedtuple(
            'Item',
            'item_id title item_url image_url price',
            defaults=(None, ) * 5
        )

    def __init__(self, keywords: list, db_path=':memory:'):
        super().__init__(keywords, db_path)
        self.info['site'] = 'lashinbang'
        self._get_update_time()
        self.crawler = LashinbangCrawler()
        self._create_table('lashinbang')
        self._create_table('lashinbang_temp', overwrite=True)

    def _create_table(self, name: str, overwrite=False) -> None:
        if not self.check_exist(name, overwrite=overwrite):
            sql = \
                f'CREATE TABLE {name}'\
                '(id INTEGER PRIMARY KEY AUTOINCREMENT,'\
                'item_id INTEGER UNIQUE NOT NULL,'\
                'title TEXT,'\
                'item_url TEXT,'\
                'image_url TEXT,'\
                'price INTEGER,'\
                'record_time REAL,'\
                'update_time REAL)'
            self._con.execute(sql)
            self._con.commit()

    def _to_table(self, data: list) -> None:
        '''Pack items generated from get_item() into a temp table.'''
        try:
            sql = \
                'INSERT OR IGNORE INTO lashinbang_temp '\
                '(item_id, title, item_url, image_url, price) '\
                'VALUES '\
                '(?, ?, ?, ?, ?)'
            self._con.executemany(sql, data)
        except Exception as exc:
            self._con.rollback()
            logger.error(f'Cannot write items into db. '\
                f'Exc message: {exc}')
            raise
        else:
            sql = \
                'UPDATE lashinbang_temp '\
                'SET '\
                '(record_time, update_time) = (?, ?)'
            self._con.execute(sql, [self.info['time']] * 2)
            self._con.commit()

    def get_item(self) -> None:
        '''Run Crawler and parse the dict of text into Item objects.
        Then pack all items into table lashinbang_temp for later
        examination.'''
        raw = self.crawler.get_many(self.keywords)
        self.info['page'] = sum(
            raw[kw]['pages'] for kw in raw
        )
        self.info['error'] = self.crawler.error_count
        items = [] 
        for kw in raw:
            pages = raw[kw]['pages']
            for p in range(pages):
                try:
                    js = json.loads(raw[kw][p + 1])
                    js = js['kotohaco']['result']['items']
                except TypeError as exc:
                    logger.error(f'Cannot load {kw} '\
                        f'page {p + 1} into json format. Exc: {exc}')
                    continue
                except KeyError as exc:
                    logger.error(f'{kw} page {p + 1} was loaded, '\
                        f'but not in expected format. Exc: {exc}')
                    self.info['error'] += 1
                    continue
                for i in js:
                    items.append(self.Item(
                        i['itemid'],
                        i['title'], 
                        i['url'], 
                        i['image'], 
                        i['price']
                    ))
        self.info['count'] = len(items)
        logger.info(f'Received {self.info["page"]} pages, '\
            f'{self.info["count"]} items with {self.info["error"]} error.')
        self._to_table(items)

    def compare(self) -> tuple:
        '''Compare old records with temp and return a tuple contains new
        / discounted / sold entries.'''
        if self.info['last'] == 0.0:
            new, discount, sold = [], [], []
            self.info['new'] = 0
            self.info['discount'] = 0
            self.info['sold'] = 0
            logger.info('Skip compare in first run.')
            return new, discount, sold
        # Find first-appeared & reappeared entires.
        sql = \
            'SELECT item_id, title, item_url, image_url, price '\
            'FROM lashinbang_temp '\
            'WHERE item_id NOT IN '\
                '(SELECT item_id FROM lashinbang) '\
            'OR item_id IN '\
                '(SELECT item_id FROM lashinbang WHERE update_time < ?)'
        new = self._con.execute(sql, [self.info['last']]).fetchall()
        self.info['new'] = len(new)
        # Find discounted entires.
        sql = \
            'SELECT l.item_id, l.title, l.item_url, l.image_url, '\
                'l.price AS old, t.price AS new '\
            'FROM lashinbang_temp AS t INNER JOIN lashinbang AS l '\
                'ON t.item_id = l.item_id '\
            'WHERE l.price > t.price'
        discount = self._con.execute(sql).fetchall()
        self.info['discount'] = len(discount)
        # Only check for sold items if no error occured.
        # Find disappeared entries since last stable update.
        if self.info['error'] == 0:
            sql = \
                'SELECT item_id, title, item_url, image_url, price '\
                'FROM lashinbang '\
                'WHERE item_id NOT IN '\
                    '(SELECT item_id FROM lashinbang_temp)'\
                'AND update_time >= ?'
            sold = self._con.execute(sql, 
                [self.info['last']]).fetchall()
            self.info['sold'] = len(sold)
        else:
            sold = []
            self.info['sold'] = 0
        logger.info(f'Find {self.info["new"]} new item, '\
            f'{self.info["discount"]} discount, {self.info["sold"]} sold.')
        return new, discount, sold

    def update(self) -> None:
        '''Update table with new information from temp. This will modify
        database and thus irreversable.'''
        # Update recorded entires.
        sql = \
            'SELECT price, update_time, item_id '\
            'FROM lashinbang_temp '\
            'WHERE item_id IN (SELECT item_id FROM lashinbang)'
        query = self._con.execute(sql).fetchall()
        sql = \
            'UPDATE lashinbang '\
            'SET (price, update_time) = (?, ?) '\
            'WHERE item_id = ?'
        self._con.executemany(sql, query)
        # Insert new entires.
        sql = \
            'INSERT INTO lashinbang '\
                '(item_id, title, item_url, image_url, price, '\
                'record_time, update_time) '\
            'SELECT item_id, title, item_url, image_url, price, '\
            'record_time, update_time '\
            'FROM lashinbang_temp AS t '\
            'WHERE t.item_id NOT IN (SELECT item_id FROM lashinbang)'
        self._con.execute(sql)
        # Append log info.
        sql = \
            'INSERT INTO log (site, time, error, '\
                'page, count, new, discount, sold) '\
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        data = [
            self.info['site'], self.info['time'], self.info['error'],
            self.info['page'], self.info['count'], self.info['new'],
            self.info['discount'], self.info['sold']
        ]
        logger.info('Update database with new info.')
        self._con.execute(sql, data)
        self._con.commit()

    def get_message(self) -> list:
        '''Return a list of strings generated from update result. Each
        item is less than 4096 characters.'''
        new, discount, sold = self.compare()
        msg = \
            f'<b>Lashinbang Updater</b><br>'\
            f'Time: {self.from_timestamp(time.time())}<br>'\
            f'Entries: {self.info["count"]}    '\
            f'Error: {self.info["error"]}<br>'
        msg += f'<br><b>[NEW] ---------- {self.info["new"]}</b><br>'
        for i in new:
            msg += \
                f'<a href="{i["item_url"]}">'\
                f'{i["item_id"]}  {i["title"]}</a><br>'\
                f'JPY {i["price"]}<br>'
        msg += \
            f'<br><b>[DISCOUNT] ---------- {self.info["discount"]}</b><br>'
        for i in discount:
            msg += \
                f'<a href="{i["item_url"]}">'\
                f'{i["item_id"]}  {i["title"]}</a><br>'\
                f'JPY {i["old"]} -> {i["new"]}<br>'
        msg += f'<br><b>[SOLD] ---------- {self.info["sold"]}</b><br>'
        for i in sold:
            msg += \
                f'<a href="{i["item_url"]}">'\
                f'{i["item_id"]}  {i["title"]}</a><br>'\
                f'JPY {i["price"]}<br>'
        return self._split(msg)


def main(keywords: list, db_path: str, update=False) -> dict:
    manager = LashinbangManager(keywords, db_path)
    manager.get_item()
    msg = manager.get_message()
    if update:
        manager.update()
    no_sound = not bool(
        manager.info['new'] \
        or manager.info['discount'] \
        or manager.info['sold']
        )
    return {'message': msg, 'disable_notification': no_sound}


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - \
    %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    msg = main()