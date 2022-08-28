# !/usr/bin/python3

import logging
import time
from collections import namedtuple
from concurrent import futures

import requests

from bot_classutil import CrawlerBase, CrawlerManager
from bot_dpoputil import generate_DPOP


MAX_TIMEOUT = 15
# MAX_PAGE_LIMIT = 1

# logging settings are specified in main script.
logger = logging.getLogger(__name__)


class MercariCrawler(CrawlerBase):

    url = 'https://api.mercari.jp/search_index/search'

    def __init__(self, threads=8):
        super().__init__(threads)
        self._s.headers.update({
        'DPOP': None,
        'X-Platform': 'web',  # mercari requires this header
        'Accept': '*/*',
        'Accept-Encoding': 'deflate, gzip'
    })

    def get_one(self, keyword: str, timeout=MAX_TIMEOUT) -> str:
        '''Get page 1 (first 100 entires) given specific keyword. Return
        text result.
        '''
        params = self._get_params(keyword)
        try:
            self._s.headers['DPOP'] = self._get_DPOP()
            r = self._s.get(self.url, params=params, timeout=timeout)
            r.raise_for_status()
        except requests.HTTPError as exc:
            logger.error(f'Cannot get page for keyword {keyword}. '\
                f'Exc message: {exc}')
            self.error_count += 1
            raise
        return r.json()

    def get_many(self, keywords: list) -> dict:
        '''Get all possible pages for each keyword, return a dict object
        with json-like structure.
        Example: {keyword : result, ...}
        '''
        logger.info(f'Start getting {len(keywords)} word(s).')
        todo = {self.executor.submit(self.get_one, kw) : kw \
            for kw in keywords}
        result = {}
        for future in futures.as_completed(todo):
            text = future.result() if not future.exception() else {}
            result[todo[future]] = text
        return result

    @staticmethod
    def _get_params(keyword: str, page=0) -> dict:
        params = {
        'keyword': keyword,
        'limit': 120,
        'page': page,
        # page_token? e.g. page_token=v1%3A1
        'sort': 'created_time',
        'order': 'desc',
        }
        return params

    @staticmethod
    def _get_DPOP() -> dict:
        '''Get DPOP signature requried in headers.'''
        dpop =  generate_DPOP(
            uuid='Mercari Python Bot',
            method='GET',
            url='https://api.mercari.jp/search_index/search'
        )
        return dpop


class MercariManager(CrawlerManager):

    Item = namedtuple(
            'Item',
            'item_id seller_id title item_url image_url price onsale',
            defaults=(None, ) * 7
        )

    def __init__(self, keywords: list, db_path=':memory:'):
        super().__init__(keywords, db_path)
        self.info['site'] = 'mercari'
        self._get_update_time()
        self.crawler = MercariCrawler()
        self._create_table('mercari')
        self._create_table('mercari_temp', overwrite=True)

    def _create_table(self, name: str, overwrite=False) -> None:
        if not self.check_exist(name, overwrite=overwrite):
            sql = \
                f'CREATE TABLE {name}'\
                '(id INTEGER PRIMARY KEY AUTOINCREMENT,'\
                'item_id TEXT UNIQUE NOT NULL,'\
                'seller_id TEXT,'\
                'title TEXT,'\
                'item_url TEXT,'\
                'image_url TEXT,'\
                'price INTEGER,'\
                'onsale INTEGER,'\
                'record_time REAL,'\
                'update_time REAL)'
            self._con.execute(sql)
            self._con.commit()

    def _to_table(self, data: list) -> None:
        '''Pack items generated from get_item() into a temp table.'''
        try:
            sql = \
                'INSERT OR IGNORE INTO mercari_temp '\
                '(item_id, seller_id, title, item_url, image_url, '\
                'price, onsale)'\
                'VALUES '\
                '(?, ?, ?, ?, ?, ?, ?)'
            self._con.executemany(sql, data)
        except Exception as exc:
            self._con.rollback()
            logger.error(f'Cannot write items into db. '\
                f'Exc message: {exc}')
            raise
        else:
            sql = \
                'UPDATE mercari_temp '\
                'SET '\
                '(record_time, update_time) = (?, ?)'
            self._con.execute(sql, [self.info['time']] * 2)
            self._con.commit()

    @staticmethod
    def _get_image_url(url: str) -> str:
        orig_url = 'https://static.mercdn.net/item/detail/orig/photos/'
        orig_url += url.rsplit('/', 1)[1]
        return orig_url

    @staticmethod
    def _get_item_url(iid: str) -> str:
        item_url = 'https://jp.mercari.com/item/' + iid
        return item_url

    def get_item(self) -> None:
        '''Run Crawler and parse the dict of text into Item objects.
        Then pack all items into table mercari_temp for later examination.
        '''
        raw = self.crawler.get_many(self.keywords)
        self.info['page'] = len(raw)
        self.info['error'] = self.crawler.error_count
        result = [] 
        for kw in raw:
            if 'data' not in raw[kw].keys():
                logger.error(f'No data in {kw}.')
                self.info['error'] += 1
                continue
            for i in raw[kw]['data']:
                item = self.Item(
                    item_id=i['id'],
                    seller_id=i['seller']['id'],
                    title=i['name'],
                    item_url=self._get_item_url(i['id']),
                    image_url=self._get_image_url(i['thumbnails'][0]),
                    price=i['price'],
                    onsale=1 if i['status'] == 'on_sale' else 0,
                )
                result.append(item)
        self.info['count'] = len(result)
        logger.info(f'Received {self.info["page"]} pages, '\
            f'{self.info["count"]} items with {self.info["error"]} error.')
        self._to_table(result)

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
        # Find first-appeared & reappeared entries.
        sql = \
            'SELECT item_id, title, item_url, image_url, price '\
            'FROM mercari_temp '\
            'WHERE onsale = 1 '\
            'AND (item_id NOT IN (SELECT item_id FROM mercari) '\
                'OR item_id IN '\
                '(SELECT item_id FROM mercari WHERE onsale = 0))'
        new = self._con.execute(sql).fetchall()
        self.info['new'] = len(new)
        # Find discounted entries.
        sql = \
            'SELECT t.item_id, t.title, t.item_url, t.image_url, '\
                'l.price AS old, t.price AS new '\
            'FROM mercari_temp AS t INNER JOIN mercari AS l '\
                'ON t.item_id = l.item_id '\
            'WHERE old > new AND t.onsale = 1'
        discount = self._con.execute(sql).fetchall()
        self.info['discount'] = len(discount)
        # Find sold out entries.
        sql = \
            'SELECT item_id, title, item_url, image_url, price '\
            'FROM mercari_temp '\
            'WHERE onsale = 0 '\
            'AND (item_id NOT IN (SELECT item_id FROM mercari) '\
                'OR item_id IN '\
                '(SELECT item_id FROM mercari WHERE onsale = 1))'
        sold = self._con.execute(sql).fetchall()
        self.info['sold'] = len(sold)
        logger.info(f'Find {self.info["new"]} new item, '\
            f'{self.info["discount"]} discount, {self.info["sold"]} sold.')
        return (new, discount, sold)

    def update(self) -> None:
        '''Update table with new information from temp. This will modify
        database and thus irreversable.'''
        # Update recorded entires.
        sql = \
            'SELECT price, onsale, update_time, item_id '\
            'FROM mercari_temp '\
            'WHERE item_id IN (SELECT item_id FROM mercari)'
        query = self._con.execute(sql).fetchall()
        sql = \
            'UPDATE mercari '\
            'SET (price, onsale, update_time) = (?, ?, ?) '\
            'WHERE item_id = ?'
        self._con.executemany(sql, query)
        # Insert new entires.
        sql = \
            'INSERT INTO mercari '\
                '(item_id, seller_id, title, item_url, image_url, '\
                'price, onsale, record_time, update_time) '\
            'SELECT item_id, seller_id, title, item_url, image_url, '\
                'price, onsale, record_time, update_time '\
            'FROM mercari_temp AS t '\
            'WHERE t.item_id NOT IN (SELECT item_id FROM mercari)'
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
            f'<b>Mercari Updater</b><br>'\
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
    manager = MercariManager(keywords, db_path)
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