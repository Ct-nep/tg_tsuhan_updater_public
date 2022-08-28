# !/usr/bin/python3

import logging
import time
import urllib.parse
from collections import namedtuple
from concurrent import futures

import requests
from bs4 import BeautifulSoup

from bot_classutil import CrawlerBase, CrawlerManager


MAX_TIMEOUT = 15
# MAX_PAGE_LIMIT = 1

# logging settings are specified in main script.
logger = logging.getLogger(__name__)


class YahooCrawler(CrawlerBase):

    url = 'https://auctions.yahoo.co.jp/search/search'

    def __init__(self, threads=8):
        super().__init__(threads)

    def get_one(self, keyword: str, timeout=MAX_TIMEOUT) -> str:
        '''Get page 1 (first 100 entires) given specific keyword. Return
        text result.
        '''
        params = self._get_params(keyword)
        try:
            r = self._s.get(self.url, params=params, timeout=timeout)
            r.encoding = 'utf-8'
            r.raise_for_status()
        except requests.HTTPError as exc:
            logger.error(f'Cannot get page for keyword {keyword}. '\
                f'Exc message: {exc}')
            self.error_count += 1
            raise
        return r.text

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
            text = future.result() if not future.exception() else ''
            result[todo[future]] = text
        return result

    @staticmethod
    def _get_params(keyword: str, page=1) -> dict:
        offset = (page - 1) * 100 + 1
        params = {
            'auccat' : '',
            'tab_ex' : 'commerce',
            'ei' : 'utf-8',
            'aq' : -1,
            'oq' : '',
            'sc_i' : '',
            'exflg' : 1,
            'p' : keyword,      # Query keyword
            'b' : offset,        # Index start from 1
            'n' : 100       # Item in one page
        }
        return params


class YahooManager(CrawlerManager):

    Item = namedtuple(
            'Item',
            'item_id seller_id title item_url image_url bid_price '\
                'full_price end bid_num',
            defaults=(None, ) * 9
        )

    def __init__(self, keywords: list, db_path=':memory:'):
        super().__init__(keywords, db_path)
        self.info['site'] = 'yahoo'
        self._get_update_time()
        self.crawler = YahooCrawler()
        self._create_table('yahoo')
        self._create_table('yahoo_temp', overwrite=True)

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
                'bid_price INTEGER,'\
                'full_price INTEGER,'\
                'end REAL,'\
                'bid_num INTEGER,'\
                'record_time REAL,'\
                'update_time REAL)'
            self._con.execute(sql)
            self._con.commit()

    def _to_table(self, data: list) -> None:
        '''Pack items generated from get_item() into a temp table.'''
        try:
            sql = \
                'INSERT OR IGNORE INTO yahoo_temp '\
                '(item_id, seller_id, title, item_url, image_url, '\
                'bid_price, full_price, end, bid_num)'\
                'VALUES '\
                '(?, ?, ?, ?, ?, ?, ?, ?, ?)'
            self._con.executemany(sql, data)
        except Exception as exc:
            self._con.rollback()
            logger.error(f'Cannot write items into db. '\
                f'Exc message: {exc}')
            raise
        else:
            sql = \
                'UPDATE yahoo_temp '\
                'SET '\
                '(record_time, update_time) = (?, ?)'
            self._con.execute(sql, [self.info['time']] * 2)
            self._con.commit()

    @staticmethod
    def _check_empty(soup: BeautifulSoup) -> bool:
        '''Return True if page contains no item info, else False.'''
        return True if soup.find('div', 'Empty') else False

    @staticmethod
    def _get_bid_num(text: str) -> int:
        '''Parse text format bid info to get numbers.'''
        try:
            num = int(text[:-1])
        except Exception:
            try:
                num = int(text)
            except Exception:
                num = -1
        return num

    @staticmethod
    def _clear_query(url: str) -> str:
        '''Clear query params in the url.'''
        p = urllib.parse.urlparse(url)
        p = p._replace(query='')
        return urllib.parse.urlunparse(p)

    def get_item(self) -> None:
        '''Run Crawler and parse the dict of text into Item objects.
        Then pack all items into table yahoo_temp for later examination.
        '''
        raw = self.crawler.get_many(self.keywords)
        self.info['page'] = len(raw)
        self.info['error'] = self.crawler.error_count
        result = [] 
        for kw in raw:
            try:
                soup = BeautifulSoup(raw[kw], 'html.parser')
            except Exception as exc:
                logger.error(f'Cannot parse keyword {kw}. Exc: {exc}')
                self.info['error'] += 1
                continue
            if self._check_empty(soup):
                continue
            banner = soup.find_all('div', 'Product__detail')
            for i in banner:
                # Skip promotion.
                if i.find('div', 'Product__featured'):
                    continue
                bonus = i.div.attrs
                title = i.h3.a.attrs
                bid = i.find('span', 'Product__bid').text
                bid = self._get_bid_num(bid)
                item = self.Item(
                    item_id=bonus['data-auction-id'],
                    seller_id=bonus['data-auction-sellerid'],
                    title=title['data-auction-title'],
                    item_url=title['href'],
                    image_url=self._clear_query(title['data-auction-img']),
                    bid_price=bonus['data-auction-price'],
                    full_price=bonus['data-auction-buynowprice'],
                    end=float(bonus['data-auction-endtime']),
                    bid_num=bid
                )
                result.append(item)
        self.info['count'] = len(result)
        logger.info(f'Received {self.info["page"]} pages, '\
            f'{self.info["count"]} items with {self.info["error"]} error.')
        self._to_table(result)

    def compare(self) -> tuple:
        '''Compare old records with temp and return a tuple contains new
        / discounted / bid entries.'''
        if self.info['last'] == 0.0:
            new, discount, bid = [], [], []
            self.info['new'] = 0
            self.info['discount'] = 0
            self.info['bid'] = 0
            logger.info('Skip compare in first run.')
            return new, discount, bid
        # Find first-appeared & reappeared entries.
        # Only include items reappear after at least a day.
        expire = self.info['last'] - (60 * 60 * 24)
        sql = \
            'SELECT item_id, title, item_url, image_url, '\
                'bid_price, full_price, end, bid_num '\
            'FROM yahoo_temp '\
            'WHERE item_id NOT IN '\
                '(SELECT item_id FROM yahoo) '\
                'OR item_id IN '\
                '(SELECT item_id FROM yahoo WHERE update_time < ?)'
        new = self._con.execute(sql, [expire]).fetchall()
        self.info['new'] = len(new)
        # Find discounted entries.
        sql = \
            'SELECT t.item_id, t.title, t.item_url, t.image_url, '\
                'l.bid_price AS old_bid, l.full_price AS old_full, '\
                't.bid_price AS new_bid, t.full_price AS new_full, '\
                't.end, t.bid_num '\
            'FROM yahoo_temp AS t INNER JOIN yahoo AS l '\
                'ON t.item_id = l.item_id '\
            'WHERE (old_bid > new_bid OR old_full > new_full) '\
            'AND l.bid_num = 0'
        discount = self._con.execute(sql).fetchall()
        self.info['discount'] = len(discount)
        # Find bidding entries.
        sql = \
            'SELECT t.item_id, t.title, t.item_url, t.image_url, '\
                't.bid_price, t.full_price, t.end, '\
                'l.bid_num AS old_num, t.bid_num AS new_num '\
            'FROM yahoo_temp AS t INNER JOIN yahoo AS l '\
                'ON t.item_id = l.item_id '\
            'WHERE old_num < new_num'
        bid = self._con.execute(sql).fetchall()
        self.info['bid'] = len(bid)
        logger.info(f'Find {self.info["new"]} new item, '\
            f'{self.info["discount"]} discount, {self.info["bid"]} bid.')
        return (new, discount, bid)

    def update(self) -> None:
        '''Update table with new information from temp. This will modify
        database and thus irreversable.'''
        # Update recorded entires.
        sql = \
            'SELECT bid_price, full_price, end, bid_num, '\
                'update_time, item_id '\
            'FROM yahoo_temp '\
            'WHERE item_id IN (SELECT item_id FROM yahoo)'
        query = self._con.execute(sql).fetchall()
        sql = \
            'UPDATE yahoo '\
            'SET (bid_price, full_price, end, bid_num, update_time) '\
                '= (?, ?, ?, ?, ?) '\
            'WHERE item_id = ?'
        self._con.executemany(sql, query)
        # Insert new entires.
        sql = \
            'INSERT INTO yahoo '\
                '(item_id, seller_id, title, item_url, image_url, '\
                'bid_price, full_price, end, bid_num, record_time, '\
                'update_time) '\
            'SELECT t.item_id, t.seller_id, t.title, t.item_url, '\
                't.image_url, t.bid_price, t.full_price, t.end, '\
                't.bid_num, t.record_time, t.update_time '\
            'FROM yahoo_temp AS t '\
            'WHERE t.item_id NOT IN (SELECT item_id FROM yahoo)'
        self._con.execute(sql)
        # Append log info. For format concern, this will be the only
        # place in this code block that record entry as "sold" rather
        # than "bid".
        sql = \
            'INSERT INTO log (site, time, error, '\
                'page, count, new, discount, sold) '\
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        data = [
            self.info['site'], self.info['time'], self.info['error'],
            self.info['page'], self.info['count'], self.info['new'],
            self.info['discount'], self.info['bid']
        ]
        logger.info('Update database with new info.')
        self._con.execute(sql, data)
        self._con.commit()

    def get_message(self) -> list:
        '''Return a list of strings generated from update result. Each
        item is less than 4096 characters.'''
        new, discount, bid = self.compare()
        msg = \
            f'<b>Yahoo Updater</b><br>'\
            f'Time: {self.from_timestamp(time.time())}<br>'\
            f'Entries: {self.info["count"]}    '\
            f'Error: {self.info["error"]}<br>'
        msg += f'<br><b>[NEW] ---------- {self.info["new"]}</b><br>'
        for i in new:
            if i['full_price']:
                price = f'JPY {i["bid_price"]}[{i["full_price"]}]    '
            else:
                price = f'JPY {i["bid_price"]}    '
            msg += \
                f'<a href="{i["item_url"]}">'\
                f'{i["item_id"]}  {i["title"]}</a><br>'\
                + price + f'{i["bid_num"]} bid<br>'\
                f'End {self.from_timestamp(i["end"])}<br>'
        msg += \
            f'<br><b>[DISCOUNT] ---------- {self.info["discount"]}</b><br>'
        for i in discount:
            if i['new_full']:
                price = f'JPY {i["old_bid"]}[{i["old_full"]}] '\
                    f'-> {i["new_bid"]}[{i["new_full"]}]    '
            else:
                price = f'JPY {i["old_bid"]} -> {i["new_bid"]}    '
            msg += \
                f'<a href="{i["item_url"]}">'\
                f'{i["item_id"]}  {i["title"]}</a><br>'\
                + price + f'{i["bid_num"]} bid<br>'\
                f'End {self.from_timestamp(i["end"])}<br>'
        msg += \
            f'<br><b>[BID] ---------- {self.info["bid"]}</b><br>'
        for i in bid:
            if i['full_price']:
                price = f'JPY {i["bid_price"]}[{i["full_price"]}]    '
            else:
                price = f'JPY {i["bid_price"]}    '
            msg += \
                f'<a href="{i["item_url"]}">'\
                f'{i["item_id"]}  {i["title"]}</a><br>'\
                + price + f'{i["old_num"]} -> {i["new_num"]} bid<br>'\
                f'End {self.from_timestamp(i["end"])}<br>'
        return self._split(msg)


def main(keywords: list, db_path: str, update=False) -> dict:
    manager = YahooManager(keywords, db_path)
    manager.get_item()
    msg = manager.get_message()
    if update:
        manager.update()
    no_sound = not bool(
        manager.info['new'] \
        or manager.info['discount'] \
        or manager.info['bid']
        )
    return {'message': msg, 'disable_notification': no_sound}


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - \
    %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    msg = main()