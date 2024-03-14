from loguru import logger
from ProxyList import ProxyList
from Config import App
from MySqlHandler import MySqlHandler
from ProxyHandler import ProxyChecker
import time


def save_proxies(proxy_list, name):
    if len(proxy_list) == 0:
        logger.info(f'no valid proxies got, waiting ...')
        return
    for proxy in proxy_list:
        is_in_db = MySqlHandler.has_proxy(proxy)
        if not ProxyChecker.check(proxy):
            logger.warning(f'invalid {proxy.type}://{proxy.ip}:{proxy.port}')
            if is_in_db:
                MySqlHandler.update(proxy, 100)
            continue
        if is_in_db:
            logger.info(f'update {proxy.type}://{proxy.ip}:{proxy.port} in db')
            MySqlHandler.update(proxy, 0)
        else:
            logger.info(f'insert {proxy.type}://{proxy.ip}:{proxy.port} to db')
            MySqlHandler.insert(proxy)


if __name__ == '__main__':
    logger.add('ProxyFree.log', rotation="5 MB", retention=2)
    App.load('Conf.json')
    MySqlHandler.init(App.all())
    while True:
        while True:
            if MySqlHandler.connect():
                break
            time.sleep(3)
        save_proxies(ProxyList.fetch_all())
        logger.info('All done. Waiting for the next query...')
        time.sleep(600)
