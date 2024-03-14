import concurrent.futures
from fp.fp import FreeProxy
from Proxy import Proxy
import requests
import time
from loguru import logger
from ProxyHandler import ProxyChecker
from concurrent.futures import ThreadPoolExecutor, ALL_COMPLETED
from selenium import webdriver
from Config import App
from RuleFactory import RuleFactory

HEADERS = {'DNT': '1', 'Upgade-Insecure-Requests': '1', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36 OPR/60.0.3255.170'}


class ProxyList:
    def __init__(self):
        # [ Proxy(), ]
        self.free_proxies = []
        #self.pool = ThreadPoolExecutor(64)
        #self.tasklist = []

    @staticmethod
    def __get_data_source_from_file(file: str) -> str:
        logger.info('handle rule on: {}', file)
        data = ""
        with open(file, 'r') as f:
            data = f.read()
        return data

    @staticmethod
    def __get_data_source_from_requests(url: str, timeout: int) -> str:
        logger.info('handle rule on: {} via requests', url)
        data = ''
        req_proxy = None
        proxy_type, proxy_ip, proxy_port = ProxyChecker.get_a_proxy()
        if len(proxy_ip) != 0 and len(proxy_type) != 0 and len(proxy_port) != 0:
            req_proxy = {f'{proxy_type}': f'{proxy_ip}:{proxy_port}'}
        try:
            response = requests.get(url, headers=HEADERS, proxies=req_proxy, timeout=timeout)
            if response.status_code == 200:
                data = response.text
        except Exception as e:
            logger.error(f'request {url}: {e}')
        return data

    @staticmethod
    def __get_data_source_from_selenium(url: str, timeout: int, webdriver_path: str) -> str:
        logger.info('handle rule on: {} via selenium', url)
        data_source = ''
        proxy_type, proxy_ip, proxy_port = ProxyChecker.get_a_proxy()
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument("--no-report-upload")
        options.add_argument("--disable-logging")
        options.add_argument("--disable-crash-reporter")
        options.add_argument("--disable-component-update")
        options.add_argument("--disable-breakpad")
        options.add_argument("--disable-notifications")
        options.add_argument("--no-first-run")
        options.add_argument("--disable-background-networking")
        if len(proxy_ip) > 0 and len(proxy_port) > 0:
            options.add_argument(str.format("--proxy-server={}:{}", proxy_ip, proxy_port))
        driver = None
        if len(webdriver_path) > 0:
            driver = webdriver.Chrome(options=options, service=webdriver.ChromeService(executable_path=webdriver_path))
        else:
            driver = webdriver.Chrome(options=options)
        try:
            driver.get(url)
            driver.implicitly_wait(1)
            data_source = driver.page_source
        except Exception as e:
            logger.error(f'get {url}: {e}')
        driver.quit()
        return data_source

    def fetch_rules_free_proxies(self):
        rules = RuleFactory.load_rules(App.config('spider_rule'))
        if len(rules) == 0:
            return []
        for rule in rules:
            if not rule.enable:
                continue
            for i in range(1, rule.policy.page_max + 1):
                data_source = ""
                url = rule.policy.url
                if rule.policy.page_max > 1:
                    url = f'{url}{i}'
                if not url.startswith('http'):
                    data_source = ProxyList.__get_data_source_from_file(url)
                elif rule.policy.use_selenium:
                    data_source = ProxyList.__get_data_source_from_selenium(url, rule.policy.timeout_of_pull, rule.policy.webdriver_path)
                else:
                    data_source = ProxyList.__get_data_source_from_requests(url, rule.policy.timeout_of_pull)
                self.free_proxies.extend(rule.parse_proxies(data_source))
                time.sleep(5)
        return self.free_proxies

    def fetch_github_free_proxies(self):
        logger.info('fatch proxies from https://github.com/jundymek/free-proxy')
        try:
            github_free_proxies = FreeProxy().get_proxy_list(False)
            for item in github_free_proxies:
                fragments = item.split(':')
                if len(fragments) == 2:
                    record = Proxy()
                    record.ip = fragments[0]
                    record.port = int(fragments[1])
                    record.type = 'http'
                    self.free_proxies.append(record)
        except Exception as e:
            logger.error(f'{e}')
        return self.free_proxies

    @staticmethod
    def fetch_all():
        fetcher = ProxyList()
        fetcher.fetch_github_free_proxies()
        fetcher.fetch_rules_free_proxies()
        logger.info("rough proxies: {}", len(fetcher.free_proxies))
        return fetcher.free_proxies
