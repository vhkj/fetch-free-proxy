import re
from Proxy import Proxy
import pycountry
from loguru import logger
from MySqlHandler import MySqlHandler
from bs4 import BeautifulSoup
import time


class Policy:
    def __init__(self, name: str):
        self.name = name
        self.proxy_type = ''
        self.url = ''
        self.use_selenium = False
        self.webdriver_path = ''
        self.timeout_of_pull = 5
        self.timeout_of_check = 10
        self.country_short_filters = ''
        self.country_filters = ''
        self.page_max = 1

    def parse(self, data: str) -> list:
        return []

    def name(self):
        return self.name


class RegexPolicy(Policy):
    def __init__(self, rule: dict):
        super(RegexPolicy, self).__init__('regex')
        self.ip_extract_pattern = None
        self.port_extract_pattern = None
        self.proxy_type_pattern = None
        self.country_extract_pattern = None
        self.country_short_extract_pattern = None
        self.url = rule['url']
        keys = rule.keys()
        for key in keys:
            if key != 'ip' and key != 'port' and key != 'country' and key != 'country_short' and key != 'type':
                if key == 'timeout_of_pull':
                    self.timeout_of_pull = rule['timeout_of_pull']
                elif key == 'timeout_of_check':
                    self.timeout_of_check = rule['timeout_of_check']
                elif key == 'country_short_filters':
                    self.country_short_filters = rule[key]
                elif key == 'use_selenium':
                    self.use_selenium = rule['use_selenium']
                elif key == 'webdriver_path':
                    self.webdriver_path = rule['webdriver_path']
                continue
            pattern = None
            if key != 'type':
                pattern = re.compile(rule[key])
            if key == 'ip':
                self.ip_extract_pattern = pattern
            elif key == 'port':
                self.port_extract_pattern = pattern
            elif key == 'type':
                if len(rule[key]) > 0:
                    if rule[key] == 'socks5' or rule[key] == 'socks4' or rule[key] == 'http' or rule[key] == 'https':
                        self.proxy_type = rule[key]
                    else:
                        self.proxy_type_pattern = re.compile(rule[key])
            elif key == 'country':
                self.country_extract_pattern = pattern
            elif key == 'country_short':
                self.country_short_extract_pattern = pattern

    def parse(self, data: str) -> list:
        ret = []
        try:
            ips = []
            ports = []
            countries = []
            country_shorts = []
            proxy_types = []
            if self.ip_extract_pattern:
                ips = re.findall(self.ip_extract_pattern, data)
            if self.port_extract_pattern:
                ports = re.findall(self.port_extract_pattern, data)
            if self.country_extract_pattern:
                countries = re.findall(self.country_extract_pattern, data)
            if self.country_short_extract_pattern:
                country_shorts = re.findall(self.country_short_extract_pattern, data)
            if len(self.proxy_type) == 0:
                if self.proxy_type_pattern:
                    proxy_types = re.findall(self.proxy_type_pattern, data)
                else:
                    logger.error(f'no valid proxy "type" found on {self.url}, it can be actually type or regex exp')
                    return ret
            if len(ips) == 0 or len(ports) == 0 or len(ips) != len(ports):
                logger.warning(f'{self.url}: IPs not match PORTs or no IPs matched')
                return ret
            i = 0
            has_country = True if len(ips) == len(countries) else False
            has_country_short = True if len(ips) == len(country_shorts) else False
            has_types = True if len(proxy_types) == len(ips) else False
            for ip in ips:
                filtered = False
                proxy = Proxy()
                if len(ip) == 0:
                    continue
                proxy.ip = ip
                if len(ports[i]) == 0:
                    continue
                try:
                    proxy.port = int(ports[i])
                except ValueError as ve:
                    logger.error(f'{ve}')
                    continue
                if has_types:
                    proxy.type = proxy_types[i]
                elif len(self.proxy_type) > 0:
                    proxy.type = self.proxy_type
                else:
                    proxy.type = 'http'
                if has_country_short:
                    proxy.country_short = str(country_shorts[i]).upper()
                elif has_country:
                    country = countries[i]
                    if len(country) > 0:
                        if country == 'United States of America':
                            country = 'United States'
                        if len(self.country_filters) > 0 and country in self.country_filters:
                            filtered = True
                        else:
                            try:
                                proxy.country_short = pycountry.countries.get(name=country).alpha_2
                            except TypeError:
                                pass
                if not filtered and len(self.country_short_filters) > 0 and len(proxy.country_short) > 0:
                    if proxy.country_short in self.country_short_filters:
                        filtered = True
                if not filtered:
                    logger.success("{} {}://{}:{}", self.url, proxy.type, proxy.ip, proxy.port)
                    ret.append(proxy)
                else:
                    logger.debug("Filtered {}://{}:{}", proxy.type, proxy.ip, proxy.port)
                i += 1
        except Exception as e:
            logger.error(f'{e}')
        return ret


class CommonHtmlTablePolicy(Policy):
    def __init__(self, rule: dict):
        super(CommonHtmlTablePolicy, self).__init__('common-html-table')
        self.config = rule
        self.ip_port_mixed = False
        keys = rule.keys()
        self.url = self.config['url']
        if 'timeout_of_fetch' in keys:
            self.timeout_of_pull = self.config['timeout_of_fetch']
        if 'timeout_of_check' in keys:
            self.timeout_of_check = self.config['timeout_of_check']
        if 'country_short_filters' in keys:
            self.country_short_filters = self.config['country_short_filters']
        if 'country_filters' in keys:
            self.country_filters = self.config['country_filters']
        if 'use_selenium' in keys:
            self.use_selenium = self.config['use_selenium']
        if 'webdriver_path' in keys:
            self.webdriver_path = self.config['webdriver_path']
        if 'page_max' in keys:
            self.page_max = self.config['page_max']
        if 'type' in keys:
            self.proxy_type = self.config['type']
        if 'ip_port_mixed' in keys:
            self.ip_port_mixed = self.config['ip_port_mixed']

    def parse(self, data: str) -> list:
        ret = []
        try:
            soup = BeautifulSoup(data, 'html.parser')
            table = soup.find('table')
            if not table:
                return ret
            rows = table.find_all('tr')
            if not rows or len(rows) == 0:
                return ret
            for row in rows:
                cols = row.find_all('td')
                if not cols or len(cols) < 3:
                    continue
                proxy = Proxy()
                proxy.ip = str(cols[self.config['ip_index']].text).strip()
                if len(proxy.ip) == 0:
                    continue
                port = ''
                if self.config['port_index'] > 0:
                    port = str(cols[self.config['port_index']].text).strip()
                elif self.ip_port_mixed:
                    # 再次分割
                    ip_port = proxy.ip.split(':')
                    if len(ip_port) == 2:
                        proxy.ip = ip_port[0]
                        port = ip_port[1]
                else:
                    continue
                if len(port) == 0:
                    continue
                try:
                    proxy.port = int(port)
                except ValueError:
                    continue
                country = ''
                country_short = ''
                filtered = False
                if self.config['country_index'] >= 0:
                    country = str(cols[self.config['country_index']].text).strip()
                if self.config['country_short_index'] >= 0:
                    country_short = str(cols[self.config['country_short_index']].text).strip().upper()
                else:
                    try:
                        if len(country) > 0:
                            if country == 'United States of America':
                                country = 'United States'
                            proxy.country_short = pycountry.countries.get(name=country).alpha_2
                    except Exception as ee:
                        logger.error(f'{ee}')
                if len(country) > 0 and len(self.country_filters) > 0:
                    if len(self.country_filters) > 0 and country in self.country_filters:
                        filtered = True
                elif not filtered and len(country_short) > 0:
                    if len(self.country_short_filters) > 0 and country_short in self.country_short_filters:
                        filtered = True
                    else:
                        proxy.country_short = country_short

                if filtered:
                    logger.debug(f'Filtered {proxy.ip}:{proxy.port}')
                    continue

                if self.config['type_index'] >= 0:
                    proxy.type = str(cols[self.config['type_index']].text).strip().lower()
                if len(proxy.type) == 0 and len(self.proxy_type) == 0:
                    proxy.type = 'http'
                if proxy.type == 'socks4/5':
                    proxy.type = 'socks5'
                ret.append(proxy)
        except Exception as e:
            logger.error(f'{e}')
        return ret


class Rule:
    def __init__(self, rule: dict):
        name = rule['policy']
        self.policy = None
        self.enable = True
        if 'enable' in rule.keys():
            self.enable = rule['enable']
        if name == 'regex':
            self.policy = RegexPolicy(rule)
        elif name == 'common-html-table':
            self.policy = CommonHtmlTablePolicy(rule)

    def parse_proxies(self, data: str) -> list:
        return self.policy.parse(data)


