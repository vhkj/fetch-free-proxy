import random
from Proxy import Proxy
import requests
from loguru import logger
from MySqlHandler import MySqlHandler
import random


class ProxyChecker:
    @staticmethod
    def check(proxy: Proxy):
        base_url = "http://www.gstatic.com/generate_204"
        req_proxy = { proxy.type: str.format("{}:{}", proxy.ip, proxy.port)}
        try:
            response = requests.get(base_url, proxies=req_proxy, timeout=1)
            if response.status_code == 204:
                logger.success("{}://{}:{}", proxy.type, proxy.ip, proxy.port)
                return True
        except Exception as e:
            logger.warning(f'invalid {proxy.type}://{proxy.ip}:{proxy.port}')
        return False

    @staticmethod
    def get_a_proxy() -> (str, str, int):
        proxies = MySqlHandler.get_valid_proxies(50)
        if len(proxies) == 0:
            return '', '', 0
        proxy = random.choice(proxies)
        # req_proxy = {proxy.type: str.format("{}:{}", proxy.ip, proxy.port)}
        return proxy.type, proxy.ip, proxy.port

