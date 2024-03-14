from Proxy import Proxy


class ProxyPool:
    __proxies = []

    @staticmethod
    def add_to_pool(proxies: list):
        ProxyPool.__proxies.extend(proxies)

    @staticmethod
    def del_from_pool(proxy: Proxy):
        ProxyPool.__proxies.remove(proxy)

