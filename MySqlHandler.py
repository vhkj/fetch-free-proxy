import mysql.connector
from loguru import logger
from Proxy import Proxy
from Config import App


class MySqlHandler:
    __conn = None
    __config = {}

    @staticmethod
    def init(conf: dict):
        MySqlHandler.__config["user"] = conf.get("user")
        MySqlHandler.__config["password"] = conf["password"]
        MySqlHandler.__config["host"] = conf["host"]
        MySqlHandler.__config["port"] = conf["port"]
        MySqlHandler.__config["raise_on_warnings"] = conf["raise_on_warnings"]
        MySqlHandler.__config["database"] = conf["database"]

    @staticmethod
    def __get_conn():
        try:
            if MySqlHandler.__conn:
                if MySqlHandler.__conn.is_connected():
                    return MySqlHandler.__conn
                else:
                    MySqlHandler.__conn.close()
            MySqlHandler.__conn = mysql.connector.connect(**MySqlHandler.__config)
        except mysql.connector.Error as e:
            logger.error(f'{e}')
        return MySqlHandler.__conn

    @staticmethod
    def destroy():
        if MySqlHandler.__conn:
            MySqlHandler.__conn.close()
            MySqlHandler.__conn = None

    @staticmethod
    def connect():
        if App.config('nosql'):
            return True
        conn = MySqlHandler.__get_conn()
        if not conn:
            return False
        return conn.is_connected()

    @staticmethod
    def insert(proxy: Proxy):
        if App.config('nosql'):
            return True
        inserted = False
        conn = MySqlHandler.__get_conn()
        if not conn:
            return inserted
        try:
            # 获取cursor对象
            cursor = conn.cursor()
            # 插入数据的SQL语句
            insert_query_prefix = str.format("INSERT INTO {}.proxy", MySqlHandler.__config["database"])
            insert_query = insert_query_prefix + " (`ip`, `port`, `failcount`, `type`) VALUES (%s, %s, %s, %s)"
            # 要插入的数据
            data = (proxy.ip, proxy.port, 0, proxy.type)
            # 执行插入操作
            cursor.execute(insert_query, data)
            # 提交事务
            conn.commit()
            inserted = True
        except mysql.connector.Error as e:
            logger.error(f'插入数据失败：{e}')
        finally:
            if 'cursor' in locals():
                cursor.close()
        return inserted

    @staticmethod
    def update(proxy: Proxy, fail_count=0):
        if App.config('nosql'):
            return True
        updated = False
        conn = MySqlHandler.__get_conn()
        if not conn:
            return updated
        try:
            # 获取cursor对象
            cursor = conn.cursor()
            update_query = str.format("UPDATE {}.proxy SET failcount = {} WHERE ip = '{}' and port = {}", MySqlHandler.__config["database"], fail_count, proxy.ip, proxy.port)
            cursor.execute(update_query)
            conn.commit()
            updated = True
        except mysql.connector.Error as e:
            logger.error(f'更新数据失败：{e}')
        finally:
            if 'cursor' in locals():
                cursor.close()
        return updated

    @staticmethod
    def has_proxy(proxy: Proxy):
        if App.config('nosql'):
            return True
        found = False
        conn = MySqlHandler.__get_conn()
        if not conn:
            return found
        try:
            # 获取cursor对象
            cursor = conn.cursor()
            select_query = str.format("SELECT * FROM `{}`.`proxy` WHERE ip = '{}' and port = {}", MySqlHandler.__config["database"], proxy.ip, proxy.port)
            cursor.execute(select_query)
            result = cursor.fetchall()
            found = True if len(result) > 0 else False
        except mysql.connector.Error as e:
            logger.error(f'查询数据失败：{e}')
        finally:
            if 'cursor' in locals():
                cursor.close()
        return found

    @staticmethod
    def get_valid_proxies(count: int):
        proxies = []
        if App.config('nosql'):
            return proxies
        conn = MySqlHandler.__get_conn()
        if not conn:
            return proxies
        try:
            # 获取cursor对象
            cursor = conn.cursor()
            select_query = str.format("SELECT * FROM `{}`.`proxy` WHERE failcount < 100 LIMIT 0,{}", MySqlHandler.__config["database"], count)
            cursor.execute(select_query)
            result = cursor.fetchall()
            for row in result:
                proxy = Proxy()
                proxy.ip = row[1]
                proxy.port = row[2]
                proxy.type = row[3]
                proxies.append(proxy)
        except mysql.connector.Error as e:
            logger.error(f'查询数据失败：{e}')
        finally:
            if 'cursor' in locals():
                cursor.close()
        return proxies
