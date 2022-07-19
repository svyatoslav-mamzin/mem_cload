import logging
from codecs import decode
import backoff as backoff
from pymemcache import MemcacheServerError
from pymemcache.client import base


class MemCacheConnector:

    def __init__(self, addr):
        self.addr = addr

    def create_connection(self):
        try:
            return base.Client(self.addr)
        except Exception:
            raise


class MemCacheConnections:

    clients = {}

    @classmethod
    def get_client(cls, addr, new=False):
        if new or addr not in cls.clients:
            logging.debug(f"Новое подключение к {addr}")
            print(f'{addr=}')
            cls.clients[addr] = MemCacheConnector(addr).create_connection()
        else:
            logging.debug(f'Подключение к {addr}')
        return cls.clients[addr]


@backoff.on_exception(backoff.expo, (ConnectionRefusedError, MemcacheServerError), max_tries=3, jitter=None)
def memc_get(addr, *args, **kwargs):
    value = MemCacheConnections().get_client(addr).get(*args, **kwargs)
    return value if value is None else decode(value)


@backoff.on_exception(backoff.expo, (ConnectionRefusedError, MemcacheServerError), max_tries=3, jitter=None)
def memc_set(addr, *args, **kwargs):
    return MemCacheConnections().get_client(addr).set(*args, **kwargs)
