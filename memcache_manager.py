

class MemCacheConnector:

    def __init__(self, addr):
        self.addr = addr
        self.con = None

    def create_connection(self):
        try:
            client = memcache.Client([memc_addr])
        except Exception:
            raise
        return client


class MemCacheConnections:

    clients = {}

    @classmethod
    def get_client(cls, addr, new=False):
        if new or addr not in cls.connections:
            logging.info(f"Новое подключение к {addr}")
            cls.clients[db_name] = MemCacheConnector(addr).create_connection()
        else:
            logging.info(f'Подключение к {addr}')

        return cls.clients[addr]


