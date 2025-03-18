from typing import Union
from pymemcache import serde
from pymemcache.client.base import PooledClient

from translator.progress_interface import ProgressReadInterface

PROGRESS_IS_INDEXING_KEY = 'is_indexing'
PROGRESS_PERCENT_COMPLETE_KEY = 'percent_complete'


def create_memcached_client(server: str) -> PooledClient:
    return PooledClient(
        server,
        max_pool_size=256,
        connect_timeout=1,
        timeout=30,
        ignore_exc=True,
        no_delay=True,
        serde=serde.pickle_serde,
    )


class MemcachedReadProgress(ProgressReadInterface):

    def __init__(self, client: PooledClient, prefix: str):
        self.client = client
        self.prefix = prefix

    @property
    def is_indexing(self) -> bool:
        return self.client.get(f'{self.prefix}{PROGRESS_IS_INDEXING_KEY}', False)

    @property
    def percent_complete(self) -> int:
        percent = self.client.get(f'{self.prefix}{PROGRESS_PERCENT_COMPLETE_KEY}', 0)
        return int(percent)


class MemcachedWriteProgress:

    def __init__(self, client: PooledClient, prefix: str, num_entites: int):
        self.client = client
        self.prefix = prefix
        self.percent_per_entity = 100 / num_entites

    @property
    def is_indexing(self) -> bool:
        return self.client.get(f'{self.prefix}{PROGRESS_IS_INDEXING_KEY}', False)

    @is_indexing.setter
    def is_indexing(self, value: bool):
        self.client.set(f'{self.prefix}{PROGRESS_IS_INDEXING_KEY}', value, noreply=False)

    @property
    def percent_complete(self) -> Union[int, float]:
        percent = self.client.get(f'{self.prefix}{PROGRESS_PERCENT_COMPLETE_KEY}', 0)
        return percent

    @percent_complete.setter
    def percent_complete(self, value: Union[int, float]):
        self.client.set(f'{self.prefix}{PROGRESS_PERCENT_COMPLETE_KEY}', value, noreply=False)

    def add_entities_complete(self, num_entities: int):
        if num_entities <= 0:
            return
        percent = num_entities * self.percent_per_entity
        self.percent_complete += percent

    def reset(self):
        self.client.set(f'{self.prefix}{PROGRESS_IS_INDEXING_KEY}', False, noreply=False)
        self.client.set(f'{self.prefix}{PROGRESS_PERCENT_COMPLETE_KEY}', 0, noreply=False)

    def close(self):
        self.client.close()
