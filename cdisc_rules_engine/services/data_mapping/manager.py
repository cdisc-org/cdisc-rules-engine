from multiprocessing.managers import BaseManager
from .data_map import GlobalDataMap


class MyManager(BaseManager):
    pass


MyManager.register("GlobalDataMap", GlobalDataMap)
