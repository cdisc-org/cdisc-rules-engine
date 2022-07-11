from enum import Enum


class BaseEnum(Enum):
    @classmethod
    def contains(cls, value):
        return value in cls.values()

    @classmethod
    def values(cls):
        return set(item.value for item in cls)
