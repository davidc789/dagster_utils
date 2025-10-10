from collections.abc import MutableMapping
from typing import Any, TypeVar

_T = TypeVar('_T')


class Registry[_T](MutableMapping):
    """ A generic registry for storing objects. """
    def __init__(self, *args, **kwargs):
        self._registry = {}

    def register(self, name: str, obj: _T) -> None:
        self._registry[name] = obj
        return obj

    def __getitem__(self, key):
        return self._registry[key]

    def __setitem__(self, key, value, /):
        self._registry[key] = value

    def __delitem__(self, key, /):
        del self._registry[key]

    def __len__(self):
        return len(self._registry)

    def __iter__(self):
        return iter(self._registry)
