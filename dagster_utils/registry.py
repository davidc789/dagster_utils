from collections.abc import MutableMapping


class Registry(MutableMapping):
    """ A registry for data sources. """
    def __init__(self, *args, **kwargs):
        self._registry = {}

    def register(self, cls):
        self._registry[cls.__name__] = cls
        return cls

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
