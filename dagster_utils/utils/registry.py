import inspect
import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import MutableMapping, TypeVar

_T = TypeVar("_T")


class Registry(MutableMapping[str, _T]):
    """ A generic registry for storing objects. """
    def __init__(self, *args, **kwargs):
        self._registry = {}

    def register(self, name: str, obj: _T) -> None:
        if name in self._registry:
            raise ValueError(f"Name {name} is already registered against {self._registry[name]}.")

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


class RegisterSubclass(object):
    """ A Mixin for tracking all subclasses. """
    def __init_subclass__(cls, **kwargs):
        # ABCs do not need to be registered.
        if inspect.isabstract(cls):
            super().__init_subclass__(**kwargs)
            return


        # Setup the class as a base registry class
        if RegisterSubclass in cls.__bases__:
            if cls.__dict__.get("registry") is not None:
                raise warnings.warn(
                    f"The registry attribute on {cls.__name__} is used for internal purposes by the package. "
                    "It is an advanced feature to override this attribute in subclasses and typically not recommended.",
                    UserWarning,
                )
            cls.registry = Registry()
            cls._dagster_utils_base_class = cls
        else:
            cls._dagster_utils_base_class.registry.register(cls.__name__) = cls
            super().registry.register(cls.__name__, cls)
        super().__init_subclass__(**kwargs)
        print(f"{cls.__name__} is subclassing {cls.__dagster_utils_base_class__.__name__}")
