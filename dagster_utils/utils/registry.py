import inspect
import warnings
from typing import MutableMapping, TypeVar, Callable, TYPE_CHECKING, Hashable

if TYPE_CHECKING:
    from typing import ClassVar

from sqlalchemy.orm import DeclarativeBase

_K = TypeVar("_K", bound=Hashable, default=str)
_V = TypeVar("_V")
__all__ = ["Registry", "RegisterSubclass"]


class Registry(MutableMapping[_K, _V]):
    """ A generic registry for storing objects. """
    def __init__(self):
        self._registry = {}

    def register(self, name: _K, obj: _V) -> None:
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
    """ A Mixin for tracking all subclasses.

    `registry` is reserved for tracking all subclasses.
    `subclass_discriminator` can optionally be specified to change which class field to use as the subclass identifier.
    If this value is not specified in a subclass, its class name will be used.
    This value must be unique for each subclass.
    """
    if TYPE_CHECKING:
        __name__: ClassVar[str]

        registry: ClassVar[Registry]

    def __init_subclass__(cls, subclass_discriminator="type", **kwargs):
        subclass_key = cls.__dict__.get(subclass_discriminator, cls.__name__)

        # Ensure the integrity of the registry before proceeding.
        if hasattr(cls, "registry") and not isinstance(cls.registry, Registry):
            raise TypeError(f"The registry attribute on {cls.__name__} is not a valid instance of 'Registry'.")

        # Set up the class as a base registry class
        if RegisterSubclass in cls.__bases__:
            if "registry" in cls.__dict__:
                warnings.warn(
                    f"The registry attribute on {cls.__name__} is used for internal purposes by the package. "
                    "It is an advanced feature to override this attribute in subclasses and typically not recommended.",
                    UserWarning,
                )
            elif hasattr(cls, "registry"):
                warnings.warn(
                    f"{cls.__name__} is already a subclass of class with 'RegisterSubclass'. "
                    f"The original registry will be overridden.",
                    UserWarning,
                )
            cls.registry = Registry()
        elif not hasattr(cls, "registry"):
            raise AttributeError(
                f"{cls.__name__} is not a subclass of class with 'RegisterSubclass'. "
                f"Please ensure the base class inherits from 'RegisterSubclass'.")
        # All classes except ABCs are to be registered.
        elif not inspect.isabstract(cls):
            # Use the following code for debugging inheritance issues.
            # cls._dagster_utils_base_class.registry.register(cls.__name__, cls)
            cls.registry.register(subclass_key, cls)

        super().__init_subclass__(**kwargs)


class RegisterDecorator(object):
    """ A mixin for registering functions through decorator methods. """
    registry: Registry

    def __init__(self):
        self.registry = Registry()

    def register(self, fn: Callable | None = None, *args, **kwargs) -> Callable:
        if fn is None:
            return lambda _fn: self.register(_fn, *args, **kwargs)

        self.registry.register(fn.__name__, fn)
        return fn
