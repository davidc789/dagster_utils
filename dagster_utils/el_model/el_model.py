import inspect
from abc import ABC
from sys import implementation
from typing import Any, Callable, Annotated, TypeVar, NamedTuple, ClassVar

import dagster as dg
from pydantic import BaseModel

from ..resources import DataSpecResource, ConnectionResource
from ..resources.abc import DataResource
from ..utils.registry import Registry
from ..utils.decorator_utils import safe_is_subclass

_SourceType = TypeVar("_SourceType", bound=DataResource)
_TargetType = TypeVar("_TargetType", bound=DataResource)


class ELModelConfig(dg.Model):
    # TODO: Enforce model interface
    read_options: dict[str, Any]
    write_options: dict[str, Any]


class ELModelSpec[_SourceType, _TargetType](object):
    source: _SourceType
    target: _TargetType
    backend: str | None = None
    options: dict[str, str] | None = None


class ImplementationKey(NamedTuple):
    """ Represents a key used for mapping to the actual implementation function.

    Attributes:
        name: The name of the implementation key. Need to be unique given source_type and target_type.
        source_type: The type of the source for which the implementation is defined.
        target_type: The type of the target for which the implementation is defined.
    """
    name: str
    source_type: str
    target_type: str


ELModelImplementationFn = Callable[[ELModelSpec[_SourceType, _TargetType]], Any]


class ELModelImplementation(BaseModel):
    name: str
    source_type: str
    target_type: str
    backend: str | None = None
    options_type: str | None = None
    compute_fn: ELModelImplementationFn

    registry: ClassVar[Registry[ImplementationKey, "ELModelImplementation"]] = Registry[ImplementationKey, "ELModelImplementation"]()

    @classmethod
    def register(
            cls,
            fn: ELModelImplementationFn = None,
            *,
            name: str | None = None,
            is_default: bool = False,
            source_type: str | type[DataSpecResource] | None = None,
            target_type: str | type[DataSpecResource] | None = None,
            backend: str | None = None,
    ):
        # When the decorator is called with arguments, pass them into the decorator.
        if fn is None:
            def inner_fn(fn: Callable):
                return cls.register_implementation(
                    fn,
                    name=name,
                    is_default=is_default,
                    source_type=source_type,
                    target_type=target_type,
                    backend=backend,
                )

            return inner_fn

        if not inspect.isfunction(fn):
            raise TypeError("Only functions may be registered as implementations.")

        parameters = inspect.signature(fn).parameters
        el_model_parameters = [
            (k, v)
            for k, v in parameters.items()
            if safe_is_subclass(v.annotation, ELModelSpec)
        ]
        if len(el_model_parameters) != 1:
            raise ValueError(
                f"Exactly one ELModelSpec parameter should be provided. Received {len(el_model_parameters)}.")

        if source_type is None and target_type is None:
            # If no types are provided, use the types provided in the annotation.
            annotation = el_model_parameters[0][1].annotation

            if annotation.__args__ is None:
                raise ValueError(
                    "Mising type annotations for the ELModelSpec parameter. "
                    "Please provide the types as type annotations or through the source_type and target_type arguments "
                    "in the decorator.")
            if len(annotation.__args__) != 2:
                raise ValueError(
                    f"Malformed type annotations for the ELModelSpec parameter. "
                    f"Expecting 2 type parameters, received {len(annotation.__args__)}.")

            source_type = annotation.__args__[0]
            target_type = annotation.__args__[1]

            if not safe_is_subclass(source_type, DataSpecResource):
                raise ValueError(f"Source type must be a subclass of DataSpecResource. Received type {source_type}.")
            if not safe_is_subclass(target_type, DataSpecResource):
                raise ValueError(f"Target type must be a subclass of DataSpecResource. Received type {target_type}.")
        elif source_type is None or target_type is None:
            raise ValueError("Both source_type and target_type must be provided.")

        # Convert all types to strings.
        if isinstance(source_type, type):
            source_type: str = source_type.model_fields["type"].default
        if isinstance(target_type, type):
            target_type: str = target_type.model_fields["type"].default

        implementation_name = fn.__name__ if name is None else name
        key = ImplementationKey(
            name=implementation_name,
            source_type=source_type,
            target_type=target_type,
        )
        implementation = ELModelImplementation(
            name=implementation_name,
            source_type=source_type,
            target_type=target_type,
            backend=backend,
            compute_fn=fn,
        )
        cls.registry.register(key, implementation)
        return fn


class ELModel(dg.Model, ABC):
    """MODEL SUMMARY HERE."""
    description: str | None
    source: DataSpecResource
    target: DataSpecResource
    config: ELModelConfig
    pipeline: list[str] | None = None
