import datetime
import hashlib
import json
from functools import partial
from pathlib import Path
from typing import Callable, Literal, Any

import dagster as dg
import pytz
from cloudpickle import cloudpickle

HashAlgorithm = Literal["md5"]

def parse_hash_algorithm(hash_algorithm: HashAlgorithm) -> Callable[[bytes], str]:
    if hash_algorithm == "md5":
        return partial(hashlib.md5, usedforsecurity=False)
    else:
        raise ValueError(f"Unsupported hash algorithm '{hash_algorithm}'")


def stable_hash(*args: str | bytes, hash_algorithm: HashAlgorithm = "md5") -> str:
    """ Given some arguments, produces a stable 64-bit hash of their contents.

    Supports bytes and strings. Strings will be UTF-8 encoded. Copied from prefect.

    :param args: Items to include in the hash.
    :param hash_algorithm: Hash algorithm from hashlib to use.
    :returns: A hex hash.
    """
    hash_callable = parse_hash_algorithm(hash_algorithm)
    h = hash_callable()
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        h.update(a)
    return h.hexdigest()


def object_hash(*args: Any, hash_algorithm: HashAlgorithm = "md5", raise_on_failure: bool = False,
                **kwargs: Any) -> str | None:
    """ Attempt to hash objects by dumping to JSON or serializing with cloudpickle.

    Copied from prefect.

    :param args: Positional arguments to hash.
    :param hash_algorithm: Hash algorithm to use.
    :param raise_on_failure: If True, raise exceptions instead of returning None.
    :param kwargs: Keyword arguments to hash.
    :returns: A hash string or None if hashing failed.

    :raises HashError: If objects cannot be hashed and raise_on_failure is True.
    """
    json_error = None
    pickle_error = None

    try:
        return stable_hash(json.dumps((args, kwargs), sort_keys=True), hash_algorithm=hash_algorithm)
    except Exception as e:
        json_error = str(e)

    try:
        return stable_hash(cloudpickle.dumps((args, kwargs)), hash_algorithm=hash_algorithm)
    except Exception as e:
        pickle_error = str(e)

    if raise_on_failure:
        msg = (
            "Unable to create hash - objects could not be serialized.\n"
            f"  JSON error: {json_error}\n"
            f"  Pickle error: {pickle_error}"
        )
        raise ValueError(msg)

    return None


def file_hash(path: str, method: Literal["content", "metadata"] = "content", hash_algorithm: HashAlgorithm = "md5"):
    """ Given a path to a file, produces a stable hash of the selected file.

    Hash value can be computed precisely based on the entire file content, or heuristically based on metadata. Copied
    from prefect.

    :param path: The path to a file.
    :param method: Method to use for computing the hash. Either "content" or "metadata".
    :param hash_algorithm: Hash algorithm from hashlib to use.
    :returns: A hash of the file.
    """
    if method == "content":
        contents = Path(path).read_bytes()
        return stable_hash(contents, hash_algorithm=hash_algorithm)
    elif method == "metadata":
        stats = Path(path).stat()
        return object_hash(stats.st_mtime, stats.st_size, hash_algorithm)
    else:
        raise ValueError(f"Unsupported observe method '{method}'")
    # timezone = pytz.timezone("Australia/Melbourne")
    # modified_time = datetime.datetime.fromtimestamp(stats.st_mtime, tz=timezone)
