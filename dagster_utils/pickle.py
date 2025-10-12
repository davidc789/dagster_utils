# Internal pickle library that optionally uses cloudpickle

try:
    import cloudpickle as pickle
    from cloudpickle import dump, dumps, load, loads
except ImportError:
    from pickle import dump, dumps, load, loads
