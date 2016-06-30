"""Utilities for pickling"""


def pickle_instance_method(method):
    """Pickle class instance method."""
    func_name = method.__func__.__name__
    obj = method.__self__
    cls = method.__self__.__class__
    return unpickle_instance_method, (func_name, obj, cls)


def unpickle_instance_method(func_name, obj, cls):
    """Unpickle class instance method."""
    for cls in cls.mro():
        try:
            func = cls.__dict__[func_name]
        except KeyError:
            pass
        else:
            break
    return func.__get__(obj, cls)
