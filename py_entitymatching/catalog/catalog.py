# coding=utf-8
import logging

logger = logging.getLogger(__name__)


class Singleton(object):
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.
    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.
    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.
    Limitations: The decorated class cannot be inherited from.
    """

    def __init__(self, decorated):
        self._decorated = decorated

    # noinspection PyPep8Naming
    def Instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.
        """
        try:
            return self._instance
        except AttributeError:
            # noinspection PyAttributeOutsideInit
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


@Singleton
class Catalog(object):
    """
    Class to store and retrieve catalog information
    """

    def __init__(self):
        self.properties_catalog = {}

    def init_properties_for_id(self, obj_id):
        self.properties_catalog[obj_id] = {}
        return True

    def init_properties(self, df):
        df_id = id(df)
        self.init_properties_for_id(df_id)

    def get_property_for_id(self, obj_id, name):
        d = self.properties_catalog[obj_id]
        return d[name]

    def get_property(self, df, name):
        df_id = id(df)
        return self.get_property_for_id(df_id, name)

    def set_property_for_id(self, obj_id, name, value):
        d = self.properties_catalog[obj_id]
        d[name] = value
        self.properties_catalog[obj_id] = d
        return True

    def set_property(self, df, name, value):
        df_id = id(df)
        return self.set_property_for_id(df_id, name, value)

    def get_all_properties_for_id(self, obj_id):
        d = self.properties_catalog[obj_id]
        return d

    def get_all_properties(self, df):
        df_id = id(df)
        return self.get_all_properties_for_id(df_id)

    def del_property_for_id(self, obj_id, name):
        d = self.properties_catalog[obj_id]
        del d[name]
        self.properties_catalog[obj_id] = d
        return True

    def del_property(self, df, name):
        df_id = id(df)
        return self.del_property_for_id(df_id, name)

    def del_all_properties_for_id(self, obj_id):
        del self.properties_catalog[obj_id]
        return True

    def del_all_properties(self, df):
        df_id = id(df)
        return self.del_all_properties_for_id(df_id)

    def get_catalog(self):
        return self.properties_catalog

    def del_catalog(self):
        self.properties_catalog = {}
        return True

    def get_catalog_len(self):
        return len(self.properties_catalog)

    def is_catalog_empty(self):
        return len(self.properties_catalog) == 0

    def is_df_info_present_in_catalog(self, df):
        return id(df) in self.properties_catalog

    def is_property_present_for_id(self, obj_id, name):
        d = self.properties_catalog[obj_id]
        return name in d

    def is_property_present_for_df(self, df, name):
        df_id = id(df)
        return self.is_property_present_for_id(df_id, name)
