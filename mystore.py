from arctic import Arctic


class Store(object):
    def __init__(self, connection_string=None):
        if connection_string is not None:
            self.connection_string = connection_string
        else:
            setting = "localhost"
            if setting is None:
                raise ValueError("")
            else:
                self.connection_string = setting
        self.store = Arctic(self.connection_string)


class Library(Store):
    def __init__(self, connection_string=None):
        super().__init__(connection_string=connection_string)

    def initialize_library(self, library_key):
        self.store.initialize_library(library_key)

    def get_or_initialize_library(self, library_key):
        if not self.store.library_exists(library_key):
            self.initialize_library(library_key)

        return self.store[library_key]
