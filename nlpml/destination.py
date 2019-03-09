#!/usr/bin/env python
'''
'''

from abc import ABC, abstractmethod
import json

class SerialDestination(ABC):
    """
    Base class for all implementations of something that needs serial writing.
    """

    @abstractmethod
    def write(self, item, id=None):
        """
        Write the next item to the destination.
        :param item: the data to write
        :param id: the id of the item, usually an item index
        :return:
        """
        pass

    def close(self):
        """
        Close the destination if necessary, otherwise do nothing.
        :return:
        """
        pass

    def get_data(self):
        """
        Returns the data from the destination if this is an in-memory destination.
        For other kinds of destination, this could return None or whatever makes sense.
        :return:
        """
        return None

    @abstractmethod
    def size(self):
        """
        Returns the number of items written to that destination.
        :return: number of items
        """


class SdJsonLinesFile(SerialDestination):
    """
    A destination which writes each item as json to a line in a destination file.
    """

    def __init__(self, file, id_key=None):
        """
        Destination for writing items to a line of JSON each in a file.
        :param file: the file to write to.
        :param id_key: if not None, the key to use to store the id of the item in the json data.
        In that case the item must implement map-like assignment and the item will get modified
        before getting written.
        """
        self.file = file
        self.fh = open(file, "wt", encoding="utf8")
        self.id_key = id_key
        self.n = 0

    def write(self, item, id=None):
        if self.id_key:
            item[self.id_key] = id
        self.fh.write(json.dumps(item))
        self.n += 1
        self.fh.write("\n")

    def size(self):
        return self.n
