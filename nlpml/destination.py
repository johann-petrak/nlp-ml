#!/usr/bin/env python
'''
Base class and example implementations for serial destinations. Anything that implements
write and optionally close can be used too.
A destination can get just the item or a tuple with id and item, depending on how the processor
was configured before running.
'''

from abc import ABC, abstractmethod
import json


class SerialDestination(ABC):
    """
    Base class for all implementations of something that needs serial writing.
    """

    def __init__(self):
        self.data = None

    @abstractmethod
    def write(self, item):
        """
        Write the next item to the destination.
        :param item: the data to write
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
        return self.data

    def set_data(self, data):
        """
        This can be used to set the data in the original instance if a copy of the instance was used
        in a different process to create the data.
        :param data: the data to set
        :return:
        """
        self.data = data

    def size(self):
        """
        Returns the number of items written to that destination.
        :return: number of items
        """


class SdJsonLinesFile(SerialDestination):
    """
    A destination which writes each item as json to a line in a destination file.
    """

    def __init__(self, file):
        """
        Destination for writing items to a line of JSON each in a file.
        :param file: the file to write to.
        """
        self.file = file
        self.fh = open(file, "wt", encoding="utf8")
        self.n = 0

    def write(self, item):
        self.fh.write(json.dumps(item))
        self.n += 1
        self.fh.write("\n")

    def size(self):
        return self.n


class SdList(SerialDestination):
    """
    A destination for lists
    """

    def __init__(self, thelist):
        if not isinstance(thelist, list):
            raise Exception("Must be a list")
        self.data = thelist

    def write(self, item):
        import sys
        self.data.append(item)

    def size(self):
        return len(self.n)


class SdMap(SerialDestination):
    """
    A destination for maps/dictionaries. For these, the destination needs to receive the tuple (id, item) from
    the processor!
    """

    def __init__(self, themap):
        if not isinstance(themap, map):
            raise Exception("Must be a map")
        self.data = themap

    def write(self, item):
        if not isinstance(item, tuple) or not len(item) == 2:
            raise Exception("write must get a tuple (id, item) instead of item!")
        self.data[item[0]] = item[1]

    def size(self):
        return len(self.data)

