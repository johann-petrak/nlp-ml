#!/usr/bin/env python
'''
Base class and examples for serial sources. Not much needed because any iterable thing can be used
as a SerialSource.
'''

from abc import ABC, abstractmethod


class SerialSource(ABC):
    """
    A serial source needs to be iterable and optionally implement close().
    """
    @abstractmethod
    def __next__(self):
        pass

    def close(self):
        pass

