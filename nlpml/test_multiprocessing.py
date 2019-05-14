#!/usr/bin/env python
'''
Some tests for the multiprocessing code.
'''


import unittest
from unittest import TestCase
from loguru import logger
from .multidistrproc import Supervisor


class TestSerial1(TestCase):

    def test1verysimple(self):
        l = []
        indata = [1, 2, 3]
        target = [1, 2, 2, 4, 3, 6]

        def myworker1(x):
            logger.info("myworker1 called with {}".format(x))
            return [x, x*2]

        def myconsumer1(x):
            logger.info("myconsumer1 called with {}".format(x))
            l.append(x)

        sv = Supervisor()
        sv.add_producer(indata)
        sv.add_worker(myworker1, listify=False)
        sv.add_consumer(myconsumer1)
        sv.run()
        logger.info("Got l={}".format(l))
        self.assertEqual(l, target)


if __name__ == '__main__':

    unittest.main()
