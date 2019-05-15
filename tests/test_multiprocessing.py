#!/usr/bin/env python
'''
Some tests for the multiprocessing code.
'''


import unittest
from unittest import TestCase
from loguru import logger
import multiprocessing as mp

from nlpml.multidistrproc import Supervisor, ConsumerProcess


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


class TestMultiParts1(TestCase):

    def test_consumer_process1(self):
        # attempt to test the code in the same process
        mgr = mp.Manager()
        cflag = mgr.Value('l', 0)
        aflag = mgr.Value('l', 0)
        cqueue = mgr.Queue()
        cp = ConsumerProcess()

        def cfunc(x):
            logger.info("cfunc got {}".format(x))
        for x in range(5):
            cqueue.put(x)
        cinfo = {"maxerrors": 0, "maxqsize": 0}
        cp(cfunc, cqueue, cflag, aflag, cinfo, 0)


if __name__ == '__main__':

    unittest.main()
