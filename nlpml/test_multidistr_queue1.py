#!/usr/bin/env python
'''
Some tests for the multiprocessing code.
'''


import unittest
from unittest import TestCase
from loguru import logger
import multiprocessing as mp
from time import sleep

from . multidistr_queue import Supervisor, ConsumerProcess, ConsumerInfo, WorkerInfo, WorkerProcess, \
    ProducerInfo, ProducerProcess, get_all_from_queue


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
        ret = sv.run()
        logger.info("Got l={}".format(l))
        self.assertEqual(l, target)
        logger.info("Running sequentially returned: {}".format(ret))


class TestMultiParts1(TestCase):

    def test_consumer_process1(self):
        # attempt to test the code in the same process
        cflag = mp.Value('l', 0)
        aflag = mp.Value('l', 0)
        cqueue = mp.Queue()

        def cfunc(x):
            logger.info("cfunc got {}".format(x))

        ci = ConsumerInfo()
        ci.consumer = cfunc
        ci.maxerrors = 0
        ci.maxqsize = 0
        ci.fflag = cflag
        ci.aflag = aflag
        ci.empty_retry = 0.5  # half second per retry
        ci.empty_max = 2    # retry twice
        ci.iqueue = cqueue

        cp = ConsumerProcess(ci)

        for x in range(5):
            cqueue.put(x)
        cp(0)

    def test_worker_process1(self):
        fflag = mp.Value('l', 0)
        aflag = mp.Value('l', 0)
        iqueue = mp.Queue()
        oqueue = mp.Queue()
        rqueue = mp.Queue()

        def wfunc(x):
            logger.info("wfunc go {}".format(x))
            return [2*x]

        wi = WorkerInfo()
        wi.worker = wfunc
        wi.fflag = fflag
        wi.aflag = aflag
        wi.iqueue = iqueue
        wi.oqueue = oqueue
        wi.rqueue = rqueue
        wi.empty_max = 2
        wi.empty_retry = 0.5

        wp = WorkerProcess(wi)
        for x in range(5):
            iqueue.put(x)
        # if we set the finish flag before actually invoking the worker process, it will finish as soon as the
        # queue is empty
        fflag.value = 1
        wp(0)
        ret = get_all_from_queue(oqueue)
        logger.info("Result worker queue contained: {}".format(ret))
        self.assertEqual(ret, [0, 2, 4, 6, 8])

    def test_producer_process1(self):
        aflag = mp.Value('l', 0)
        fflag = mp.Value('l', 0)
        oqueue = mp.Queue()

        piter = [0, 1, 2, 3, 4, 5]
        pi = ProducerInfo()
        pi.producer = piter
        pi.oqueue = oqueue
        pi.aflag = aflag
        pi.fflag = fflag
        pp = ProducerProcess(pi)
        pp(0)
        ret = get_all_from_queue(oqueue)
        logger.info("Result producer queue contained: {}".format(ret))
        self.assertEqual(ret, [0, 1, 2, 3, 4, 5])


if __name__ == '__main__':

    unittest.main()
