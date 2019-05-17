#!/usr/bin/env python
'''
Some tests for the multiprocessing code.
'''


import unittest
from unittest import TestCase
from loguru import logger
import multiprocessing as mp
from time import sleep

from nlpml.multidistrproc import Supervisor, ConsumerProcess, ConsumerInfo, WorkerInfo, WorkerProcess, \
    ProducerInfo, ProducerProcess, get_all_from_queue


class TestLocal1(TestCase):

    def test1simple(self):
        indata = list(range(20))

        def myworker1(x):
            logger.info("myworker1 called with {}".format(x))
            sleep(0.7)
            return [x]

        shared_sum1 = mp.Value('l', 0)
        def myconsumer1(x):
            logger.info("myconsumer1 called with {}".format(x))
            with shared_sum1.get_lock():
                shared_sum1.value += x
        shared_sum2 = mp.Value('l', 0)
        def myconsumer2(x):
            logger.info("myconsumer2 called with {}".format(x))
            with shared_sum2.get_lock():
                shared_sum2.value += x

        sv = Supervisor()
        sv.add_producer(indata)
        sv.add_worker(myworker1, listify=False, nproc=3)
        sv.add_consumer(myconsumer1)
        sv.add_consumer(myconsumer2)
        ret = sv.run()
        logger.info("Local MP run finished, ret={}".format(ret))
        logger.info("Local MP run finished, shared_sum1={}".format(shared_sum1.value))
        logger.info("Local MP run finished, shared_sum2={}".format(shared_sum2.value))
        target = sum(indata)
        self.assertEqual(shared_sum1.value + shared_sum2.value, target)


if __name__ == '__main__':

    unittest.main()
