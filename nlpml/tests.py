import unittest
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
streamhandler = logging.StreamHandler(stream=sys.stderr)
formatter = logging.Formatter(
                '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
streamhandler.setFormatter(formatter)
logger.addHandler(streamhandler)

# add file handler to gatelfdata and our own loggers
#filehandler = logging.FileHandler("test_api.log")
#logger1 = logging.getLogger("gatelfdata")
#logger1.setLevel(logging.INFO)
#logger1.addHandler(filehandler)
#logger.addHandler(filehandler)

from processingresources import PrCallFunction, ProcessingResource


def putinlist(item):
    ret = []
    ret.append(item)
    return ret


def plus1(x):
    return x + 1


def times3(x):
    return x * 3


class TestProcessingResources1(unittest.TestCase):

    def test_prcallfunction(self):
        pr = PrCallFunction(putinlist)

        item = "something"
        ret = pr(item)
        assert ret == [item]

        assert pr.supports_multiprocessing() == True
        assert ProcessingResource.supports_multiprocessing(pr) == True

        pipeline = []
        assert ProcessingResource.supports_multiprocessing(pipeline) == True
        pipeline.append(pr)
        assert ProcessingResource.supports_multiprocessing(pipeline) == True

class TestProcessor1(unittest.TestCase):

    def test_serial1(self):
        from processor import SequenceProcessor
        from destination import SdList
        source = [1, 2, 3, 4, 5]
        results = []
        dest1 = SdList(results)
        pr1 = PrCallFunction(plus1)
        pr2 = PrCallFunction(times3)
        pipeline = [pr1, pr2]
        proc = SequenceProcessor(source, nprocesses=1, pipeline=pipeline, destination=dest1)
        ret = proc.run()
        assert ret == (5, 0, False, False)
        assert dest1.get_data() == [6, 9, 12, 15, 18]

        results = []
        dest1 = SdList(results)
        proc = SequenceProcessor(source, nprocesses=1, pipeline=None, destination=dest1)
        ret = proc.run()
        assert ret == (5, 0, False, False)
        logger.info("Result is {}".format(results))
        assert dest1.get_data() == [1, 2, 3, 4, 5]

        results = []
        dest1 = SdList(results)
        proc = SequenceProcessor(source, nprocesses=3, pipeline=pipeline, destination=dest1)
        ret = proc.run()
        assert ret == (5, 0, False, False)
        logger.info("Result is {}".format(results))
        assert ret == (5, 0, False, False)
        logger.info("destination get_data is {}".format(dest1.get_data()))
        assert dest1.get_data() == [6, 9, 12, 15, 18]

