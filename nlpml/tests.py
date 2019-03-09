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
