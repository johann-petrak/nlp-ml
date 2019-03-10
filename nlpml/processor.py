#!/usr/bin/env python
'''
Classes and functions for dealing with processing items with a pipeline where the processing
is optionally done in parallel using multiprocessing.
'''

# NOTES: a processor is something that processes items by running those items through a
# pipeline. Currently there are two ways for how this can be done:
# * the SequenceProcessor gets the items from one or more sequences and then has k processes
#   running those items through the pipeline in parallel. If the sequence processor is
#   configured to also have a sequential output process, then the processed items are sent
#   to a single process that runs a destination writer. Otherwise, the pipeline is
#   responsible to do whatever is needed with each processed item in a way that is mp-able.
#   Order is not preserved
# * the DatasetProcessor has every process in the pool process a subset of the items in
#   the dataset (item indices mod processnumber) by accessing those items and running
#   each item through the pipeline. NOTE: if the dataset is a ProcessingDataset, items
#   are first processed by the pipeline defined there!
#   NOTE: storing of data can be accomplished by having a caching dataset as the outermost
#   dataset and doing the processing inside that wrapper through a processing dataset.
#   However this could also use some Pr which essentially stores each item somewhere as
#   a file or otherwise in a parallelizable fashion.
#   Finally this processor can also have a serial destination writer component for putting
#   the items back into e.g. a file. Order is not preserved!

from abc import ABC, abstractmethod
from processingresources import ProcessingResource
from multiprocessing import Pool, Queue
import multiprocessing
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
streamhandler = logging.StreamHandler(stream=sys.stderr)
formatter = logging.Formatter(
                '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
streamhandler.setFormatter(formatter)
logger.addHandler(streamhandler)


class Processor(ABC):

    @abstractmethod
    def run(self):
        """
        Actually run the processor. This returns once all data has been processed.
        :return: a tuple: total number of items processed, items with an error
        """
        pass


def run_pipeline_on(pipeline, item, **kwargs):
    """
    Helper function for running the pipeline on the item, returns the processed item.
    :param pipeline: pipeline or Pr
    :param item: item to be processed
    :param kwargs: the keyword arguments to pass on to the Prs
    :return: processed item
    """
    if pipeline is None:
        return item
    if isinstance(pipeline, list):
        for pr in pipeline:
            item = run_pipeline_on(pr, item, kwargs)
    else:
        item = pipeline(item, kwargs)
    return item


def source_reader(iqueue, source, **kwargs):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    streamhandler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    streamhandler.setFormatter(formatter)
    logger.addHandler(streamhandler)
    have_error = False
    try:
        for id, item in enumerate(source):
            iqueue.put((id, item))
        if hasattr(source, "close") and callable(source.close):
            source.close()
    except Exception as ex:
        logger.error("Error in source_reader: {}".format(ex))
        have_error = True
    return have_error


def destination_writer(oqueue, destination, use_destination_tuple=False, **kwargs):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    streamhandler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    streamhandler.setFormatter(formatter)
    logger.addHandler(streamhandler)
    have_error = False
    try:
        for id, item in oqueue:
            if use_destination_tuple:
                destination.write((id, item))
            else:
                destination.write(item)
        if hasattr(destination, "close") and callable(destination.close):
            destination.close()
    except Exception as ex:
        logger.error("Error in destination_writer: {}".format(ex))
        have_error = True
    return have_error


class SequenceProcessor(Processor):
    """
    For processing items that come from a serial source or go to a serial source.
    Optionally can send data back to a serial destination.
    """

    def __init__(self, source, nprocesses=1, pipeline=None,
                 destination=None, use_destination_tuple=False, maxsize_iqueue=10,
                 maxsize_oqueue=10, runtimeargs={}):
        """
        Process some serial access source and optionally send the processed items to
        a serial destination.
        :param source: anything that can be iterated over or a SerialSource object
        (an instance of collections.Iterator). If the object has a "close" attribute that
        is callable, it is closed when the iterator is exhausted.
        :param nprocesses: number of processes to run in parallel. If 1, no multiprocessing code is used. If 0,
        uses as many processes as there are CPUs. If < 0, use as many processes as there are cpus, but at most
        abs(nprocesses).
        :param pipeline: the processing pipeline, a single Pr or a list of Prs
        :param destination: a SerialDestination object or anything that implements write and optionally close.
        :param use_destination_tuple: if True, send the tuple (id, item) to the write function of the destination
        instead of just item.
        :param maxsize_iqueue: the maximum number of items to put into the input queue before locking
        :param maxsize_oqueue: the maximum number of items to put into the output queue before locking
        :param runtimeargs: a dictionary of kwargs to add to the kwargs passed on to the Prs
        """
        import collections
        if isinstance(source, collections.Iterator):
            self.source = source
        else:
            raise Exception("Source must be a SerialSource or any Iterator")
        if nprocesses > 0:
            self.nprocesses = nprocesses
        elif nprocesses == 0:
            self.nprocesses = multiprocessing.cpu_count()
        else:
            self.nprocesses = min(multiprocessing.cpu_count(), abs(nprocesses))
        self.pipeline = pipeline
        if hasattr(destination, "write") and callable(destination.write):
            pass
        else:
            raise Exception("Destination must be a SerialDestination instance or implement write(item) and optional close()")
        self.destination = destination
        self.maxsize_iqeueue = maxsize_iqueue
        self.maxsize_oqeueue = maxsize_oqueue
        self.runtimeargs = runtimeargs
        self.use_destination_tuple = use_destination_tuple

    def _make_pipeline_runner(self, input_queue, output_queue=None):
        """
        Create a closure for retrieving items from a queue and running the pipipeline
        on them and optionally sending the result to another queue
        :return: function to be used for running the pipeline
        """
        def pipeline_runner_seq(pipeline, **kwargs):
            """
            The actual function to run the pipeline
            :param pipeline:
            :return:
            """
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            streamhandler = logging.StreamHandler(stream=sys.stderr)
            formatter = logging.Formatter(
                '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
            streamhandler.setFormatter(formatter)
            logger.addHandler(streamhandler)
            n_total = 0
            n_nok = 0
            for id, item in input_queue:
                n_total += 1
                try:
                    item = run_pipeline_on(pipeline, item, **kwargs)
                    if output_queue is not None:
                        output_queue.put((id, item))
                except Exception as ex:
                    logger.error("Error processing item {} in process {}: {}".format(id, kwargs["pid"], ex))
                    n_nok += 1
            return n_total, n_nok
        return pipeline_runner_seq()

    def run(self):
        """
        Actually runs the pipeline over all the data. Returns a tuple of number of items processed
        in total, and number of items that had some error.
        :return: a tuple, total number of items, items with error, if reader had errors, if writer had errors
        """
        n_total = 0
        n_nok = 0
        if self.processes == 1:
            # just run everything directly in here, no multiprocessing
            for id, item in enumerate(self.source):
                n_total += 1
                try:
                    item = run_pipeline_on(self.pipeline, item, id=id, pid=1)
                except Exception as ex:
                    logger.error("Error processing item {}: {}".format(id, ex))
                    n_nok += 1
                # now if there is a destination, pass it on to there
                if self.destination:
                    self.destination.write(item, id=id)
        else:
            # ok, do the actual multiprocessing
            # first, check if the pipeline contains any Pr which is single process only
            if not ProcessingResource.supports_multiprocessing(self.pipeline):
                raise Exception("Cannot run multiprocessing, pipeline contains single processing PR")
            # first set up a pool for the workers
            pool = Pool(self.nprocesses)
            kw = self.runtimeargs
            kw["nprocesses"] = self.nprocesses
            rets = []
            input_queue = Queue(maxsize=self.maxsize_iqeueue)
            reader_pool = Pool(1)
            reader_pool_ret = pool.apply_async(source_reader, (input_queue, self.source), **kw)
            if self.destination is not None:
                output_queue = Queue(maxsize=self.maxsize_oqeueue)
                writer_pool = Pool(1)
                writer_pool_ret = pool.apply_async(destination_writer, (output_queue, self.destination), **kw)
            pipeline_runner = self._make_pipeline_runner(input_queue, output_queue)
            for i in range(self.nprocesses):
                # logger.info("Starting process {}".format(i))
                kw["pid"] = i
                tmpkw = kw.copy()
                ret = pool.apply_async(pipeline_runner, (self.pipeline,), tmpkw)
                rets.append(ret)
            # now wait for the processes and pool to finish
            pool.close()
            pool.join()
            reader_pool.close()
            reader_pool.join()
            reader_error = reader_pool_ret.get()
            writer_error = False
            if self.destination:
                writer_pool.close()
                writer_pool.join()
                writer_error = writer_pool_ret.get()
            # actually get the total values for n_total and n_nok from the per-process results
            for r in ret:
                n_total += r[0].get()
                n_nok += r[1].get()
        return n_total, n_nok, reader_error, writer_error


class DatasetProcessor(Processor):
    """
    For processing items from a Dataset, where accessing each item is independent
    of any other item. Each item can optionally be sent to a serial destination.
    """

    def __init__(self, dataset, destination=None):
        pass

    def run(self):
        pass


