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

# TODO: figure out how to implement the "stoponerror" behaviour for multiprocessing.
# E.g. the source iterator and the processing pool need to know when the destination processor
# encountered an error and stop. How to communicate this to them?
# OK, this should be possible by sharing a multiprocessing.Value between all of them.
# Also, instead of having a boolean for this, we could configure the maximum number of errors
# to allow: if this is exceeded, processing ends. This could be done by having one int Value
# per process and one of the processes regularly checking the sum of all those values.
# Then if the sum exceeds the limit, that process updates a shared boolean value which gets
# checked by all processes, if it is set, all of them terminate

# TODO: change the way how pipelines work: a pipeline is just a special PR, never a list.
# This can then be used to handle the problem of PRs calculating global stats:
# * if a PR calculates global stats, they have to return the data via get_data()
# * the PR has to implement merge_data(listofdata) to handle merging a list of results into a single one
#   The result of merging will get merged with the result in the instance where merge_data is called, but
#   in the usual setting, this instance will not have any result (because only copies were used in different processes)
# * after merge_result, the get_data call will return the merged result, but this is also returned by merge_result

# TODO: implement the dataset processor, the current version is based on the old sequence processor

from abc import ABC, abstractmethod
from processingresources import ProcessingResource
from multiprocessing import Pool, Queue
from destination import SerialDestination
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
            item = run_pipeline_on(pr, item, **kwargs)
    else:
        item = pipeline(item, **kwargs)
    return item


def source_reader(iqueue, source, **kwargs):
    logger = logging.getLogger(__name__+".source_reader")
    nprocesses = kwargs["nprocesses"]
    logger.debug("Called with queue={}, source={}, nprocesses={}, kwargs={}".format(iqueue, source, nprocesses, kwargs))
    have_error = False
    try:
        for id, item in enumerate(source):
            logger.debug("Writing to input queue id={}".format(id))
            iqueue.put((id, item))
        if hasattr(source, "close") and callable(source.close):
            source.close()
    except Exception as ex:
        logger.error("Error in source_reader: {}".format(ex))
        have_error = True
    # in order to signal that we are finished we send tuples with None, None over the queue, one for each
    # of the worker processes
    for i in range(nprocesses):
        logger.debug("Writing to input queue id={}".format(None))
        iqueue.put((None, None))
    return have_error


def destination_writer(oqueue, destination, use_destination_tuple=False, **kwargs):
    logger = logging.getLogger(__name__+".destination_writer")
    nprocesses = kwargs["nprocesses"]
    logger.debug("Called with queue={}, destination={}, nprocesses={}, kwargs={}".format(oqueue, destination, nprocesses, kwargs))
    have_error = False
    try:
        # Each of the processes in the pool is sending items, when they are out of items they send
        # a tuple with two Nils.
        # We have to retrieve all the nprocesses nil tuples in order to prevent the processes to potentially block
        # when writing, so we make sure we exit only after we have seen all of them
        n_none = 0
        while n_none < nprocesses:
            logger.debug("Reading from output queue")
            id, item = oqueue.get()
            logger.debug("Got from output queue: id={}".format(id))
            if id is not None:
                if use_destination_tuple:
                    destination.write((id, item))
                else:
                    destination.write(item)
            else:
                n_none += 1
        if hasattr(destination, "close") and callable(destination.close):
            destination.close()
    except Exception as ex:
        logger.error("Error in destination_writer: {}".format(ex))
        have_error = True
    in_memory_result = None
    if isinstance(destination, SerialDestination):
        in_memory_result = destination.get_data()
    return have_error, in_memory_result


class PipelineRunnerSeq:

    def __init__(self, input_queue, output_queue=None):
        self.input_queue = input_queue
        self.output_queue = output_queue

    def __call__(self, pipeline, **kwargs):
        logger = logging.getLogger(__name__ + ".PipelineRunnerSeq")
        logger.debug("Started PipelineRunnerSeq")
        n_total = 0
        n_nok = 0
        # if we get a tuple where id and item are None, we immediately end reading the
        # queue! Even if there is no data for us at all, we should always at least get that end of work signal!
        while True:
            logger.debug("Reading from input queue")
            id, item = self.input_queue.get()
            logger.debug("Got from input queue id={}".format(id))
            if id is None:
                break
            n_total += 1
            try:
                kwargs["id"] = id
                logger.debug("Running the pipeline on id {} item {}".format(id, item))
                item = run_pipeline_on(pipeline, item, **kwargs)
                logger.debug("Run pipeline item is now {}".format(item))
                if self.output_queue is not None:
                    logger.debug("Writing to output queue id {} item {}".format(id, item))
                    self.output_queue.put((id, item))
            except Exception as ex:
                logger.error("Error processing item {} in process {}: {}".format(id, kwargs["pid"], ex))
                n_nok += 1
        logger.debug("Writing to output queue None")
        self.output_queue.put((None, None))
        # TODO: we should return the results lists from the PRs that produce results here somehow
        return n_total, n_nok


class SequenceProcessor(Processor):
    """
    For processing items that come from a serial source or go to a serial source.
    Optionally can send data back to a serial destination.
    """

    def __init__(self, source, nprocesses=1, pipeline=None,
                 destination=None, use_destination_tuple=False, maxsize_iqueue=10,
                 maxsize_oqueue=10, runtimeargs={}, stoponerror=False):
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
        :param stoponerror: try to stop as soon as possible as soon as an error is encountered.
        NOTE: not yet implemented!!!
        """
        import collections
        if isinstance(source, collections.Iterator) or isinstance(source, collections.Iterable):
            self.source = source
        else:
            raise Exception("Source must be a SerialSource or any Iterator or Iterable")
        if nprocesses > 0:
            self.nprocesses = nprocesses
        elif nprocesses == 0:
            self.nprocesses = multiprocessing.cpu_count()
        else:
            self.nprocesses = min(multiprocessing.cpu_count(), abs(nprocesses))
        # if we use multiprocessing, check if the pipeline can be pickled!
        if self.nprocesses != 1:
            import pickle
            try:
                tmp = pickle.dumps(pipeline)
            except Exception as ex:
                raise Exception("Pipeline cannot be pickled, cannot use multiprocessing, error: {}".format(ex))
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

    def run(self):
        """
        Actually runs the pipeline over all the data. Returns a tuple of number of items processed
        in total, and number of items that had some error.
        :return: a tuple, total number of items, items with error, if reader had errors, if writer had errors
        """
        logger = logging.getLogger(__name__+".SequenceProcessor.run")
        n_total = 0
        n_nok = 0
        if self.nprocesses == 1:
            # just run everything directly in here, no multiprocessing
            for id, item in enumerate(self.source):
                n_total += 1
                try:
                    item = run_pipeline_on(self.pipeline, item, id=id, pid=1)
                except Exception as ex:
                    logger.error("Error processing item {}: {}".format(id, ex))
                    n_nok += 1
                # now if there is a destination, pass it on to there
                writer_error = False
                if self.destination:
                    # TODO: catch writer error!
                    if self.use_destination_tuple:
                        self.destination.write((id, item))
                    else:
                        self.destination.write(item)
            # TODO: instead of the for loop, use something that allows to trap reader error
            return n_total, n_nok, False, False
        else:
            # ok, do the actual multiprocessing
            # first, check if the pipeline contains any Pr which is single process only
            if not ProcessingResource.supports_multiprocessing(self.pipeline):
                raise Exception("Cannot run multiprocessing, pipeline contains single processing PR")
            # first set up a pool for the workers
            pool = Pool(self.nprocesses)
            kw = self.runtimeargs
            kw["nprocesses"] = self.nprocesses
            kw.update(self.runtimeargs)
            rets = []
            # TODO: should all queues come from the same manager instance or different ones??
            mgr = multiprocessing.Manager()
            input_queue = mgr.Queue(maxsize=self.maxsize_iqeueue)
            reader_pool = Pool(1)
            logger.debug("Running reader pool apply async")
            reader_pool_ret = pool.apply_async(source_reader, [input_queue, self.source], kw)
            output_queue = None
            if self.destination is not None:
                output_queue = mgr.Queue(maxsize=self.maxsize_oqeueue)
            #    writer_pool = Pool(1)
            #    logger.debug("Running writer pool apply async")
            #    writer_pool_ret = pool.apply_async(destination_writer, [output_queue, self.destination, self.nprocesses], kw)
            pipeline_runner = PipelineRunnerSeq(input_queue, output_queue)
            for i in range(self.nprocesses):
                # logger.info("Starting process {}".format(i))
                kw["pid"] = i
                tmpkw = kw.copy()
                logger.debug("Running worker pool process {} apply async".format(i))
                ret = pool.apply_async(pipeline_runner, (self.pipeline,), tmpkw)
                rets.append(ret)
            # now wait for the processes and pool to finish
            # TODO If we have a destination, we could actually do the processing for that one right here instead of
            # a separate process!
            if self.destination is not None:
                logger.info("Calling destination writer with {} {} {} {}".format(output_queue, self.destination, self.nprocesses, kw))
                destination_writer(output_queue, self.destination, **kw)

            pool.close()
            pool.join()
            reader_pool.close()
            reader_pool.join()
            reader_error = reader_pool_ret.get()
            writer_error = False
            #if self.destination:
            #    writer_pool.close()
            #    writer_pool.join()
            #    writer_error, d = writer_pool_ret.get()
            #    if isinstance(self.destination, SerialDestination) and d is not None:
            #        self.destination.set_data(d)

            for r in rets:
                rval = r.get()
                n_total += rval[0]
                n_nok += rval[1]
            return n_total, n_nok, reader_error, writer_error


class DatasetProcessor(Processor):
    """
    For processing items from a Dataset, where accessing each item is independent
    of any other item. Each item can optionally be sent to a serial destination.
    """

    def __init__(self, dataset, destination=None, nprocesses=1, pipeline=None,
                 use_destination_tuple=False, maxsize_oqueue=10, runtimeargs={}):
        """
        Process a dataset with the pipeline. Note that the dataset could already wrap a ProcessDataset
        with its own pipeline. The pipeline specified here can be left as None to not do any
        processing at all.
        :param nprocesses: number of processes to run in parallel. If 1, no multiprocessing code is used. If 0,
        uses as many processes as there are CPUs. If < 0, use as many processes as there are cpus, but at most
        abs(nprocesses).
        :param pipeline: the processing pipeline, a single Pr or a list of Prs
        :param destination: a SerialDestination object or anything that implements write and optionally close.
        :param use_destination_tuple: if True, send the tuple (id, item) to the write function of the destination
        instead of just item.
        :param maxsize_oqueue: the maximum number of items to put into the output queue before locking
        :param runtimeargs: a dictionary of kwargs to add to the kwargs passed on to the Prs
        """
        self.dataset = dataset
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
        self.maxsize_oqeueue = maxsize_oqueue
        self.runtimeargs = runtimeargs
        self.use_destination_tuple = use_destination_tuple

    def _make_pipeline_runner(self, dataset, output_queue=None):
        """
        Create a closure for retrieving a subset of items from a dataset and running the pipipeline
        on them and optionally sending the result to another queue. The subset of items for each runner
        is the set whre the item indexes are modulo processid.
        :return: function to be used for running the pipeline
        """
        def pipeline_runner_dataset(pipeline, **kwargs):
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
            # iterate over just the items in the dataset we are interested in, depending on pid
            pid = kwargs["pid"]
            nprocesses = kwargs["nprocesses"]
            size = self.dataset.size()
            for id in range(pid, size, nprocesses):
                item = dataset[id]
                n_total += 1
                try:
                    kwargs["id"] = id
                    item = run_pipeline_on(pipeline, item, **kwargs)
                    if output_queue is not None:
                        output_queue.put((id, item))
                except Exception as ex:
                    logger.error("Error processing item {} in process {}: {}".format(id, kwargs["pid"], ex))
                    n_nok += 1
            return n_total, n_nok
        return pipeline_runner_dataset

    def run(self):
        """
        Actually runs the pipeline over the dataset. Returns a tuple of number of items processed
        in total, and number of items that had some error.
        :return: a tuple, total number of items, items with error, if writer had errors (or None if no destination)
        """
        n_total = 0
        n_nok = 0
        if self.processes == 1:
            # just run everything directly in here, no multiprocessing
            for id, item in enumerate(self.dataset):
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
            # first, check if the pipeline contains any Pr which is single process only
            if not ProcessingResource.supports_multiprocessing(self.pipeline):
                raise Exception("Cannot run multiprocessing, pipeline contains single processing PR")
            # first set up a pool for the workers
            pool = Pool(self.nprocesses)
            kw = self.runtimeargs
            kw["nprocesses"] = self.nprocesses
            kw.update(self.runtimeargs)
            rets = []
            output_queue = None
            if self.destination is not None:
                output_queue = multiprocessing.Manager().Queue(maxsize=self.maxsize_oqeueue)
                writer_pool = Pool(1)
                writer_pool_ret = pool.apply_async(destination_writer, (output_queue, self.destination), kw)
            pipeline_runner = self._make_pipeline_runner(self.dataset, output_queue)
            for i in range(self.nprocesses):
                # logger.info("Starting process {}".format(i))
                kw["pid"] = i
                tmpkw = kw.copy()
                ret = pool.apply_async(pipeline_runner, (self.pipeline,), tmpkw)
                rets.append(ret)
            # now wait for the processes and pool to finish
            pool.close()
            pool.join()
            writer_error = False
            if self.destination:
                writer_pool.close()
                writer_pool.join()
                writer_error = writer_pool_ret.get()
            # actually get the total values for n_total and n_nok from the per-process results
            for r in rets:
                rval = r.get()
                n_total += rval[0]
                n_nok += rval[1]
        return n_total, n_nok, writer_error


