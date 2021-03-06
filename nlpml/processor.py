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

# Handling end of job and errors in multiprocessing mode:
# * for the sequential processor, the source reader sends as many tuples with id=None as there are workers.
#   Each worker immendiately terminates if it gets such a tuple on the queue
# * If there is a destination, a worker that terminates sends a id=None tuple to the destination
# * the destination counts the number of id=None tuples it gets and terminates if it is equal to the number of workers
# * If the destination catches an error, the abortflag shared value is set to 1. All workers check that value and
#   immediately terminate without sending anything to the destination queue.
#   Also the source reader checks that flag and returns without sending the None tuple
# * If a worker has to terminate because of maximum errors or unrecoverable error it sets the abort flag
#   Again the reader and all other worker processes should terminate without sending anything.
#   In addition the destination, if we have one, also checks that flag and immediately returns

# TODO: figure out how to implement the "stoponerror" behaviour for multiprocessing.
# E.g. the source iterator and the processing pool need to know when the destination processor
# encountered an error and stop. How to communicate this to them?
# OK, this should be possible by sharing a multiprocessing.Value between all of them.
# Also, instead of having a boolean for this, we could configure the maximum number of errors
# to allow: if this is exceeded, processing ends. This could be done by having one int Value
# per process and one of the processes regularly checking the sum of all those values.
# Then if the sum exceeds the limit, that process updates a shared boolean value which gets
# checked by all processes, if it is set, all of them terminate
# First approach: stop on k errors per process, do not try to sum the errors up over processes!
# Every process monitors its errors and sets the global flag, every process monitors the flag
# and stops as soon as the flag is set.

# TODO: change the way how pipelines work: a pipeline is just a special PR, never a list.
# This can then be used to handle the problem of PRs calculating global stats:
# * if a PR calculates global stats, they have to return the data via get_data()
# * the PR has to implement merge_data(listofdata) to handle merging a list of results into a single one
#   The result of merging will get merged with the result in the instance where merge_data is called, but
#   in the usual setting, this instance will not have any result (because only copies were used in different processes)
# * after merge_result, the get_data call will return the merged result, but this is also returned by merge_result

# TODO: implement the dataset processor, the current version is based on the old sequence processor

from abc import ABC, abstractmethod
import multiprocessing as mp
from multiprocessing import Pool, Value
import logging
import sys
from nlpml.processingresources import ProcessingResource

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
    """
    Reads items from the source and sends them for processing via the iqueue (which is then read by the
    actual worker processes). If we get an error while reading, this method terminates in the same way
    as if end of source input would have been reached, but returns True.
    :param iqueue: the queue to use to send the items to
    :param source: the source to iterate over
    :param kwargs: the kwargs set by the caller, this HAS to have the nprocesses arg set!
    :return: False if no error, True if error encountered
    """
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
    """
    Reads items from the oqueue and writes them to the destination. If we get an error during writing,
    we pass the exception on to the caller which should abort the worker processes immediately.
    :param oqueue: the queue that contains the items from the worker processes
    :param destination: where to write the items to
    :param use_destination_tuple: if True, writes the tuple (id, item) instead of just item
    :param kwargs: the keyword arguments set by the caller this HAS to inclue nprocesses
    :return:
    """
    logger = logging.getLogger(__name__+".destination_writer")
    nprocesses = kwargs["nprocesses"]
    logger.debug("Called with queue={}, destination={}, nprocesses={}, kwargs={}".format(oqueue, destination, nprocesses, kwargs))
    have_error = False
    # NOTE: we do NOT catch any exceptions here so they bubble up to the caller!

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


class PipelineRunnerSeq:

    def __init__(self, input_queue, output_queue=None):
        self.input_queue = input_queue
        self.output_queue = output_queue

    def __call__(self, pipeline, **kwargs):
        """
        This is what gets executed by each worker of the SequentialProcessor. We read from the
        input queue the items from the source and if there is an output queue, we send the processed
        item there. After finishing, we send a special tuple with id None to the destination, if we have one.
        This constantly checks the abortflag which will be set if there is an error in the destination writer.
        If the flag indicates an error, we immediately return (without writing the special tuple).
        :param pipeline: the Pr to call for each item
        :param kwargs: the kwargs set by the caller, must include pid, maxerrors and abortflag
        :return:
        """
        pid = kwargs["pid"]
        logger = logging.getLogger(__name__ + ".PipelineRunnerSeq."+str(pid))
        logger.debug("Started PipelineRunnerSeq")
        n_total = 0
        n_nok = 0
        abortflag = kwargs["abortflag"]
        maxerrors = kwargs["maxerrors"]
        # if we get a tuple where id and item are None, we immediately end reading the
        # queue! Even if there is no data for us at all, we should always at least get that end of work signal!
        while True:
            logger.debug("Reading from input queue")
            if abortflag.value != 0:
                logger.info("Abort flag set, aborting worker...")
                return n_total, n_nok
            id, item = self.input_queue.get()
            logger.debug("Got from input queue id={}".format(id))
            if id is None:
                break
            n_total += 1
            try:
                kwargs["id"] = id
                logger.debug("Running the pipeline on id {} item {}".format(id, item))
                if pipeline is not None:
                    item = run_pipeline_on(pipeline, item, **kwargs)
                logger.debug("Run pipeline item is now {}".format(item))
                if self.output_queue is not None:
                    logger.debug("Writing to output queue id {} item {}".format(id, item))
                    self.output_queue.put((id, item))
            except Exception as ex:
                logger.error("Error processing item {} in process {}: {}".format(id, kwargs["pid"], ex))
                n_nok += 1
                if n_nok > maxerrors:
                    abortflag.value = 1

        logger.debug("Writing to output queue None")
        self.output_queue.put((None, None))
        # TODO: we should return the results lists from the PRs that produce results here somehow
        return n_total, n_nok


class PipelineRunnerDataset:

    def __init__(self, dataset, output_queue=None):
        self.dataset = dataset
        self.output_queue = output_queue

    def __call__(self, pipeline, **kwargs):
        pid = kwargs["pid"]
        logger = logging.getLogger(__name__ + ".PipelineRunnerDataset."+str(pid))
        logger.debug("Started PipelineRunnerDataset")
        n_total = 0
        n_nok = 0

        # iterate over just the items in the dataset we are interested in, depending on pid
        nprocesses = kwargs["nprocesses"]
        size = len(self.dataset)
        for id in range(pid, size, nprocesses):
            logger.debug("Getting dataset item id={}".format(id))
            item = self.dataset[id]
            logger.debug("Got item id={}".format(id))
            n_total += 1
            try:
                kwargs["id"] = id
                logger.debug("Running the pipeline on id {} item {}".format(id, item))
                if pipeline is not None:
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
        return n_total, n_nok


class SequenceProcessor(Processor):
    """
    For processing items that come from a serial source or go to a serial source.
    Optionally can send data back to a serial destination.
    """

    def __init__(self, source, nprocesses=1, pipeline=None,
                 destination=None, use_destination_tuple=False, maxsize_iqueue=10,
                 maxsize_oqueue=10, runtimeargs={}, maxerrors=0):
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
        :param maxerrors: maximum number of continuable errors to allow per process before everything gets stopped.
        Note: errors in the reader or writer always terminate the process.
        """
        import collections
        if isinstance(source, collections.Iterator) or isinstance(source, collections.Iterable):
            self.source = source
        else:
            raise Exception("Source must be a SerialSource or any Iterator or Iterable")
        if nprocesses > 0:
            self.nprocesses = nprocesses
        elif nprocesses == 0:
            self.nprocesses = mp.cpu_count()
        else:
            self.nprocesses = min(mp.cpu_count(), abs(nprocesses))
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
        self.maxerrors = maxerrors

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
            # NOTE: if the source throws an error, this will bubble up and terminate the method so we do not
            # need to trap the error here!
            for id, item in enumerate(self.source):
                n_total += 1
                haveerror = False
                try:
                    if self.pipeline is not None:
                        item = run_pipeline_on(self.pipeline, item, id=id, pid=1)
                except Exception as ex:
                    logger.error("Error processing item {}: {}".format(id, ex))
                    n_nok += 1
                    if n_nok > self.maxerrors:
                        raise Exception("Maximum number of errors ({}) reached, aborting".format(n_nok))
                    haveerror = True
                if not haveerror and self.destination:
                    try:
                        if self.use_destination_tuple:
                            self.destination.write((id, item))
                        else:
                            self.destination.write(item)
                    except Exception as ex:
                        raise Exception("Error trying to write to the destination: {}".format(ex))
            # the reader and writer errors are thrown earlier, so if we arrive here, there were none
            return n_total, n_nok, False, False
        else:
            # ok, do the actual multiprocessing
            # first, check if the pipeline contains any Pr which is single process only
            if not ProcessingResource.supports_multiprocessing(self.pipeline):
                raise Exception("Cannot run multiprocessing, pipeline contains single processing PR")
            # shared value to indicate that we need to abort the workers if set to something other than zero
            abortflag = Value('i', 0)
            # set up a pool for the workers
            pool = Pool(self.nprocesses)
            kw = self.runtimeargs
            kw["nprocesses"] = self.nprocesses
            kw.update(self.runtimeargs)
            kw["abortflag"] = abortflag
            kw["maxerrors"] = self.maxerrors
            rets = []
            mgr = mp.Manager()
            input_queue = mgr.Queue(maxsize=self.maxsize_iqeueue)
            reader_pool = Pool(1)
            logger.debug("Running reader pool apply async")
            reader_pool_ret = pool.apply_async(source_reader, [input_queue, self.source], kw)
            output_queue = None
            if self.destination is not None:
                output_queue = mgr.Queue(maxsize=self.maxsize_oqeueue)
            pipeline_runner = PipelineRunnerSeq(input_queue, output_queue)
            for i in range(self.nprocesses):
                kw["pid"] = i
                tmpkw = kw.copy()
                logger.debug("Running worker pool process {} apply async".format(i))
                ret = pool.apply_async(pipeline_runner, (self.pipeline,), tmpkw)
                rets.append(ret)
            writer_error = False
            # handle writing the results to the destination in our own process here, that way, in-memory
            # destinations are created in our own process!
            if self.destination is not None:
                logger.debug("Calling destination writer with {} {} {} {}".format(output_queue, self.destination, self.nprocesses, kw))
                writer_error = False
                # we trap writer errors so we can try to wait for and finish the other processes!
                try:
                    destination_writer(output_queue, self.destination, **kw)
                except Exception as ex:
                    writer_error = True
                    abortflag.value = 1
                    logger.error("Encountered writer error: {}".format(ex))
            # now wait for the processes and pool to finish
            pool.close()
            pool.join()
            reader_pool.close()
            reader_pool.join()
            reader_error = reader_pool_ret.get()
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
        # logging.getLogger(__name__).setLevel(logging.DEBUG)
        self.dataset = dataset
        if nprocesses > 0:
            self.nprocesses = nprocesses
        elif nprocesses == 0:
            self.nprocesses = mp.cpu_count()
        else:
            self.nprocesses = min(mp.cpu_count(), abs(nprocesses))
        # if we use multiprocessing, check if the pipeline can be pickled!
        if pipeline is not None and self.nprocesses != 1:
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
        self.maxsize_oqeueue = maxsize_oqueue
        self.runtimeargs = runtimeargs
        self.use_destination_tuple = use_destination_tuple

    def run(self):
        """
        Actually runs the pipeline over the dataset. Returns a tuple of number of items processed
        in total, and number of items that had some error.
        :return: a tuple, total number of items, items with error, if writer had errors (or None if no destination)
        """
        logger = logging.getLogger(__name__+".DatasetProcessor.run")
        n_total = 0
        n_nok = 0
        if self.nprocesses == 1:
            # just run everything directly in here, no multiprocessing
            for id, item in enumerate(self.dataset):
                n_total += 1
                try:
                    if self.pipeline is not None:
                        item = run_pipeline_on(self.pipeline, item, id=id, pid=1)
                except Exception as ex:
                    logger.error("Error processing item {}: {}".format(id, ex))
                    n_nok += 1
                # now if there is a destination, pass it on to there
                if self.destination:
                    # TODO: catch writer error!
                    if self.use_destination_tuple:
                        self.destination.write((id, item))
                    else:
                        self.destination.write(item)
            # TODO: instead of the for loop, use something that allows to trap reader error
            return n_total, n_nok, False
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
            mgr = mp.Manager()
            if self.destination is not None:
                output_queue = mgr.Queue(maxsize=self.maxsize_oqeueue)
            pipeline_runner = PipelineRunnerDataset(self.dataset, output_queue)
            for i in range(self.nprocesses):
                # logger.info("Starting process {}".format(i))
                kw["pid"] = i
                tmpkw = kw.copy()
                logger.debug("Running worker pool process {} apply async".format(i))
                ret = pool.apply_async(pipeline_runner, (self.pipeline,), tmpkw)
                rets.append(ret)
            writer_error = False
            if self.destination is not None:
                logger.debug("Calling destination writer with {} {} {} {}".format(output_queue, self.destination, self.nprocesses, kw))
                writer_error = destination_writer(output_queue, self.destination, **kw)
            pool.close()
            pool.join()
            for r in rets:
                rval = r.get()
                n_total += rval[0]
                n_nok += rval[1]
            return n_total, n_nok, writer_error
