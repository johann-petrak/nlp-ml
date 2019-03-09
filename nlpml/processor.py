#!/usr/bin/env python
'''
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
import json
from multiprocessing import Pool, Queue


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


class SequenceProcessor(Processor):
    """
    For processing items that come from a serial source or go to a serial source.
    Optionally can send data back to a serial destination.
    """

    def __init__(self, source, nprocesses=1, pipeline=None, destination=None, runtimeargs={}):
        """
        Process some serial access source and optionally send the processed items to
        a serial destination.
        :param source: a SerialSource object or anything else that is an iterator or generator.
        (an instance of collections.Iterator)
        :param pipeline: the processing pipeline, a single Pr or a list of Prs
        :param destination: a SerialDestination object
        :param runtimeargs: a dictionary of kwargs to pass on to the Prs
        """
        import collections
        if isinstance(source, collections.Iterator):
            self.source = source
        else:
            raise Exception("Source must be an Iterator")
        self.processes = nprocesses
        self.pipeline = pipeline
        self.destination = destination

    def run(self):
        if self.processes == 1:
            # just run everything directly in here, no multiprocessing
            for id, item in enumerate(self.source):
                item = run_pipeline_on(self.pipeline, item, id=id, pid=1)
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
            # before we start the workers, we start the source reader and if necessary
            # the destination writer
            # TODO!
            for i in range(self.nprocesses):
                # logger.info("Starting process {}".format(i))
                kw["pid"] = i
                tmpkw = kw.copy()
                # TODO: figure out how to run over the data depending on having a destination or not
                # the pipeline running could be a closure or an instance of a PipelineRunner class
                # initialized appropriately
                ret = pool.apply_async(pipeline_runner_seq, (self.pipeline), tmpkw)




class DatasetProcessor(Processor):
    """
    For processing items from a Dataset, where accessing each item is independent
    of any other item.
    """

    def __init__(self, dataset, destination=None):
        pass

    def run(self):
        pass


class SerialSource(ABC):
    """

    """

class SerialDestination(ABC):
    """
    Base class for all implementations of something that needs serial writing.
    """

    @abstractmethod
    def write(self, item, id=None):
        """
        Write the next item to the destination.
        :param item: the data to write
        :param id: the id of the item, usually an item index
        :return:
        """
        pass

    def close(self):
        """
        Close the destination if necessary, otherwise do nothing.
        :return:
        """
        pass

    def get_data(self):
        """
        Returns the data from the destination if this is an in-memory destination.
        For other kinds of destination, this could return None or whatever makes sense.
        :return:
        """
        return None

    @abstractmethod
    def size(self):
        """
        Returns the number of items written to that destination.
        :return: number of items
        """


class SdJsonLinesFile(SerialDestination):
    """
    A destination which writes each item as json to a line in a destination file.
    """

    def __init__(self, file, id_key=None):
        """
        Destination for writing items to a line of JSON each in a file.
        :param file: the file to write to.
        :param id_key: if not None, the key to use to store the id of the item in the json data.
        In that case the item must implement map-like assignment and the item will get modified
        before getting written.
        """
        self.file = file
        self.fh = open(file, "wt", encoding="utf8")
        self.id_key = id_key
        self.n = 0

    def write(self, item, id=None):
        if self.id_key:
            item[self.id_key] = id
        self.fh.write(json.dumps(item))
        self.n += 1
        self.fh.write("\n")

    def size(self):
        return self.n
