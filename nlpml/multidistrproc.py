#!/usr/bin/env python
'''
Support for running jobs in multiprocessing or distributed processing form.
'''


# TODO: the following stuff needs checking
# * type annotations for Queue and Value may need to consider what we get from the package versus manager (proxies)

# TODO: implement distributed processing:
# * for this each component (producer, worker, consumer) gets a non-empty machine setting
# * on that machine we need to have a controller process running which in turn has a syncmanager process started.
# * the controller process reads a special queue to get instructions for who to start a bunch of worker, consumer,
#   producer processes
# * But the difference is that we probably cannot send over the queue and values directly, instead, each of the
#   remote controllers must get these from our own central sync manager


import os
import collections
from typing import List, Tuple, Union, Callable, Dict, Optional
import multiprocessing as mp
from multiprocessing import Queue, Value, Process
from collections.abc import Iterable, Iterator
import queue
from time import sleep
from collections import Counter

from loguru import logger


def get_all_from_queue(thequeue: mp.Queue):
    """
    Retrives everything on the queue until we see the queue empty (not waiting for any more after that)
    Return all elements as a list
    :param thequeue:
    :return:
    """
    ret = []
    while True:
        try:
            ret.append(thequeue.get_nowait())
        except queue.Empty:
            break
    return ret


def curprocessinfo() -> Tuple[int, int, str]:
    if hasattr(os, 'getppid'):
        ppid = os.getppid()
    else:
        ppid = None
    cp = mp.current_process()
    return cp.pid, ppid, cp.name


def queue_readanddiscard(thequeue: Queue) -> None:
    while True:
        try:
            thequeue.get_nowait()
        except queue.Empty:
            break


class ComponentInfo:
    """
    A ComponentInfo stores all the info relevant for one component (a producers, a worker etc) of the whole
    job. This info includes the list of active processes, the queues used and other data which may cause problems
    when serialising for sending to some other proces. This means that a ComponentInfo instance should never get
    passed to a process, but it can be used to initialise something that gets passed to the process, in our case,
    it can be used to initialise the corresponding ComponentProcess instances.

    Since the componentinfo also keeps the info about the started processes and used queues, it can be used to
    wait for the processing to finish using the join() method.
    """
    machine: Optional[Tuple[str, int]]  # None or a tuple (hostname/ip, port)
    nproc: int = 1       # number of processes
    aflag: Value      # abort flag
    fflag: Value      # finish flag
    # the defaults here are mainly to simplify the unit tests, in normal use these should get
    # set by the code creating the specific component infor for a specific purpose
    ntotal: Value = mp.Value('l', 0)     # total number of items tried to process/produce/consume
    nerror: Value = mp.Value('l', 0)     # number of items where we got an error (ntotal-nerror is number successful)
    maxerrors: int = 0   # maximum number of errors allowed in this component
    countevery: int = 100  # update shared counters every that many iterations
    procs: List[Process]  # list of processes running this component
    timeout: Optional[int] = None  # timeout, by default None to indicate that there is no timeout

    def join(self):
        """
        Wait for all the processes to complete. If a timeout is specified wait that long for the first process,
        then wait only very shortly (1 second) for the remaining processes. If a timeout is specified and
        exhausted, then all the processes which are still running are terminated.
        :param timeout:
        :return:
        """
        for i, p in enumerate(self.procs):
            if self.timeout is not None:
                if i == 0:
                    usetimeout = self.timeout
                else:
                    usetimeout = 1
            else:
                usetimeout = None
            logger.info("Trying to shut down process {} with timeout {}".format(p, usetimeout))
            p.join(timeout=usetimeout)
            if p.is_alive():
                logger.error("Process {} alive after join attempt, terminating".format(p))
                p.terminate()


class ConsumerInfo(ComponentInfo):
    consumer: Callable
    iqueue: Queue
    maxqsize: int
    empty_retry: int = 1
    empty_max: int = 30


class WorkerInfo(ComponentInfo):
    worker: Callable
    iqueue: Queue
    oqueue: Queue
    rqueue: Queue = None   # this one is optional
    maxqsize: int  # maximum capacity of input queue
    listify: bool = False
    empty_retry: int = 1
    empty_max: int = 30


class ProducerInfo(ComponentInfo):
    producer: Union[Iterator, Iterable]
    oqueue: Queue


class ComponentProcess:
    def __init__(self, info: ComponentInfo):
        self.aflag: Value = info.aflag
        self.fflag: Value = info.fflag
        self.shared_ntotal = info.ntotal
        self.shared_nerror = info.nerror
        self.maxerrors: int = info.maxerrors
        self.ntotal = 0
        self.nerror = 0
        self.countevery = info.countevery


class ConsumerProcess(ComponentProcess):
    """
    Class that represents a consumer process which in turn invokes the actual consumer callable.
    This class gets initialised before getting passed to each consumer process and then gets called
    once to get executed in the process. It should process all items it gets from the input queue and
    and then terminate.
    """
    def __init__(self, info: ConsumerInfo) -> None:
        super().__init__(info)
        self.consumer = info.consumer
        self.iqueue = info.iqueue
        self.empty_max = info.empty_max
        self.empty_retry = info.empty_retry

    def __call__(self, myid: int = 0) -> None:
        """
        Run the consumer process. This reads items from the input queue and passes them on to the consumer function.
        Normal termination happens when the cflag is one and the input queue is empty.
        If the input queue is empty otherwise, the process will check again after empty_retry seconds. If the input
        queue is still empty after empty_max retries, the process will abort. The process will also abort if more
        than maxerrors exception are thrown when processing some item.
        :param myid: the serial number of this process
        :return:
        """
        from loguru import logger
        pinfo = curprocessinfo()
        logger.info("Starting consumer {} process pid={} ppid={} name={}".format(myid, pinfo[0], pinfo[1], pinfo[2]))
        haderrors: int = 0
        hadretried: int = 0
        niterations: int = 0
        while True:
            niterations += 1
            if niterations % self.countevery == 0:
                # update the shared counters
                with self.shared_nerror.get_lock():
                    self.shared_nerror.value += self.nerror
                self.nerror = 0
                with self.shared_ntotal.get_lock():
                    self.shared_ntotal.value += self.ntotal
                self.ntotal = 0
            if self.aflag.value == 1:
                logger.error("Consumer {}, got abort flag, consuming rest of queue".format(myid))
                queue_readanddiscard(self.iqueue)
                logger.error("Consumer {} aborting".format(myid))
                break
            finishflag = self.fflag.value
            try:
                item = self.iqueue.get_nowait()
                hadretried = 0  # reset the empty queue attempts counter, since we got a non-empty queue!
                # we got an item, pass it on to the actual consumer function
                # catch any exception and depending on maxerrors abort early
                try:
                    self.ntotal += 1
                    logger.info("DEBUG: consumer process {} passing item {} to consumer".format(myid, item))
                    self.consumer(item)
                except Exception as ex:
                    # log the error
                    logger.exception("Caught exception in ConsumerProcess")
                    haderrors += 1
                    self.nerror += 1
                    # if we had more than maxerrors, terminate "immediately". To avoid problems this means we
                    # go on to read and discard elements from the queue until it is empty
                    if haderrors > self.maxerrors:
                        logger.error("Got more than {} errors for this consumer process, terminating".format(self.maxerrors))
                        self.aflag.value = 1
                        queue_readanddiscard(self.iqueue)
                        break
            except queue.Empty:
                # if the queue is empty, let us check if the finish flag was set before we did try to read the queue
                # if yes we should terminate, otherwise wait a second before checking again
                if finishflag == 1:
                    logger.info("Consumer {} found finish flag, exiting".format(myid))
                    break
                else:
                    hadretried += 1
                    if hadretried > self.empty_max:
                        self.aflag.value = 1
                        logger.error("Consumer queue empty for more than {} retries, aborting".format(self.empty_max))
                        break
                    sleep(self.empty_retry)
        # update the shared counters only when finishing to save locking overhead time
        with self.shared_nerror.get_lock():
            self.shared_nerror.value += self.nerror
        with self.shared_ntotal.get_lock():
            self.shared_ntotal.value += self.ntotal
        logger.info("Stopping consumer {} process pid={} ppid={} name={}".format(myid, pinfo[0], pinfo[1], pinfo[2]))


class WorkerProcess(ComponentProcess):
    """
    Class that represents a worker process which in turn invokes the actual worker callable.
    This class gets initialised before getting passed to each worker process and then gets called
    once to get executed in the process. It should process all items it gets from the input queue and
    and send the resulting processed items to the output queue (if there is one, otherwise do nothing)
    If the worker callable has the right method, it should also get the result and send it over the result queue.
    """
    def __init__(self, info: WorkerInfo) -> None:
        super().__init__(info)
        self.worker = info.worker
        self.iqueue = info.iqueue
        self.oqueue = info.oqueue
        self.rqueue = info.rqueue
        self.listify = info.listify
        self.empty_max = info.empty_max
        self.empty_retry = info.empty_retry

    def __call__(self, myid: int) -> None:
        from loguru import logger
        pinfo = curprocessinfo()
        logger.info("Starting worker {} process pid={} ppid={} name={}".format(myid, pinfo[0], pinfo[1], pinfo[2]))
        haderrors: int = 0
        hadretried: int = 0
        niterations: int = 0
        # for debugging mainly, so we can see how much work each of several parallel processes does
        process_ntotal = 0
        process_nerror = 0
        while True:
            niterations += 1
            logger.info("DEBUG: worker process {}/pid={} iteration {}".format(myid, pinfo[0], niterations))
            if niterations % self.countevery == 0:
                # update the shared counters
                with self.shared_nerror.get_lock():
                    self.shared_nerror.value += self.nerror
                self.nerror = 0
                with self.shared_ntotal.get_lock():
                    self.shared_ntotal.value += self.ntotal
                self.ntotal = 0
            if self.aflag.value == 1:
                logger.error("Worker {}, got abort flag, consuming rest of queue".format(myid))
                queue_readanddiscard(self.iqueue)
                logger.error("Worker {} aborting".format(myid))
                break
            finishflag = self.fflag.value
            try:
                item = self.iqueue.get_nowait()
                hadretried = 0  # reset the empty queue attempts counter, since we got a non-empty queue!
                # we got an item, pass it on to the actual consumer function
                # catch any exception and depending on maxerrors abort early
                try:
                    self.ntotal += 1
                    process_ntotal += 1
                    logger.info("DEBUG: worker process {}/pid={} passing item {} to worker".format(myid, pinfo[0], item))
                    ret = self.worker(item)
                    if self.listify:
                        ret = [ret]
                    for x in ret:
                        logger.info("DEBUG: worker process {}/pid={} putting result item {} on queue".format(myid, pinfo[0], item))
                        self.oqueue.put(x)
                except Exception as ex:
                    # log the error
                    logger.exception("Caught exception in WorkerProcess")
                    haderrors += 1
                    self.nerror += 1
                    process_nerror += 1
                    # if we had more than maxerrors, terminate "immediately". To avoid problems this means we
                    # go on to read and discard elements from the queue until it is empty
                    if haderrors > self.maxerrors:
                        logger.error("Got more than {} errors for this worker process, terminating".format(self.maxerrors))
                        self.aflag.value = 1
                        queue_readanddiscard(self.iqueue)
                        break
            except queue.Empty:
                logger.info(
                    "DEBUG: worker process {}/pid={} got empty queue".format(myid, pinfo[0]))
                # if the queue is empty, let us check if the finish flag was set before we did try to read the queue
                # if yes we should terminate, otherwise wait a second before checking again
                if finishflag == 1:
                    logger.info("Worker {} found finish flag, exiting".format(myid))
                    break
                else:
                    hadretried += 1
                    if hadretried > self.empty_max:
                        self.aflag.value = 1
                        logger.error("Queue empty for more than {} retries, aborting".format(self.empty_max))
                        break
                    sleep(self.empty_retry)
        # the result/state and if yes, put the result on the rqueue
        # if the worker has a method "result()" we call it and put whatever it returns on the result queue
        # If the worker represents a whole pipeline, then the result is really a (possibly nested) list of
        # the results of the leaf workers. The worker should also have a "combine_results()" method which
        # is then used to combine all those results
        if self.rqueue is not None and hasattr(self.worker, "result") and callable(self.result) and \
                hasattr("merge_results") and callable(self.worker.merge_results):
            try:
                result = self.worker.result()
            except Exception as ex:
                logger.error("Exception when trying to retrieve results for worker {}".format(self.myid))
                result = None
            self.rqueue.put(result)
        with self.shared_nerror.get_lock():
            self.shared_nerror.value += self.nerror
        with self.shared_ntotal.get_lock():
            self.shared_ntotal.value += self.ntotal
        logger.info("Stopping worker {} process pid={} ppid={} name={}, total={}, error={}".
                    format(myid, pinfo[0], pinfo[1], pinfo[2], process_ntotal, process_nerror))


class ProducerProcess(ComponentProcess):
    """
    Class that represents a producer process which in turn processes the actual producer iterator/iterable
    This class gets initialised before getting passed to each consumer process and then gets called
    once to get executed in the process. It should iterate over all items in the iterable/iterator and
    send those items off to the producer queue.

    NOTE: this process can block and hang forever if the elements which get put into the queue are not retrieved.
    We could work around this by using some long timeout and aborting if the queue is full even after waiting for
    that long, but for now, we assume that the workers and consumers will always empty the queue anyway.
    """
    def __init__(self, info: ProducerInfo) -> None:
        super().__init__(info)
        self.producer = info.producer
        self.oqueue = info.oqueue

    def __call__(self, myid: int) -> None:
        from loguru import logger
        pinfo = curprocessinfo()
        logger.info("Starting producer {} process pid={} ppid={} name={}".format(myid, pinfo[0], pinfo[1], pinfo[2]))
        if not isinstance(self.producer, Iterator):
            # We expect this to always work for now since we check the type when the producer gets added!
            producer_iter = iter(self.producer)
        else:
            producer_iter = self.producer
        # iterate "manually" so we can catch and count errors
        haderrors: int = 0
        niterations: int = 0
        while True:
            niterations += 1
            if niterations % self.countevery == 0:
                # update the shared counters
                with self.shared_nerror.get_lock():
                    self.shared_nerror.value += self.nerror
                self.nerror = 0
                with self.shared_ntotal.get_lock():
                    self.shared_ntotal.value += self.ntotal
                self.ntotal = 0
            if self.aflag.value == 1:
                logger.error("Producer {}, got abort flag, aborting".format(myid))
                break
            try:
                el = next(producer_iter)
                self.ntotal += 1
                logger.info("DEBUG: producer {} putting item {} on queue".format(myid, el))
                self.oqueue.put(el)
            except StopIteration:
                logger.info("Producer {} finished".format(myid))
                break
            except Exception as ex:
                logger.exception("Got an exception during producer:next")
                haderrors += 1
                self.nerror += 1
                self.ntotal += 1  # we do not count this one if we get an exception so count here
                if haderrors > self.maxerrors:
                    self.aflag.value = 1
                    logger.error("More than {} errors in producer {}, aborting".format(self.maxerrors, myid))
                    break
        with self.shared_nerror.get_lock():
            self.shared_nerror.value += self.nerror
        with self.shared_ntotal.get_lock():
            self.shared_ntotal.value += self.ntotal
        logger.info("Stopping producer {} process pid={} ppid={} name={}".format(myid, pinfo[0], pinfo[1], pinfo[2]))


class Supervisor:
    """
    This class handles everything necessary to run a pipeline of processing steps in parallel and maybe on
    different machines. It can be used to define how the processing should take place: how to produce data items
    (producer), how to process the items (worker) and optionally what to do with the processed items (consumer).
    An item can be anything, all the components (producer, workers, consumers) simply need to expect the same
    kind of items. A worker is any function that takes an item and produces a list with any number of items.
    If several workers are run in sequence, then each worker will process all the items in the list returned from the
    previous worker.
    """

    def __init__(self) -> None:
        # lists of producer/worker/consumer info dicts
        # keys initially are: code, nproc, machine, maxerrors
        # additional keys: proc (process object)
        self._producers: List[ProducerInfo] = []
        self._workers: List[WorkerInfo] = []
        self._consumers: List[ConsumerInfo] = []
        self.sequential: bool = True
        self.local: bool = True
        self.nproducers: int = 0
        self.nworkers: int = 0
        self.nconsumers: int = 0

    def add_producer(self,
                     producer: Union[Iterable, Iterator],
                     nproc: int = 1,
                     machine: Optional[Tuple[str, int]] = None,
                     maxerrors: int = 0) -> None:
        """
        Add a producer to the beginning of the processing pipeline. A producer somehow generates items to get processed
        by workers.
        NOTE: if more than one producer is added, they are run in parallel (so sequential processing is disabled)
        :param producer: something that is iterable, generating items
        :param nproc: how many producers to run in parallel, if more than one, producers should know how to make sure that
        no duplicate data is produced.
        :param machine: if None, run on the local host, otherwise the (host,port) specification of a computer to run
        the producer(s) on. TODO: config for how to do the remote running!
        :param maxerrors: how many exceptions to allow when an item is generated, if maxerrors is reached, the whole
        processing will abort
        :return:
        """
        if isinstance(producer, collections.Iterable) or isinstance(producer, collections.Iterable):
            pass
        else:
            raise Exception("producer must be an Iterator or Iterable")
        pi = ProducerInfo()
        pi.producer = producer
        pi.nproc = nproc
        pi.machine = machine
        pi.maxerrors = maxerrors
        self._producers.append(pi)
        self.nproducers = len(self._producers)
        if nproc > 1 or machine is not None or self.nproducers > 1:
            self.sequential = False
        if machine is not None:
            self.local = False

    def add_worker(self,
                   worker: Callable,
                   nproc: int = 1,
                   machine: Optional[Tuple[str, int]] = None,
                   maxerrors: int = 0,
                   maxqsize: int = 20,
                   listify: bool = False):
        """
        Add a worker as the next stage in the processing.
        A worker must be a callable that takes an item and returns a list of items. If a worker does not
        put its return value into a list, then listify can be set to True to fulfil that requirement.
        NOTE: if more than one worker is added, each worker is run after the previous one. Workers run sequentially
        between workers but can run in parallel for each worker if nproc is larger than 1.
        :param worker: something that is callable, optionally something that implements the Processor interface.
        :param nproc: how many worker processes to run in parallel
        :param machine: if None, run on the local machine, otherwise the host:port specification of the machine
        :param maxqsize: maximum number of items to put on the input queue before blocking, in order to prevent
        using up too much memory.
        :param maxerrors: how many exceptions to allow before aborting everything
        :param listify: when True, a worker callable that returns a return value gets the return value wrapped in
        a list. This can be used to use existing function more easily without wrapping them to follow the monad
        style return type.
        :return:
        """
        wi = WorkerInfo()
        wi.worker = worker
        wi.nproc = nproc
        wi.machine = machine
        wi.maxerrors = maxerrors
        wi.listify = listify
        wi.maxqsize = maxqsize
        self._workers.append(wi)
        self.nworkers = len(self._workers)
        if nproc > 1 or machine is not None:
            self.sequential = False
        if machine is not None:
            self.local = False

    def add_consumer(self,
                     consumer: Callable,
                     nproc: int = 1,
                     machine: Optional[Tuple[str, int]] = None,
                     maxqsize: int = 20,
                     maxerrors: int = 0):
        """
        Add a consumer.
        :param consumer: Something that is a callable and will take an item. If it has a close() method that method
        will get called after all items have been passed on.
        :param nproc: how many copies of the consumer to run in parallel
        :param machine: if None, run on the local machine, otherwise the host:port specification of the machine
        :param maxqsize: the maximum capacity of the input queue
        :param maxerrors: how many exceptions to allow before aborthing everything
        :return:
        """
        ci = ConsumerInfo()
        ci.consumer = consumer
        ci.nproc = nproc
        ci.machine = machine
        ci.maxerrors = maxerrors
        ci.maxqsize = maxqsize
        self._consumers.append(ci)
        self.nconsumers = len(self._consumers)
        if nproc > 1 or machine is not None:
            self.sequential = False
        if machine is not None:
            self.local = False

    def _run_sequentially(self):
        # we have at most one producer, otherwise we would not have sequential set
        pi = self._producers[0]
        if isinstance(pi.producer, collections.Iterator):
            theiter = pi.producer
        else:  # it must be an iterable!
            theiter = iter(pi.producer)
        finished = False
        ntotal: int = 0
        nerror: int = 0
        werrors = Counter()
        cerrors = Counter()
        perrors = 0
        while not finished:
            try:
                itemlist = [next(theiter)]
                ntotal += 1
            except StopIteration:
                # we are done!
                finished = True
                continue
            except Exception as ex:
                logger.exception("Got exception from producer iterator")
                perrors += 1
                ntotal += 1
                nerror += 1
                if perrors > pi.maxerrors:
                    raise Exception("Maximum number of producer errors reached, aborting")
            # now run each worker on the item list of the producer or previous worker, if any
            logger.debug("Processing item list {}".format(itemlist))
            for wid, wi in enumerate(self._workers):
                newitemlist = []
                for item in itemlist:
                    logger.debug("Calling worker {} with item {}".format(wi.worker, item))
                    try:
                        ret = wi.worker(item)
                    except Exception as ex:
                        logger.exception("Got an exception for worker {}".format(wid))
                        werrors[wid] += 1
                        nerror += 1
                        if werrors[wid] > wi.maxerrors:
                            raise Exception("Got more than {} errors for worker {}, aborting".format(wi.maxerrors, wid))
                    if wi.listify:
                        ret = [ret]
                    elif not isinstance(ret, list):
                        raise Exception("Value returned by the worker is not a list, maybe you need to set listify=True?")
                    newitemlist.extend(ret)
                itemlist = newitemlist
            # now run all the consumers on each of the items
            for item in itemlist:
                for cid, ci in enumerate(self._consumers):
                    try:
                        ci.consumer(item)
                    except Exception as ex:
                        logger.exception("Got an exception from consumer {}".format(cid))
                        cerrors[cid] += 1
                        nerror += 1
                        if cerrors[cid] > ci.maxerrors:
                            raise Exception("Got more than {} errors for consumer {}, aborting".format(ci.maxerrors, cid))
        # make the return data the same structure as for mp: a list of tuples of (name, number, ntotal, nerror),
        # but here name is "all", number is 1 and we only have one tuple
        return [("all", 1, ntotal, nerror)]

    def _run_locally(self):
        # When running locally, we use mp.Value() and mp.Queue() to get shared-memory resources.
        # (when running on a different machine, we need to use mgr = mp.Mgr(); mgr.Value() etc instead!

        # First, start all the consumers, if we have any
        aflag = mp.Value('l', 0)  # global abort flag
        # the queue for the producer will be set by the consumer or the first worker (there must be at least one
        # consumer or worker)
        pqueue = None
        if self.nconsumers > 0:
            # for all consumers together we need a queue and a an abort flag
            cqueue = mp.Queue()
            pqueue = cqueue
            # have one shared finish flag for all consumers!
            cflag = mp.Value('l', 0)  # signed long, initilized with 0=OK
            # now start the consumer processes and pass on the resources
            for ci in self._consumers:
                ci.procs = []
                ci.iqueue = cqueue
                ci.fflag = cflag
                ci.aflag = aflag
                ci.nerror = mp.Value('l', 0)
                ci.ntotal = mp.Value('l', 0)
                consumerprocess = ConsumerProcess(ci)
                for i in range(ci.nproc):
                    p = mp.Process(
                        target=consumerprocess,
                        name="consumer" + str(i),
                        args=(i, ))
                    ci.procs.append(p)
                    p.start()
        if self.nworkers > 0:
            # create the worker process pools starting with the last going forward
            # if there is a consumer, the last one uses the consumer queue as the output queue,
            # otherwise there is no output queue for the last!
            # Each worker group gets its own finished flag which it will check in order to figure out
            # if processing has finished
            for wid in range(len(self._workers)-1, -1, -1):  # get all the indices in reverse order
                wi = self._workers[wid]
                wi.procs = []
                if wid == len(self._workers)-1:
                    # last worker, we use the consumer queue for output
                    oqueue = cqueue
                else:
                    # otherwise use the input queue of the next worker for output
                    oqueue = self._workers[wid+1].iqueue
                iqueue = mp.Queue(maxsize=wi.maxqsize)
                wi.iqueue = iqueue
                wi.oqueue = oqueue
                pqueue = iqueue
                wflag = mp.Value('l', 0)
                wi.fflag = wflag
                wi.aflag = aflag
                wi.nerror = mp.Value('l', 0)
                wi.ntotal = mp.Value('l', 0)
                workerprocess = WorkerProcess(wi)
                for procnr in range(wi.nproc):
                    p = mp.Process(
                        target=workerprocess,
                        name="worker" + str(procnr),
                        args=(procnr, ))
                    wi.procs.append(p)
                    p.start()
        # now create the producers: we need a shared output queue and the flags to update when we are finished
        pflag = mp.Value('l', 0)  # signed long, initilized with 0=OK
        allproducerprocs = []
        for pi in self._producers:
            pi.procs = []
            pi.fflag = pflag
            pi.aflag = aflag
            pi.nerror = mp.Value('l', 0)
            pi.ntotal = mp.Value('l', 0)
            pi.oqueue = pqueue
            producerprocess = ProducerProcess(pi)
            for i in range(pi.nproc):
                p = mp.Process(
                    target=producerprocess,
                    name="producer" + str(i),
                    args=(i, )
                )
                pi.procs.append(p)
                allproducerprocs.append(p)
                p.start()
        # Everything is started, we could now start a thread or process to monitor everything!
        # TODO: something to monitor progress, number of items processed/errors etc

        # Now that we have started everything, work for things to finish:
        # First wait for all the producers to finish
        for p in allproducerprocs:
            logger.info("Waiting for producer {} to finish".format(p))
            p.join()
        # now that all the producers have finished, wait for each of the worker groups to finish in turn
        # but set their respective flag to finished first!
        for wi in self._workers:
            logger.info("Finishing worker {}".format(wi))
            wi.fflag.value = 1
            for p in wi.procs:
                logger.info("Waiting for worker {} to finish".format(p))
                p.join()
        # now that we have waited for all the worker processes, we should also wait for the consumers to finish
        # but first, set the consumer flag to finished (there is only one flag for all consumers, because they
        # all end once the last worker has ended)
        if self.nconsumers > 0:
            cflag.value = 1
            logger.info("Finishing consumers")
            for ci in self._consumers:
                for p in ci.procs:
                    logger.info("Waiting for consumer {} to finish".format(p))
                    p.join()
        # Now that all processing has finished, calculate the actual final states of all workers which have
        # state
        # TODO: if we started a monitoring thread, stop and cleanup here!
        # we go through each of the workers in sequence and check if there is something to do there
        for wi in self._workers:
            if wi.rqueue is not None and hasattr(wi.worker, "result") and callable(wi.worker.result):
                # get all the results from the result queue
                results = get_all_from_queue(wi.rqueue)
                wi.merge_results(results)
        # End of local MP processing
        # return the counters
        # from each producer, worker, consumer, get the number of total/error and put in a list of triples
        # (what, number, total, error)
        ret = []
        for pid, pi in enumerate(self._producers):
            ret.append(("producer", pid, pi.ntotal.value, pi.nerror.value))
        for wid, wi in enumerate(self._workers):
            ret.append(("worker", wid, wi.ntotal.value, wi.nerror.value))
        for cid, ci in enumerate(self._consumers):
            ret.append(("consumer", cid, ci.ntotal.value, ci.nerror.value))
        return ret

    def _run_distributed(self):
        # we have stuff running on different machines
        # for now we create a SyncManager server that registers all we need
        # before getting started
        # This expects that on each machine we want to use, the controller process has been started.
        # The controller process has started a syncserver process which shares a queue for sending
        # commands to the controller process for starting and waiting for the remote process pools.
        # TODO: this is not yet implemented!
        raise Exception("Distributed processing not yet implemented!")

    def run(self):
        """
        Actually run the whole process. Will run sequentially in the local machine if there is only one producer,
        if all nproc parameters are 1 and if all machine parameters are None.
        :return:
        """
        logger.info("Got {} producers: {}".format(self.nproducers, self._producers))
        logger.info("Got {} workers: {}".format(self.nworkers, self._workers))
        logger.info("Got {} consumers: {}".format(self.nconsumers, self._consumers))
        if self.nproducers == 0:
            raise Exception("Need at least one producer!")
        if self.nworkers + self.nconsumers == 0:
            raise Exception("Need at least one worker or consumer!")
        # nothing fancy needed, just run the whole thing sequentially!
        logger.info("Running sequentially: {}".format(self.sequential))
        logger.info("Running locally: {}".format(self.local))
        if self.sequential:
            return self._run_sequentially()
        else:
            if self.local:
                return self._run_locally()
            else:
                return self._run_distributed()


# TODO: the main for this could be used to start a remote machine controller process!
