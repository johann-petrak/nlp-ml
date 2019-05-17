#!/usr/bin/env python
'''
Support for running jobs in multiprocessing or even distributed processing form.
'''

import sys
import os
import collections
from typing import List, Tuple, Union, Callable, Dict, Optional
import multiprocessing as mp
from multiprocessing import Queue, Value, Process
from collections.abc import Iterable, Iterator
import queue
from time import sleep

from loguru import logger


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
    nproc: int        # number of processes
    aflag: Value      # abort flag
    fflag: Value      # finish flag
    maxerrors: int    # maximum number of errors allowed in this component
    procs: List[Process]  # list of processes running this component
    timeout: Optional[int] = None # timeout, by default None to indicate that there is no timeout

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
    rqueue: Queue
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
        self.maxerrors: int = info.maxerrors


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
        while True:
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
                    self.consumer(item)
                except Exception as ex:
                    # log the error
                    logger.exception("Caught exception in ConsumerProcess")
                    haderrors += 1
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
                    break
                else:
                    hadretried += 1
                    if hadretried > self.empty_max:
                        self.aflag.value = 1
                        logger.error("Consumer queue empty for more than {} retries, aborting".format(self.empty_max))
                        break
                    sleep(self.empty_retry)
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
        while True:
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
                    ret = self.worker(item)
                    if self.listify:
                        ret = [ret]
                    for x in ret:
                        self.oqueue.put(x)
                except Exception as ex:
                    # log the error
                    logger.exception("Caught exception in WorkerProcess")
                    haderrors += 1
                    # if we had more than maxerrors, terminate "immediately". To avoid problems this means we
                    # go on to read and discard elements from the queue until it is empty
                    if haderrors > self.maxerrors:
                        logger.error("Got more than {} errors for this worker process, terminating".format(self.maxerrors))
                        self.aflag.value = 1
                        queue_readanddiscard(self.iqueue)
                        break
            except queue.Empty:
                # if the queue is empty, let us check if the finish flag was set before we did try to read the queue
                # if yes we should terminate, otherwise wait a second before checking again
                if finishflag == 1:
                    break
                else:
                    hadretried += 1
                    if hadretried > self.empty_max:
                        self.aflag.value = 1
                        logger.error("Queue empty for more than {} retries, aborting".format(self.empty_max))
                        break
                    sleep(self.empty_retry)
        # TODO: now that we are outside the loop, check if our worker is something where we should send back
        # the result/state and if yes, put the result on the rqueue
        # if the worker has a method "result()" we call it and put whatever it returns on the result queue
        # If the worker represents a whole pipeline, then the result is really a (possibly nested) list of
        # the results of the leaf workers. The worker should also have a "combine_results()" method which
        # is then used to combine all those results
        if hasattr(self.worker, "result") and callable(self.result):
            try:
                result = self.worker.result()
            except Exception as ex:
                logger.error("Exception when trying to retrieve results for worker {}".format(self.myid))
                result = None
            self.rqueue.put(result)
        logger.info("Stopping worker {} process pid={} ppid={} name={}".format(myid, pinfo[0], pinfo[1], pinfo[2]))


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
        haderrors = 0
        while True:
            if self.aflag.value == 1:
                logger.error("Producer {}, got abort flag, aborting".format(myid))
                break
            try:
                el = next(producer_iter)
                self.oqueue.put(el)
            except Exception as ex:
                haderrors += 1
                if haderrors > self.maxerrors:
                    self.aflag.value = 1
                    logger.error("More than {} errors in producer {}, aborting".format(self.maxerrors, myid))
                    break
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
        self._consumers.appned(ci)
        self.nconsumers = len(self._consumers)
        if nproc > 1 or machine is not None:
            self.sequential = False
        if machine is not None:
            self.local = False

    def _run_sequentially(self):
        # we have at most one producer, otherwise we would not have sequential set
        producernerrors = 0
        pi = self._producers[0]
        if isinstance(pi.producer, collections.Iterator):
            theiter = pi.producer
        else:  # it must be an iterable!
            theiter = iter(pi.producer)
        finished = False
        while not finished:
            try:
                itemlist = [next(theiter)]
            except StopIteration:
                # we are done!
                finished = True
                continue
            except Exception as ex:
                # TODO: log the exception properly!
                logger.exception("Got an exception")
                producernerrors += 1
            if producernerrors > pi.maxerrors:
                raise Exception("Maximum number of producer errors reached, aborting")
            # now run each worker on the item list of the producer or previous worker, if any
            logger.debug("Processing item list {}".format(itemlist))
            for wi in self._workers:
                # TODO: each worker may have a different maxerror, so we need to keep
                # set of error counters, one for each worker, around
                # TODO: add exception handling and counting
                newitemlist = []
                for item in itemlist:
                    logger.debug("Calling worker {} with item {}".format(wi.worker, item))
                    ret = wi.worker(item)
                    if wi.listify:
                        ret = [ret]
                    elif not isinstance(ret, list):
                        raise Exception("Value returned by the worker is not a list, maybe you need to set listify=True?")
                    newitemlist.extend(ret)
                itemlist = newitemlist
            # now run all the consumers on each of the items
            for item in itemlist:
                for ci in self._consumers:
                    # TODO: each worker may have a different maxerorr, so we need a set of counters
                    # TODO: add exception handling and counting
                    ci.consumer(item)

    def _run_locally(self):
        # everything is run on the local machine, for this we will create all
        # shared resources (queues) on the local manager
        mgr = mp.Manager()
        # then start the processes as required and pass on the resources they need
        # We start things from the end of the pipeline in order to avoid stuffing
        # the queues too soon
        #
        # First, start all the consumers, if we have any
        aflag = mgr.Value('l', 0)  # global abort flag
        # the queue for the producer will be set by the consumer or the first worker (there must be at least one
        # consumer or worker)
        pqueue = None
        if self.nconsumers > 0:
            # for all consumers together we need a queue and a an abort flag
            cqueue = mgr.Queue()
            pqueue = cqueue
            cflag = mgr.Value('l', 0)  # signed long, initilized with 0=OK
            # now start the consumer processes and pass on the resources
            for consumerinfo in self._consumers:
                consumercode = consumerinfo["code"]
                consumernproc = consumerinfo["nproc"]
                consumermaxerrors = consumerinfo["maxerrors"]
                consumerinfo["procs"] = []
                consumerprocess = ConsumerProcess()
                for i in range(consumernproc):
                    p = mp.Process(
                        target=consumerprocess,
                        name="consumer" + str(i),
                        args=(consumercode, cqueue, cflag, aflag, consumerinfo, i))
                    consumerinfo["procs"].append(p)
                    p.start()
        if self.nworkers > 0:
            # create the worker process pools starting with the last going forward
            # if there is a consumer, the last one uses the consumer queue as the output queue,
            # otherwise there is no output queue for the last!
            # Each worker group gets its own finished flag which it will check in order to figure out
            # if processing has finished
            # TODO: !!!!!!
            for wnr in range(len(self._workers), 0, -1):
                workerinfo = self._workers[wnr]
                workercode = workerinfo["code"]
                workernproc = workerinfo["nproc"]
                workerinfo["procs"] = []
                if wnr == len(self._workers)-1:
                    # last worker, we use the consumer queue for output
                    oqueue = cqueue
                else:
                    # otherwise use the input queue of the next worker for output
                    oqueue = self.workerinfo[wnr+1]["iqueue"]
                iqueue = mgr.Queue(maxsize=workerinfo["maxqsize"])
                workerinfo["iqueue"] = iqueue
                workerinfo["oqueue"] = oqueue
                pqueue = iqueue
                wflag = mgr.Value('l', 0)
                workerprocess = WorkerProcess()
                for procnr in range(workernproc):
                    p = mp.Process(
                        target=workerprocess,
                        name="worker" + str(procnr),
                        args=(workercode, iqueue, oqueue, wflag, aflag, workerinfo, procnr))
                    consumerinfo["procs"].append(p)
                    p.start()
        # now create the producers: we need a shared output queue and the flags to update when we are finished
        pflag = mgr.Value('l', 0)  # signed long, initilized with 0=OK
        allproducerprocs = []
        for producerinfo in self._producers:
            producercode = producerinfo["code"]
            producernproc = producerinfo["nproc"]
            producerinfo["procs"] = []
            producerprocess = ProducerProcess()
            for i in range(producernproc):
                p = mp.Process(
                    target=producerprocess,
                    name="producer" + str(i),
                    args=(producercode, pqueue, pflag, aflag, producerinfo, i)
                )
                producerinfo["procs"].append(p)
                allproducerprocs.append(p)
                p.start()
        # Now that we have started everything, work for things to finish:
        # First wait for all the producers to finish
        for p in allproducerprocs:
            p.join()
        # now that all the producers have finished, wait for each of the worker groups to finish in turn
        # but set their respective flag to finished first!
        for workerinfo in self._workers:
            wflag = workerinfo["wflag"]
            wflag = 1
            for p in workerinfo["procs"]:
                p.join()
        # now that we have waited for all the worker processes, we should also wait for the consumers to finish
        # but first, set the consumer flag to finished (there is only one flag for all consumers, because they
        # all end once the last worker has ended)
        if self.nconsumers > 0:
            cflag = 1
            for consumerinfo in self._consumers:
                for p in consumerinfo["procs"]:
                    p.join()
        # Now that all processing has finished, calculate the actual final states of all workers which have
        # state
        # TODO: run the reduce_results() method to merge the states of stateful workers
        # End of local MP processing

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
        print("!!!!!", file=sys.stderr)
        logger.info("Got {} produucers: {}".format(self.nproducers, self._producers))
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
            self._run_sequentially()
        else:
            if self.local:
                self._run_locally()
            else:
                self._run_distributed()


# TODO: the main for this could be used to start a remote machine controller process!
