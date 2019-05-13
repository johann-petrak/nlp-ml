#!/usr/bin/env python
'''
Support for running jobs in multiprocessing or even distributed processing form.
'''

import collections
import multiprocessing as mp


class Consumer:
    """
    Class that represents a consumer process which in turn invokes the actual consumer callable.
    This class gets initialised before getting passed to each consumer process and then gets called
    once to get executed in the process. It should process all items it gets from the input queue and
    and then terminate.
    """
    def __init__(self):
        pass

    def __call__(self, consumerfunction, cqueue, cflag, aflag, maxerrors, myid):
        print("Starting consumer {}".format(myid))


class Worker:
    """
    Class that represents a worker process which in turn invokes the actual worker callable.
    This class gets initialised before getting passed to each worker process and then gets called
    once to get executed in the process. It should process all items it gets from the input queue and
    and send the resulting processed items to the output queue (if there is one, otherwise do nothing)
    If the worker callable has the right method, it should also get the result and send it over the result queue.
    """
    def __init__(self):
        pass

    def __call__(self, consumerfunction, iqueue, oqueue, rqueue, cflag, aflag, maxerrors, myid):
        print("Starting worker {}".format(myid))


class Producer:
    """
    Class that represents a producer process which in turn processes the actual producer iterator/iterable
    This class gets initialised before getting passed to each consumer process and then gets called
    once to get executed in the process. It should iterate over all items in the iterable/iterator and
    send those items off to the producer queue.
    """
    def __init__(self):
        pass

    def __call__(self, consumerfunction, pqueue, cflag, aflag, maxerrors, myid):
        print("Starting producer {}".format(myid))



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

    def __init__(self):
        # lists of producer/worker/consumer info dicts
        # keys initially are: code, nproc, machine, maxerrors
        # additional keys: proc (process object)
        self._producers = []
        self._workers = []
        self._consumers = []
        self.sequential = True
        self.local = True
        self.nproducers = 0
        self.nworkers = 0
        self.nconsumers = 0

    def add_producer(self, producer, nproc=1, machine=None, maxerrors=0):
        """
        Add a producer to the beginning of the processing pipeline. A producer somehow generates items to get processed
        by workers.
        NOTE: if more than one producer is added, they are run in parallel (so sequential processing is disabled)
        :param producer: something that is iterable, generating items
        :param nproc: how many producers to run in parallel, if more than one, producers should know how to make sure that
        no duplicate data is produced.
        :param machine: if None, run on the local host, otherwise the host:port specification of a computer to run
        the producer(s) on. TODO: config for how to do the remote running!
        :param maxerrors: how many exceptions to allow when an item is generated, if maxerrors is reached, the whole
        processing will abort
        :return:
        """
        if isinstance(producer, collections.Iterable) or isinstance(producer, collections.Iterable):
            pass
        else:
            raise Exception("producer must be an Iterator or Iterable")
        self._producers.append(
            {"code": producer, "nproc": nproc, "machine": machine, "maxerrors": maxerrors}
        )
        self.nproducers += 1
        if nproc > 0 or machine is not None or self.nproducers > 1:
            self.sequential = False
        if machine is not None:
            self.local = False

    def add_worker(self, worker, nproc=1, machine=None, maxerrors=0):
        """
        Add a worker as the next stage in the processing.
        NOTE: if more than one worker is added, each worker is run after the previous one. Workers run sequentially
        between workers but can run in parallel for each worker if nproc is larger than 1.
        :param worker: something that is callable, optionally something that implements the Processor interface.
        :param nproc: how many worker processes to run in parallel
        :param machine: if None, run on the local machine, otherwise the host:port specification of the machine
        :param maxerrors: how many exceptions to allow before aborting everything
        :return:
        """
        self._producers.append(
            {"code": worker, "nproc": nproc, "machine": machine, "maxerrors": maxerrors}
        )
        self.nworkers += 1
        if nproc > 0 or machine is not None:
            self.sequential = False
        if machine is not None:
            self.local = False

    def add_consumer(self, consumer, nproc=1, machine=None, maxerrors=0):
        """
        Add a consumer.
        :param consumer: Something that is a callable and will take an item. If it has a close() method that method
        will get called after all items have been passed on.
        :param nproc: how many copies of the consumer to run in parallel
        :param machine: if None, run on the local machine, otherwise the host:port specification of the machine
        :param maxerrors: how many exceptions to allow before aborthing everything
        :return:
        """
        self._producers.append(
            {"code": consumer, "nproc": nproc, "machine": machine, "maxerrors": maxerrors}
        )
        self.nconsumers += 1
        if nproc > 0 or machine is not None:
            self.sequential = False
        if machine is not None:
            self.local = False

    def _run_sequentially(self):
        # we have at most one producer, otherwise we would not have sequential set
        producernerrors = 0
        producerinfo = self._producers[0]
        theproducer = producerinfo["code"]
        theproducermaxerrors = producerinfo["maxerrors"]
        if isinstance(theproducer, collections.Iterator):
            theiter = theproducer
        else:  # it must be an iterable!
            theiter = iter(theproducer)
        while True:
            try:
                itemlist = [next(theiter)]
            except Exception as ex:
                # TODO: log the exception
                producernerrors += 1
            if producernerrors > theproducermaxerrors:
                raise Exception("Maximum number of producer errors reached, aborting")
            # now run each worker on the item list of the producer or previous worker, if any
            for oneworkerinfo in self._workers:
                # TODO: each worker may have a different maxerror, so we need to keep
                # set of error counters, one for each worker, around
                # TODO: add exception handling and counting
                theworker = oneworkerinfo["code"]
                theworkermaxerrors = oneworkerinfo["maxerrors"]
                newitemlist = []
                for item in itemlist:
                    ret = theworker(item)
                    newitemlist.extend(ret)
                itemlist = newitemlist
            # now run all the consumers on each of the items
            for item in itemlist:
                for oneconsumerinfo in self._consumers:
                    # TODO: each worker may have a different maxerorr, so we need a set of counters
                    # TODO: add exception handling and counting
                    theconsumer = oneconsumerinfo["code"]
                    theconsumermaxerrors = oneconsumerinfo["maxerrors"]
                    theconsumer(item)

    def run(self):
        """
        Actually run the whole process. Will run sequentially in the local machine if there is only one producer,
        if all nproc parameters are 1 and if all machine parameters are None.
        :return:
        """
        if self.nproducers == 0:
            raise Exception("Need at least one producer!")
        if self.nworkers + self.nconsumers == 0:
            raise Exception("Need at least one worker or consumer!")
        # nothing fancy needed, just run the whole thing sequentially!
        if self.sequential:
            self._run_sequentially()
        else:
            if self.local:
                # everything is run on the local machine, for this we will create all
                # shared resources (queues) on the local manager
                mgr = mp.Manager()
                # then start the processes as required and pass on the resources they need
                # We start things from the end of the pipeline in order to avoid stuffing
                # the queues too soon
                #
                # First, start all the consumers, if we have any
                aflag = mgr.Valye('l', 0)  # global abort flag
                if self.nconsumers > 0:
                    # for all consumers together we need a queue and a an abort flag
                    cqueue = mgr.Queue()
                    cflag = mgr.Value('l', 0)  # signed long, initilized with 0=OK
                    # now start the consumer processes and pass on the resources
                    for consumerinfo in self._consumers:
                        consumercode = consumerinfo["code"]
                        consumernproc = consumerinfo["nproc"]
                        consumermaxerrors = consumerinfo["maxerrors"]
                        consumerinfo["procs"] = []
                        consumer = Consumer()
                        for i in range(consumernproc):
                            p = mp.Process(
                                target=consumer,
                                name="consumer"+str(i),
                                args=(consumercode, cqueue, cflag, aflag, consumermaxerrors, i))
                            consumerinfo["procs"].append(p)
                            p.start()
                if self.nworkers > 0:
                    # create the worker process pools starting with the last going forward
                    # if there is a consumer, the last one uses the consumer queue as the output queue,
                    # otherwise there is no output queue for the last!
                    # Each worker group gets its own finished flag which it will check in order to figure out
                    # if processing has finished
                    pass
                # now create the producers: we need a shared output queue and the flags to update when we are finished
                pqueue = mgr.Queue()
                pflag = mgr.Value('l', 0)  # signed long, initilized with 0=OK
                allproducerprocs = []
                for producerinfo in self._producers:
                    producercode = producerinfo["code"]
                    producernproc = producerinfo["nproc"]
                    producermaxerrors = producerinfo["maxerrors"]
                    producerinfo["procs"] = []
                    producer = Producer()
                    for i in range(producernproc):
                        p = mp.Process(
                            target=producer,
                            name="producer"+str(i),
                            args=(producercode, pqueue, pflag, aflag, producermaxerrors, i)
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
            else:
                # we have stuff running on different machines
                # for now we create a SyncManager server that registers all we need
                # before getting started
                # Then, all components get started remotely and resource NAMES are
                # are sent over, together with our own hostname/port
                # TODO: this is not yet implemented!
                raise Exception("Distributed processing not yet implemented!")

