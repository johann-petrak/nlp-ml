#!/usr/bin/env python
'''
Library for extending PyTorch modules
'''

import torch
from torch.nn import functional as F
import callbacks as callbacksmodule
import sys
import numpy as np
import time

## The following should go into some utils, probably

def prefix_metricsdict(metricsdict, prefix):
    """
    Add a prefix to the names in the metrics dict. If the prefix and old name
    should be separated by an underscore or hyphen, that must be included in the prefix!
    :param dict:
    :return:
    """
    ret = {}
    for k, v in metricsdict.items():
        ret[prefix+k] = v
    return ret

def unpack_singleton(x):
    """
    Return original except when it is a sequence of length 1 in which case return the only element
    :param x: a list
    :return: the original list or its only element
    """
    if len(x) == 1:
        return x[0]
    else:
        return x

class Metric(object):
    """
    API for metric classes. this should implement __call__(self, outputs, y)
    where outputs are the outputs of the model, and y are the targets.
    """

    def __call__(self, preds, y):
        """
        Return a metric based on the predictions and the targets.
        This is a single scalar for the whole set
        :param preds:
        :param y:
        :return: scalar metric value or a list of values (e.g. for accuracy it could be accuracy, correct, total)
        """
        raise Exception("Not implemented here")


class Accuracy(Metric):

    def __init__(self, ignore_index=None, model=None):
        self.ignore_index = ignore_index
        self.model = model

    def __call__(self, outputs, y, mask=None):
        """
        Calculates the accuracy over all outputs/targets in a batch.
        Both preds and y are assumed to have shape batchsize or batchsize,maxseqlen.
        If a mask is provided, the accuracy is only calculated for the values where the mask is 1.
        If the ignore_index parameter has been set, then any target equal to that index is ignored.
        :param preds:
        :param y:
        :return:
        """
        y = y.cpu()
        preds = self.model.predict(outputs).cpu()
        if mask==None:
            if self.ignore_index is None:
                mask = torch.ones(y.size()).byte()
            else:
                mask = (y != self.ignore_index)
        same = (y == preds)
        vals = torch.masked_select(same, mask)  # this creates an 1d tensor!
        total = vals.size()[0]
        correct = vals.sum()
        return float(correct)/total



class ExtendedModule(torch.nn.Module):
    """
    Abstract class that shows the basic API of how we extend the Module implementation.
    The basic idea is that an extended module is self-contained and requires practically no
    boiler plate outside. It also tries to imitate a bit how Keras does this.
    In general, "x" is input data and "y" is output data (labels, targets).
    Like in Keras, if there are several input features, x should be a python list, otherwise
    it should be a pytorch tensor (instead of numpy array as in Keras).
    If there is some complex processing needed to convert the initial representation to
    one or more features, this is supposed to happen in the preprocessing pipeline
    (see ExtendedDataset).
    "dl" is a data loader which produces batches of data. Again we do not require any specific
    format but one possible representation is that each batch is a dictionary with entries
    for the target/output and the input(s).
    """

    def __init__(self, config=None, **kwargs):
        """
        :param config:
        :param cuda:
        :param kwargs:
        """
        super().__init__()
        # TODO: figure out where to set attributes from config or kwargs!
        self.stop_training = False  # if this is set to true, training will stop before the next batch
        # TODO: not sure if we need this at all, we should always be able to guess from the
        # number of dimenions of the output ?
        # self.is_sequence = False    # if this has a sequence dimensions for inputs and outputs
        self.clip_grad_norm_value = None
        self.clip_grad_norm_type = 2

    # Note: most standard modules have three named modules:
    # layer_input: the thing that takes the input
    # layer_hidden: the thing that processes the output of the input layer and produces the final hidden layer
    # layer_output: the layer that converts the final hidden layer into some kind of output
    # However for some models this may be completely different and also it is likely to be different
    # for models which take more than one input!

    # TODO: we need to have these arguments for init or a separate init function to set this:
    # loss=LossFunction
    # metrics=None (list of additional metrics to calculate)
    # NOTE: this will also set self.metrics_names
    # This is also where we choose an automatic optimizer if none is given
    def init_model(self, optimizer=None, loss=None, metrics=None):
        """
        This must be called before training starts. If no argument is specified, the
        defaults for the module are set.
        This does not have to be called after re-storing a model.
        :param optimizer:
        :param loss:
        :param metrics:
        :return:
        """
        raise Exception("Not implemented here")

    def is_on_cuda(self):
        """
        Return if the module is on cuda. Note: this only works if the module has at least one parameter.
        (otherwise it does not really make sense).
        :return:
        """
        return next(self.parameters()).is_cuda

    def save(self, filename):
        """
        Save the model to the filename
        :param filename:
        :return:
        """
        is_on_cuda = self.is_on_cuda()
        if is_on_cuda:
            self.cpu()
        torch.save(self, filename)
        if is_on_cuda:
            self.cuda()

    def save_weights(self, filename):
        """
        Save weights only
        :param filename:
        :return:
        """
        is_on_cuda = self.is_on_cuda()
        if is_on_cuda:
            self.cpu()
        torch.save(self.state_dict(), filename)
        if is_on_cuda:
            self.cuda()


    def train_on_batch(self, x, y):
        """
        Run a training step on the data from a single batch of inputs x and outputs y
        :param x: a batch of inputs
        :param y: a batch of outputs
        :return: a dictionary of loss and other metrics for this batch
        """
        start = time.time()
        self.train()
        if self.is_on_cuda():
            x = x.cuda()
            y = y.cuda()
        # standard approach for training on a batch
        self.zero_grad()
        # get outputs
        outputs = self.forward(x)
        allmetrics = {}
        # calculate loss from outputs
        loss = self.lossfunction(outputs, y)
        loss.backward()
        # at least for now, record the maximum gradient norm (2) so we can set the clipping
        totalnorm = 0
        for p in filter(lambda p: p.grad is not None, (self.parameters())):
            tmp1 = p.grad.data.norm(2)
            totalnorm += tmp1
        totalnorm = totalnorm ** 0.5
        if self.clip_grad_norm_value is not None:
            torch.nn.utils.clip_grad_norm_(self.parameters(),
                                           self.clip_grad_norm_value, norm_type=self.clip_grad_norm_type)
        self.optimizer.step()
        allmetrics["loss"] = float(loss)
        allmetrics["size"] = int(x.size()[0])
        allmetrics["totalnorm"] = float(totalnorm)
        allmetrics["time"] = time.time()-start
        for i, m in enumerate(self.metrics):
            allmetrics[self.metrics_names[i]] = float(m(outputs, y))
        return allmetrics

    def test_on_batch(self, x, y):
        """
        Evaluate on a batch.
        :param x:
        :param y:
        :return: dictionary of metrics
        """
        self.eval()
        if self.is_on_cuda():
            x = x.cuda()
            y = y.cuda()
        outputs = self.forward(x)
        allmetrics = {}
        # calculate loss from outputs
        loss = self.lossfunction(outputs, y)
        allmetrics["loss"] = float(loss)
        for i, m in enumerate(self.metrics):
            allmetrics[self.metrics_names[i]] = float(m(outputs, y))
        return allmetrics


    def predict_on_batch(self, x):
        """
        Return predictions for the batch of inputs. NOTE: this should always turn on eval() mode!
        :param x:
        :return: predictions for the batch
        """
        self.eval()
        raise Exception("Not implemented here")

    def fit_generator(self, dataloader, steps=None, epochs=1, validation_dataloader=None, validation_steps=None, initial_epoch=0, callbacks=None):
        """
        Train on the data provided by the generator/dataloader.
        Note: this gathers per epoch validation history automatically, but to get additional
        history, need to add our own callback!
        :param dataloader: an (extended) dataloader. If only part of a dataset should get used, this
        should get done by using the approproate dataloader.
        :param epochs:  The index of the last epoch to run. If this is combined with early_stopping then
        it is the maximum epoch number to run, but training could get stopped already earlier.
        :param validation_data: dataloader to provide batches of validation data
        :param validation_steps:
        :param initial_epoch:
        :param callbacks: list of callbacks.
        :return: History object: a callback instance that has attributes epoch (list of epoch numbers) and
        history (map of measure names, each having an array of values, one for each epoch)
        """
        # Standard implementation
        # metricsdir is a dictionary mapping metric names to values, this gets returned from
        # train_on_batch and evaluate_generator
        callbacklist = callbacksmodule.CallbackList(callbacks, model=self)
        history = callbacksmodule.EpochHistory()
        callbacklist.append(history)
        callbacklist.on_train_begin()
        for epoch in range(initial_epoch, epochs, 1):
            callbacklist.on_epoch_begin(epoch)
            batchsizes = []
            batchmetrics = {}
            step = 0
            start = time.time()
            beforefetch = time.time()
            for batch in dataloader:
                if self.stop_training:
                    # TODO: replace with proper logger!
                    print("Stopping requested!", file=sys.stderr)
                    break
                retrievetime = time.time() - beforefetch
                callbacklist.on_batch_begin(step)
                # NOTE: we assume the first element of the batch is the feature or list of features
                metricsdir = self.train_on_batch(batch[0], batch[1])
                metricsdir["getbatchtime"] = retrievetime
                batchsizes.append(len(batch[0]))
                for k, v in metricsdir.items():
                    batchmetrics.setdefault(k, []).append(v)
                metricsdir["size"] = len(batch[0])
                callbacklist.on_batch_end(step, logs=metricsdir)
                step += 1
                if steps == step:
                    break
                beforefetch = time.time()
            # epoch is completed, evaluate on the validation set
            perepochtraintime = time.time()-start
            start = time.time()
            metricsdir = self.evaluate_generator(validation_dataloader, evaluation_steps=validation_steps)
            metricsdir = prefix_metricsdict(metricsdir, "eval-")
            metricsdir["eval-time"] = perepochtraintime
            metricsdir["train-time"] = time.time()-start
            # also get average of training metrics
            for k, v in batchmetrics.items():
                if k == "totalnorm":
                    metricsdir["batch-max-" + k] = np.max(v)
                else:
                    metricsdir["batch-avg-"+k] = np.average(v, weights=batchsizes)
            callbacklist.on_epoch_end(epoch, logs=metricsdir)
        # TODO: call only if we did not interrupt the training!
        callbacklist.on_train_end()
        return history

    def evaluate_generator(self, dataloader, evaluation_steps=None):
        """
        Evaluate on the data returned from the dataloader
        :param dataloader:
        :return: dictionary of metrics over all batches from the dataloader (this is the weighted
        average if each of the batch metrics)
        """
        self.eval()
        batch_sizes = []
        batch_metrics = {}
        step = 0
        beforefetch = time.time()
        for batch in dataloader:
            # TODO: for now each batch is assumed to be a list where the first is the x and the
            # second is y (for now x is a single feature).
            # Also we already expect tensors here, so the 0-th dimension of y is the batchsize
            retrievetime = time.time() - beforefetch
            x = batch[0]
            y = batch[1]
            batch_sizes.append(y.size()[0])
            metricsdir= self.test_on_batch(x, y)
            metricsdir["getbatchtime"] = retrievetime
            # metricsdir is the dictionary of metrics, so append the values to the metrics
            for k, v in metricsdir.items():
                batch_metrics.setdefault(k, []).append(v)
            step += 1
            if evaluation_steps == step:
                break
            beforefetch = time.time()
        # TODO: calculate the averages of all metrics and return as a single dictionary!
        ret = {}
        for k, v in batch_metrics.items():
            ret[k] = np.average(v, weights=batch_sizes)
        return ret


class ClassificationModule(ExtendedModule):

    # TODO: do we even need the n_classes?
    def __init__(self, n_classes=None):
        super().__init__()
        self.n_classes = n_classes

    def init_model(self, optimizer=None, loss=None, metrics=None):
        if optimizer is None:
            parms = filter(lambda p: p.requires_grad, self.parameters())
            self.optimizer = torch.optim.Adam(parms, lr=0.015, weight_decay=1e-08)
        if loss is None:
            self.lossfunction = torch.nn.CrossEntropyLoss(ignore_index=-1)
        if metrics is None:
            self.metrics = [Accuracy(model=self)]
        self.metrics_names = [type(x).__name__ for x in self.metrics]

    def predict(self, outputs):
        """
        Calculate the predictions from the outputs, not the inputs!
        :param outputs:
        :return:
        """
        self.eval()
        _, idxs = torch.max(outputs, dim=(len(outputs.size()) - 1))  # maximum over the last dimension!
        return idxs

    def predict_on_batch(self, x):
        """
        Return predictions for the batch of inputs. For sequences this returns
        the predictions of shape batchsize,seqlen, scores of shape batchsize,seqlen,n_classes
        For non-sequences it returns the predictions as shape batchsize, scores as batchsize,n_classes,
        :param x:
        :return: predictions for the batch and scores for the batch, both as torch tensors!
        """
        self.eval()
        if self.is_on_cuda():
            x = x.cuda()
        outputs = self.forward(x)
        idxs = self.predict(outputs)
        return idxs, outputs



