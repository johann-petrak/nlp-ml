"""
Classes representing "processing resources". Each processing resource has
to implement __call__(self, item, **kwargs) and inherit from ProcessingResource.
By default every PR is assumed to be fully parallelizable, but a PR can be
set to be for single processing use only if the field singleprocess is set to True.
"""

from abc import ABC, abstractmethod
import pickle

class ProcessingResource(ABC):
    """
    Abstract base class of all processing resources. Subclasses are classes where the
    name starts with Pr.
    """
    def __init__(self):
        self.singleprocess = False

    def supports_multiprocessing(self):
        if hasattr(self, "singleprocess") and self.singleprocess:
            return False
        else:
            return True

    @abstractmethod
    def __call__(self, item, **kwargs):
        pass

    def get_data(self):
        """
        Get whatever data the PR has accumulated
        :return: the global data gathered by the PR
        """
        return None

    def merge_data(self, listofdata):
        """
        Merge a list of data gathered by copies of this PR into a single data and store it.
        :param listofdata: a list of data each element is the data from another copy of this PR
        :return: the merged data
        """
        return None


class PrCallFunction(ProcessingResource):
    """
    Processing resource to encapsulate calling any function.
    """

    def __init__(self, function, *args, **kwargs):
        """
        Wrap a function in a PR. IMPORTANT: it must be possible to pickle the function and all the arguments when
        the Pr gets serialized.
        This is tested at the time when the Pr gets created, which is not 100% accurate.
        :param function: the function to wrap.
        :param args: any constat arguments always to pass to the function, AFTER the item.
        :param kwargs: any constant keyword arguments always to pass to the function
        """
        super().__init__()
        tmp = pickle.dumps((function, args, kwargs))
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def __call__(self, item, *args, **kwargs):
        """
        Call the function. item gets passed as first positional argument, followed by all the args
        defined when this Pr was created, followed by any additional positional arguments specified
        here. The kwargs specified at init time get updated by any kwargs specified here and also
        get passed on to the function.
        :param item: the item to process
        :param args: any positional arguments to add in addition to those specified at init time
        :param kwargs: any keyword arguments to add, overriding any specified at init time
        :return:
        """
        allargs = []
        allargs.extend(self.args)
        allargs.extend(args)
        allkwargs = {}
        allkwargs.update(self.kwargs)
        allkwargs.update(kwargs)
        return self.function(item, *allargs, **allkwargs)


class PrPipeline(ProcessingResource):
    """
    Processing resource that runs a list of other processing resources in sequence
    """

    def __init__(self, listofprs):
        super().__init__()
        self.listofprs = listofprs

    def __call__(self, item, **kwargs):
        for pr in self.listofprs:
            item = pr(item)
        return item
