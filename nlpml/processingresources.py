"""
Classes representing "processing resources". Each processing resource has
to implement __call__(self, item, **kwargs) and inherit from ProcessingResource.
By default every PR is assumed to be fully parallelizable, but a PR can be
set to be for single processing use only if the field singleprocess is set to True.
"""

from abc import ABC, abstractmethod


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

    def __init__(self, function, *args):
        super().__init__()
        self.function = function
        self.args = args

    def __call__(self, item, **kwargs):
        return self.function(item, *self.args)


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
