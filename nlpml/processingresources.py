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

    def _supports_multiprocessing(self):
        if hasattr(self, "singleprocess") and self.singleprocess:
            return False
        else:
            return True

    @abstractmethod
    def __call__(self, item, **kwargs):
        pass

    # Note: the following function is defined so it can be used as a normal instance function
    # and like a static function for the class: the parameter is either self, for an instance
    # of a pipeline if called as static function.
    def supports_multiprocessing(pipeline):
        """
        Check a whole pipeline if it supports multiprocessing
        :return: true if all contained or nested Prs support multiprocessing
        """
        if pipeline is None:
            return True
        elif isinstance(pipeline, ProcessingResource):
            return pipeline._supports_multiprocessing()
        elif isinstance(pipeline, list):
            ret = True
            for pr in pipeline:
                if not ProcessingResource.supports_multiprocessing(pr):
                    return False
        return True


class PrCallFunction(ProcessingResource):
    """
    Processing resource to encapsulate calling any function.
    """

    def __init__(self, function, *args):
        self.function = function
        self.args = args

    def __call__(self, item, **kwargs):
        return self.function(item, *self.args)
