import sys
from .version import __version__
if sys.version_info[0] != 3:
    raise Exception("Only python version 3.x is supported!")
