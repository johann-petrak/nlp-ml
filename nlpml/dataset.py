#!/usr/bin/env python
'''
Temporary implementation of the Dataset interface without dependency on
pytorch to make accessing the data file in a sorted way possible.

A Dataset is something that allows direct acces to every item using bracket-notation, e.g
myds[22]. There is no initial assumption about how a dataset is represented, e.g. one file on
harddisk, a databse, something in memory etc. nor about the possibility of parallel access to
the same dataset from separate threads or processes. Specific subclasses may implement
these or other aspects in a specific way.


IMPORTANT! This has been copied over from a different library and currently
contains some features which are not used or relevant in here.
'''

# TODO: all wrappers should also hand through calls to setitem and other base methods!!

import os
import random
import sys
import pickle
import json
import numbers
import pathlib


class ExtendedDataset(object):
    """
    Our own base class for datasets which adds a few conventions:
    is_writable is False by default but some Datasets can be defined to be writable.
    Writable datasets implement __setitem__(self, key, item) for a key that is an integer.
    is_multi_read is True by default: if a dataset can be reasonably wrapped and/or read by several
    clients in multiple threads at the same time, this should be True, otherwise false. If
    doing this would corrupt the dataset or impose a significant performance penalty it should be False.
    Note: all classes that derive from this class should invoke the parent init method!
    """
    def __init__(self):
        self.is_writable = False
        self.is_multi_read = True

    def __setitem__(self, key, value):
        """
        Write/save a specific instance (identified by instance number)
        :param key:  the instance number, must be an integer
        :param value:  the instance
        :return:
        """
        raise Exception("Dataset is not writable!")


class ListDataset(ExtendedDataset):
    """
    A  list wrapped into a dataset.
    """

    def __init__(self, thelist):
        super().__init__()
        if not isinstance(thelist, list):
            raise Exception("Need a list!")
        self.data = thelist

    def __getitem__(self, key):
        return self.data[key]

    def __len__(self):
        return len(self.data)


class ShuffledDataset(ExtendedDataset):
    """
    Represents a shuffled version of another dataset. A shuffled dataset wraps an existing
    dataset by mapping the indices of the original dataset to a permutation of those indices.
    The shuffle method can be used to re-shuffle the dataset.
    """

    def __init__(self, dataset, seed=None):
        """
        :param seed: if an integer and > 0, shuffle the list of instances randomly, using the given seed.
        If the seed is 0, the RNGs random random seed is used, if seed is -1, the seed is not set at all
        and whatever the current state of the random generator is is used. If None, no shuffling is
        carried out. If this is None or not an integer, same as 0.
        """
        super().__init__()
        self.dataset = dataset
        self.seed = seed
        self.idxs = list(range(len(dataset)))
        self.shuffle(seed)

    def shuffle(self, seed=0):
        """
        Shuffle instance list order,
        :param seed: random seed to set, if seed is 0, a random random seed is used, if -1, seed is not set.
        If seed is None, no shuffling is carried out.
        :return:
        """
        if isinstance(seed, numbers.Integral):   # also allow for np.int8(n) and the like
            if seed != -1:
                if seed == 0:
                    random.seed()
                else:
                    random.seed(seed)
            random.shuffle(self.idxs)
        else:  # not an integer seed: None or some other type
            # same as seed 0
            random.seed()
            random.shuffle(self.idxs)

    def __getitem__(self, key):
        return self.dataset[self.idxs[key]]

    def __len__(self):
        return len(self.idxs)


class EveryNthDataset(ExtendedDataset):
    """
    Wraps a dataset to only provide every nth row, starting with the kth row.
    For example with n=3 and k=0, the rows 0,1,2,3,4 correspond to the
    rows 0,3,6,9,12 of the wrapped dataset, with n=3 and k=2, we get
    rows 2,5,8,11,14 etc. The wrapped dataset must allow to get used by more than
    one client at the same time!
    """

    def __init__(self, dataset, n, k):
        """
        Wrap dataset to access every nth row, starting with the kth row (k: zero-based).
        Important: if the wrapped dataset does not allow (reasonable) concurrent access, it may
        still be possible to wrap several separate dataset instances which all point to the
        same underlying resource (e.g. LineTsvDataset)
        :param dataset: the dataset to wrap, must allow multiple concurrent access
        :param n: the increment
        :param k: the offset, must be < n
        """
        super().__init__()
        if (not isinstance(n, numbers.Integral)) or (not isinstance(k, numbers.Integral)):
            raise Exception("n and k must be integers.")
        if n < 2 or k < 0 or k >= n:
            raise Exception("n must be >= 2 and k must be >= 0 and < n")
        self.n = n
        self.k = k
        self.dataset = dataset
        # precalculate the length
        otherlen = len(dataset)
        # the size of this dataset is int((otherlen + (n-k) - 1)/k)
        self.len = int((otherlen + (n - k) - 1) / k)

    def __getitem__(self, item):
        if not isinstance(item, numbers.Integral):
            raise Exception("Item must be an integer")
        if item >= self.len or item < 0:
            raise Exception("Item must be >= 0 and < {}".format(self.len))
        # the index to access in the original dataset is int(n*item)+k
        return self.dataset[item * self.n + self.k]

    def __len__(self):
        return self.len


# TODO: this should get replaced by ProcessingDataset where the transformations
# are really restricted to processing resource instances which can be pickled,
# so that this works properly with multiprocessing.
class TransformDataset(ExtendedDataset):

    def __init__(self, dataset, transforms):
        super().__init__()
        self.dataset = dataset
        if isinstance(transforms, list):
            self.transforms = transforms
        else:
            self.transforms = [transforms]

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, key):
        tmp = self.dataset[key]
        for tr in self.transforms:
            tmp = tr(tmp)
        return tmp


class LineTsvDataset(ExtendedDataset):
    """
    Represent a large TSV file or simple one document/text per line file as a dataset.
    When creating the instance, an index file is
    is created and stored along with the original file, unless it already exists.
    NOTE: this works only if lines are separated with "\n"!!!
    """

    def have_current_index(self):
        if not os.path.exists(self.indexfile):
            return False
        # if we have an index file, check if its modification date is more recent than
        # that of the data file: if not, return false
        return os.path.getmtime(self.indexfile) > os.path.getmtime(self.file)

    def __init__(self, file, indexfile=None, reinit=False,
                 encoding="utf8", cols=None, logevery=1000):
        """
        Create the dataset instance from the given file.
        :param file: the tsv file
        :param indexfile: index file to use, by default the original file path with ".dsindex" appended
        :param reinit: if True, forces re-creation of the index file even if it already exists
        :param if cols is None, the whole line is returned to the iterator, otherwise if it is a number, that
          column is returned, otherwise if it is a list of numbers, those fields are returned
        """
        self.reader = None   # set this first so even if the super init throws an exception, __del__ still finds it
        super().__init__()
        self.file = file
        if not os.path.exists(file):
            raise Exception("File does not exist: {}".format(file))
        if indexfile is None:
            indexfile = file + ".dsindex"
        self.indexfile = indexfile
        self.encoding = encoding
        self.cols = cols
        self.logevery = logevery
        # if we need to create the cache file, do this now.
        if reinit or not self.have_current_index():
            self.idx2offlen = self._index4file(file)
            with open(indexfile, "wb") as indexwriter:
                pickle.dump(self.idx2offlen, indexwriter)
        else:
            with open(indexfile, "rb") as indexloader:
                self.idx2offlen = pickle.load(indexloader)
        self.len = len(self.idx2offlen)

    def __del__(self):
        # print("DEBUG: calling __del__")
        if self.reader is not None:
            # print("DEBUG: closing reader!")
            self.reader.close()

    def _index4file(self, file):
        idx2offlen = []
        with open(file, "rb") as reader:
            startoffset = 0
            linenr = 0
            # since this is reading in binary mode, the terminator is always "\n"
            # NOTE: we could also read in text mode, specify the newline or automatically
            # recognize both both Windows and Linux newlines and then count by encoding the
            # utf8 string we get into bytes and hope for the best. However, we expect all
            # line corpora to be in Linux format for now!
            for linebytes in reader:
                # line = bytes.decode(self.encoding)
                linelen = len(linebytes)
                idx2offlen.append((startoffset, linelen))
                # print("DEBUG indexing {}/{}".format(startoffset,l))
                startoffset += linelen
                linenr += 1
                if self.logevery is not None and linenr % self.logevery == 0:
                    print("Lines indexed: {}".format(linenr), file=sys.stderr)
        return idx2offlen

    def __len__(self):
        return self.len

    def __getitem__(self, index):
        if self.reader is None:
            # print("DEBUG: opening reader!")
            self.reader = open(self.file, "rb")
        if index >= self.len or index < -self.len:
            raise IndexError()
        off, linelen = self.idx2offlen[index]
        self.reader.seek(off, os.SEEK_SET)
        bytes = self.reader.read(linelen)
        line = bytes.decode(self.encoding)
        if self.cols is None:
            return line
        else:
            fields = line.split("\t")
            if isinstance(self.cols, list):
                return [fields[i] for i in self.cols]
            else:
                return fields[self.cols]


class DirFilesDataset(ExtendedDataset):

    def path4(self, index):
        if not self.is_dynamic:
            if not isinstance(index, numbers.Integral) or index < 0:
                raise Exception("Dataset is not dynamic and index is not a non-negative integer: {}".format(index))
            if index >= self.len:
                raise Exception("Index {} larger than maximum item number in dataset {}".format(index, self.len))
        if self.path4id is not None:
            name = self.path4id(index)
        elif self.is_dynamic and isinstance(index, str):
            name = index
        elif self.paths is not None:
            name = self.paths[index]
        else:
            raise Exception("ODDD!!")
        if self.ext is not None:
            fname = name + self.ext
        else:
            fname = name + "." + self.as_format
        fpath = os.path.join(self.directory, fname)
        return fpath

    def get_dir_paths(self):
        """
        Return a list of matching files in the directory (or directory tree).

        :return: list of files in some order.
        """
        files = []
        if self.ext is not None:
            toendwith = self.ext
        else:
            toendwith = "." + self.as_format
        endlen = len(toendwith)
        for r, d, fs in os.walk(self.directory, topdown=True, followlinks=True):
            files.extend((f[0:-endlen] for f in fs if f.endswith(toendwith)))
            if not self.tree:
                break
        return files


    def load_paths(self):
        """
        Create the list of file paths and store it in self.paths.
        If self.paths is a string, try to load the paths list from that file. If it does not exist, load
        by finding the matching files in the directory.

        :return:
        """
        if isinstance(self.paths, str):
            fname = os.path.join(self.directory, self.paths)
            if os.path.exists(fname):
                if fname.endswith(".pickle"):
                    with open(fname, "rb") as fp:
                        self.paths = pickle.load(fp)
                elif fname.endswith(".json"):
                    with open(fname, "rt") as fp:
                        self.paths = json.load(fp)
                else:
                    self.paths = []
                    with open(fname, "rt", encoding="utf-8") as fp:
                        for line in fp:
                            line = line.rstrip()
                            self.paths.append(line)
                assert isinstance(self.path, list)
            else:
                self.paths = self.get_dir_paths()
                if fname.endswith(".pickle"):
                    with open(fname, "wb") as fp:
                        self.paths = pickle.dump(self.paths, fp)
                elif fname.endswith(".json"):
                    with open(fname, "wt") as fp:
                        self.paths = json.dumo(self.paths, fp)
                else:
                    self.paths = []
                    with open(fname, "wt", encoding="utf-8") as fp:
                        for f in self.paths:
                            print(f, file=fp)
        elif self.paths is None:
            self.paths = self.get_dir_paths()
        else:
            raise Exception("Odd: not loading path if it is not None or a file path, but {}".format(type(self.paths)))
        self.len = len(self.paths)

    def __init__(self, directory, as_format='pickle', paths=None, tree=False, path4id=None, is_writable=True,
                 ext=None, size=0, is_dynamic=False):
        """
        Create a dataset where instances are files in a directory. This can be either a normal dataset where the size
        needs to be known in advance or, if "is_dynamic" is True, something that allows to grow the dataset
        and set non-integer keys (see below).

        By default, the content of this dataset is the content of the directory (recursively if tree is not False),
        restricted to all the files that match the extension implied by the format. The mapping between
        row number and file path is determined at creation time and immutable afterwards.

        If paths is given, then either a list is expected that maps relative file paths to each id, or
        a string that identifies a file in the directory that contains all the file paths and which will
        get loaded at inite time. If that file does not exist, it is created on the fly.

        The size of the dataset is the size of the list (unless is_dynamic).
        Accessing an item where the file does not exist returns None.
        That item can be changed into a different value by storing it in which case the file from the
        paths list is created.

        Finally, if path4id is given, it must be a callable that returns a filename for a row number.

        If is_dynamic is True: the path4id function, if given, is not used to get the size, only numbers >= 0 are used.
        The initial size of the dataset is set to the size we got from the initial paths and may get increased if
        a row with a larger index is used, but there is no guarantee at all about the size of a dynamic dataset,
        len should never be used for such a dataset! Note that accessing rows by numeric id will not work for any
        row that is outside the known range.

        :param directory: the directory to use for the files representing each row in the dataset
        :param as_format: the format to use, currently this also has to be the file extension to use, one of
        pickle, json, torch.
        :param paths: a list of relative file paths (without the extension!!!)
        :param tree: if True, recurse the directory to find the initial file names, only relevant if paths and
        path4id are not specified.
        :param path4id: if specified, must be a callable that returns a relative path (without extension)
         for a row number (>=0)
        :param ext: if not None, the  file extension to use for all the files, default is the format name
        :param size: specify the size if path4id is used. If is_dynamic is true and path4id is used, specify the
        initial size.
        :param is_dynamic: if True, the size of the dataset is not fixed, writing to a new id/key will increase the
        size. The id when reading can be a string when reading or writing. if path4id is defined, the string
        is passed to that function and a path is expected. If path4id is not specified and the id is a string,
        then that string is directly interpreted as a path.
        """
        super().__init__()
        self.directory = directory
        self.is_writable = is_writable
        self.paths = paths
        self.tree = tree
        self.ext = ext
        self.is_dynamic = is_dynamic
        if as_format not in ['pickle', 'json', 'torch']:
            raise Exception("Format must be one of pickle, json, torch")
        self.as_format = as_format
        self.path4id = path4id
        if paths is None and path4id is None:
            # get all the matching path names, either just in the directory or recursively and store
            self.load_paths()
        elif isinstance(paths, str) and path4id is None:
            self.load_paths()
        elif isinstance(paths, list) and path4id is None:
            self.len = len(self.paths)
        elif path4id and paths is None:
            self.len = size
        else:
            raise Exception("Cannot use both path4id and paths at the same time, one or none of them needed")

    def __len__(self):
        return self.len

    def __getitem__(self, index):
        fpath = self.path4(index)
        if not os.path.exists(fpath):
            return None
        if self.as_format == "json":
            with open(fpath, "rt", encoding="utf8") as reader:
                return json.load(reader)
        elif self.as_format == "pickle":
            with open(fpath, "rb") as reader:
                return pickle.load(reader)
        elif self.as_format == "torch":
            import torch
            with open(fpath, "rb") as reader:
                return torch.load(reader, map_location="cpu")

    def __setitem__(self, index, value):
        fpath = self.path4(index)
        parent = pathlib.Path(fpath).parent
        if not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        if self.as_format == "json":
            with open(fpath, "wt", encoding="utf8") as writer:
                json.dump(value, writer)
        elif self.as_format == "pickle":
            with open(fpath, "wb") as writer:
                pickle.dump(value, writer)
        elif self.as_format == "torch":
            import torch
            with open(fpath, "wb") as writer:
                torch.save(value, writer)


class CachedDataset(ExtendedDataset):

    def __init__(self, basedataset, cachedataset, cacheonread=False):
        """
        Create a caching dataset. This will access data from the cachedataset, if it does not exist in there (entry is,
        None) will instead fall back to the base dataset. In other words, a cache dataset must be set up as a
        direct access dataset that is capable of returning None for non-existing items.

        The cache can be set up to cache on read or cache on write.

        NOTE: both datasets should maybe have the same size already but this is not checked so that a dynamic
        DirFilesDataset instance can be used as well!

        :param basedataset: any dataset
        :param cachedataset: any ExtendedDataset which allows for empty slots to be represented as None
        :param cacheonread: if True, writes to the cache as soon as an item has been read from the base dataset.
        Otherwise will only write to the cache dataset when an item is set. This allows to cache the result
        of processing efficiently.
        """
        super().__init__()
        self.is_writable = True
        self.basedataset = basedataset
        self.cachedataset = cachedataset
        self.cacheonread = cacheonread

    def __len__(self):
        return len(self.basedataset)

    def __getitem__(self, index):
        tmp = self.cachedataset[index]
        if tmp is None:
            tmp = self.basedataset
            if self.cacheonread:
                self[index] = tmp
        return tmp

    def __setitem__(self, index, value):
        self.cachedataset[index] = value


if __name__ == "__main__":
    # just run a quick sanity test
    with open("tmp_linetsvdataset.tsv", "wt", encoding="utf8") as writer:
        print("this is the first line!", file=writer)
        print("Some umlauts like ä or Ü or ś and Ñ and ì...", file=writer)
        print("this is another line", file=writer)
        print("and another", file=writer)
        print("Last one!!!", file=writer)

    ds = LineTsvDataset(file="tmp_linetsvdataset.tsv", reinit=True)
    for i, line in enumerate(ds):
        print("LineTsvDataset line {}:".format(i), line)

    print("Last line: ", ds[-1])
    print("First line: ", ds[-5])

    from torch.utils.data import DataLoader

    def cfn1(l):
        print("We got:",l)
        return l

    dl = DataLoader(ds, batch_size=2, shuffle=True, collate_fn=cfn1)

    for batch in dl:
        print("Batch: ", batch)

    ds2tmp = LineTsvDataset(file="tmp_linetsvdataset.tsv", reinit=False)
    ds2 = TransformDataset(ds2tmp, len)
    dl2 = DataLoader(ds2, batch_size=2, shuffle=True)
    for batch in dl2:
        print("Batch2: ", batch)

