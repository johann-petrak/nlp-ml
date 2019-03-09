#!/usr/bin/env python
'''
The various implementations of Dataset use different ways to make some 
standard representations of NLP data available this way.

Note: datasets can also use chaching to provide the list-like access.
When a dataset is created from a file, a cache may get created implicitly
or by providing an explicit Cache object. If an explicit Cache object is 
provided, then the internal caching is not used.
'''

import os
import pickle
import random
import sys
from torch.utils.data import Dataset


class ShuffledDataset(Dataset):
    """
    Represents a shuffled version of another dataset.
    """

    def __init__(self, dataset, seed=None):
        """
        :param seed: if not None, shuffle the list of instances randomly, using the given seed.
          If the seed is 0, the system time is used, if seed is -1, the seed is not actually set
        """
        self.seed = seed
        self.idxs = list(range(len(dataset)))
        self.shuffle(seed)


    def shuffle(self, seed=0):
        """
        Shuffle instance list order,
        :param seed: random seed to set, if seed is 0, system time is used, if -1, seed is not set.
        :return:
        """
        if seed != -1:
            if seed == 0:
                random.seed()
            else:
                random.seed(seed)
        random.shuffle(self.idxs)

    def __getitem__(self, key):
        return self.dataset[self.idxs[key]]

    def __len__(self, key):
        return len(self.idxs)


class TransformDataset(Dataset):

    def __init__(self, dataset, transforms):
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


class LineTsvDataset(Dataset):
    """
    Represent a large TSV file or simple one document/text per line file as a dataset.
    When creating the instance, an index file is
    is created and stored along with the original file, unless it already exists.
    NOTE: this works only if lines are separated with "\n"!!!
    """

    def __init__(self, file=None, indexfile=None, reinit=False, 
                 encoding="utf8", cols=None, logevery=1000):
        """
        Create the dataset instance from the given file.
        :param file: the tsv file
        :param indexfile: index file to use, by default the original file path with ".dsindex" appended
        :param reinit: if True, forces re-creation of the index file even if it already exists
        :param if cols is None, the whole line is returned to the iterator, otherwise if it is a number, that
          column is returned, otherwise if it is a list of numbers, those fields are returned
        """
        self.file = file
        if indexfile is None:
            indexfile = file + ".dsindex"
        self.indexfile = indexfile
        self.encoding = encoding
        self.cols = cols
        self.logevery = logevery
        # if we need to create the cache file, do this now.
        if reinit or not os.path.exists(indexfile):
            self.idx2offlen = self._index4file(file)
            with open(indexfile, "wb") as indexwriter:
                pickle.dump(self.idx2offlen, indexwriter)
        else:
            with open(indexfile, "rb") as indexloader:
                self.idx2offlen = pickle.load(indexloader)
        self.reader = open(self.file, "rb")
        self.len = len(self.idx2offlen)

    def __del__(self):
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
        if index >= self.len or index < -self.len:
            raise IndexError()
        off, len = self.idx2offlen[index]
        self.reader.seek(off, os.SEEK_SET)
        bytes = self.reader.read(len)
        line = bytes.decode(self.encoding)
        if self.cols is None:
            return line
        else:
            fields = line.split("\t")
            if isinstance(self.cols, list):
                return [fields[i] for i in self.cols]
            else:
                return fields[self.cols]


if __name__ == "__main__":
    # just run a quick sanity test
    with open("tmp_linetsvdataset.tsv", "wt", encoding="utf8") as writer:
        print("this is the first line!", file=writer)
        print("Some umlauts like ä or Ü or ś and Ñ and ì...", file=writer)
        print("this is another line", file=writer)
        print("and another", file=writer)
        print("Last one!!!", file=writer)

    ds = LineTsvDataset(file="tmp_linetsvdataset.tsv", reinit=True)
    for line in ds:
        print(line)

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
