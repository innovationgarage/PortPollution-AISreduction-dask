import dask.distributed
import dask.bag as db
from datetime import datetime
import argparse
import msgpack
import os
import sys
import csv
import contextlib
import dask.bag
from dask.base import tokenize
from dask.bag.core import reify
from toolz import merge
from toolz.compatibility import zip as the_zip

def glom(bag):
    return bag.map_partitions(lambda i: [i])

def zipWithIndex(data):
    return dask.bag.zip(data, db.range(data.count().compute(), data.npartitions))

def read_msgs(infilepath):
    print("XXXXXXXXXXXXXXXX READING FILE", infilepath)
    try:
        with open(infilepath, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False)
            for msg in unpacker:
                yield msg
    except Exception as e:
        print('Unable to read {}: {}'.format(infilepath, e))

def write_msgs(data, outdirpath):
    data = zipWithIndex(glom(data))
    @data.map
    def write(item):
        lines, idx = item
        filename = "%s/test-output-%s.msgpack" % (outdirpath, idx)
        print("XXXXXXXXXXXXXXXX WRITING FILE", filename)
        with open(filename, 'wb') as f:
            for msg in lines:
                msgpack.dump(msg, f)
        return filename
    return write

if __name__ == "__main__":

    '''
    Usage: 
        python test.py outdir file1.msgpack ... fileN.msgpack

    '''
    client = dask.distributed.Client('ymslanda.innovationgarage.tech:8786')


    filenames = db.from_sequence(sys.argv[2:])
    filenames = filenames.map(lambda name: (name, os.path.getmtime(name)))

    print("XXXX Number of files: %s" % len(sys.argv[1:]))
    print("XXXX Number of partitions before reading files: %s" % filenames.npartitions)

    data = filenames.map(lambda x: read_msgs(x[0])).flatten()

    print("XXXX Number of partitions after reading files: %s" % data.npartitions)

    data = data.filter(lambda x: 'draught' in x.keys())

    print("XXXX Number of partitions after filtering: %s" % data.npartitions)

    print("Wrote", write_msgs(data, sys.argv[1]).compute())
