import dask.distributed, distributed.client, dask.bag, daskutils.io.msgpack, daskutils.base, os.path, uuid, msgpack, daskutils.sort

#client = dask.distributed.Client('ymslanda.innovationgarage.tech:8786')
data = [uuid.uuid4().hex for a in range(0, 100000)]
s = daskutils.sort.MergeSort("/tmp/")
res = s.sort(dask.bag.from_sequence(data, npartitions=4))
res = res.compute()
assert len(res) == len(data)
assert res == sorted(res)
assert res == sorted(data)
