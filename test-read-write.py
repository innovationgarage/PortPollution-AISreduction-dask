import dask.distributed
import dask.bag
import daskutils.io.msgpack

client = dask.distributed.Client('ymslanda.innovationgarage.tech:8786')

names = dask.bag.from_sequence(["/ymslanda/PortPollution/ais/aishub/aishub-2018-09-24T03:20:39+00:00-%s.msgpack" % x for x in range(0, 1)])
data = daskutils.io.msgpack.read(names)
outnames = daskutils.io.msgpack.write(data, "/ymslanda/PortPollution/ais/test/testout.%s.msgpack")
print(outnames.compute())
