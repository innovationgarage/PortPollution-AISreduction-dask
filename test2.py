import dask.distributed
import dask.bag
import daskutils.io.msgpack
import msgpack

def bag_group_by(bag, grouper):
    return bag.foldby(
        grouper,
        lambda a, b: a + (b,), (),
        lambda a, b: dask.bag.concat([a, dask.bag.from_sequence(b)]),
        dask.bag.from_sequence([]))

def read(filenames):
    def read(infilepath):
        def read():
            with open(infilepath, 'rb') as f:
                unpacker = msgpack.Unpacker(f, raw=False)
                for msg in unpacker:
                    yield msg
        return dask.bag.from_sequence(read(), partition_size=1000)
    return filenames.map(lambda x: read(x)).flatten()

client = dask.distributed.Client('ymslanda.innovationgarage.tech:8786')
names = dask.bag.from_sequence(["/ymslanda/PortPollution/ais/aishub/aishub-2018-09-24T03:20:39+00:00-%s.msgpack" % x for x in range(0, 1)])
data = read(names)
bymmsi = bag_group_by(data, lambda x: x.get("mmsi", None))
mmsi1 = bymmsi.take(1)
