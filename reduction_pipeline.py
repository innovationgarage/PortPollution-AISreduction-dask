import dask.distributed
import distributed.client
import dask.bag as db
import daskutils.io.msgpack
import daskutils.base
import os
import uuid
from datetime import datetime
import argparse
import msgpack
from gributils import gribindex
from env import db_connect_string

if __name__ == "__main__":

    '''
    Usage: 
        python reduction_pipeline.py --aispath aishub/ --draughtpath draught/ --lastfilerec lastfile.rec

    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--aispath', type=str, default='aishub', help='Path to where AIS messages are stored (msgpack format required)')
    parser.add_argument('--outpath', type=str, default='outdir', help='Path to where AIS messages with new value that are read from weather data')
    parser.add_argument('--lastfilerec', type=str, default='draught_lastfile.rec', help='Path to a file containing the name of the last processed file')
    
    parser.set_defaults()
    args = parser.parse_args()

#    client = dask.distributed.Client('ymslanda.innovationgarage.tech:8786')
    
    # FIXME! add a last-red and last-mod file to know which files are to be considered
    filenames = db.from_sequence(os.listdir(args.aispath), partition_size=1)
    filenames = filenames.map(lambda name: os.path.join('aishub', name))
    
    filemods = filenames.map(lambda name: (name, os.path.getmtime(name)))

    data = daskutils.io.msgpack.read(filenames)
    data = data.filter(lambda x: 'lat' in x.keys())
    data = data.filter(lambda x: 'lon' in x.keys())
    data = data.filter(lambda x: 'timestamp' in x.keys())    

    def interp_parameter(msgs, parameter_name):
        gi = gribindex.GribIndex(db_connect_string)
        for msg in msgs:
            msg[parameter_name] = gi.interp_timestamp(lat=msg['lat'], lon=msg['lon'], timestamp=msg['timestamp'], parameter_name=parameter_name)
            yield msg
    
    data = data.map_partitions(interp_parameter, 'P Pressure')
    data = data.map_partitions(interp_parameter, 'Temperature')
    data = data.map_partitions(interp_parameter, 'U component of wind')
    data = data.map_partitions(interp_parameter, 'V component of wind')
    data = data.map_partitions(interp_parameter, 'Wave Height')
    data = data.map_partitions(interp_parameter, 'Wind Speed')
    data = data.map_partitions(interp_parameter, 'Wind Direction')
    
    dask_bag_of_filepaths = daskutils.io.msgpack.write(data, os.path.join(args.outpath, "ais-%s.msgpack"))
    dask_bag_of_filepaths.compute()

    
