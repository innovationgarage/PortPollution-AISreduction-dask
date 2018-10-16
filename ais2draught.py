from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import pyspark.sql.functions as func
from datetime import datetime
import argparse
import msgpack
import os
import sys
import csv
import contextlib

def read_msgs(infilepath):
    try:
        with open(infilepath, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False)
            for msg in unpacker:
                yield msg
    except:
        print('{} does not exist!'.format(infilepath))

def timestamp2date(msg):
    msg['date'] = msg['timestamp'].split('T')[0]
    return msg

@contextlib.contextmanager
def find_unprocessed_files(inpath, lastfilerec):
    infilenames = os.listdir(inpath)
    infilepaths = [os.path.join(inpath, x) for x in infilenames]
    infiles = [(x, os.path.getmtime(x)) for x in infilepaths]
    
    print('Processing AIS messages from {}'.format(args.aispath))

    infiles_rdd = sc.parallelize(infiles)
    infilepaths_rdd_sorted = infiles_rdd.sortBy(lambda x: x[1], ascending=False)
    
    last_modified_file = list(infilepaths_rdd_sorted.first())
    print('last_modified_file', last_modified_file)

    infilepaths_rdd_sorted = infilepaths_rdd_sorted.filter(lambda x: x==last_modified_file)    
    if os.path.isfile(args.lastfilerec):
        with open(lastfilerec) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                last_processed_file = [row[0], float(row[1])]
    else:
        last_processed_file = [None, 0]        

    print('last_processed_file', last_processed_file)
    new_infiles_rdd = infiles_rdd.filter(lambda x: float(x[1])>last_processed_file[1]).filter(lambda x: float(x[1])<last_modified_file[1])

    yield last_modified_file, new_infiles_rdd

    print('Remembering the last modified file for future reference')    
    with open(lastfilerec, mode='w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([last_modified_file[0], last_modified_file[1]])


def ais_w_draught_as_df(ais_w_draught):
    schema = StructType([
        StructField("draught", FloatType(), True),
        StructField("timestamp", StringType(), True),
        StructField("date", StringType(), True),        
        StructField("type", IntegerType(), True),
        StructField("mmsi", StringType(), True),
        StructField("ship_type", IntegerType(), True),
        StructField("length", FloatType(), True),
        StructField("spare", StringType(), True),
        StructField("spare2", StringType(), True),
        StructField("class", StringType(), True),
        StructField("scaled", StringType(), True),
        StructField("course_qual", StringType(), True),
        StructField("speed_qual", StringType(), True),
        StructField("device", StringType(), True),
        StructField("heading_qual", StringType(), True),
        StructField("haz_cargo", StringType(), True),
        StructField("nmea", StringType(), True),
        StructField("eu_id", StringType(), True),
        StructField("dac", StringType(), True),
        StructField("loaded", StringType(), True),
        StructField("tagblock_timestamp", StringType(), True),
        StructField("beam", StringType(), True),
        StructField("fid", StringType(), True),
        StructField("repeat", StringType(), True)
    ])        
    
    return spark.createDataFrame(ais_w_draught, schema)


def infiles_to_draught(new_infiles_rdd, tstrec, last_modified_file, draughtpath):
    ais_unpacked_rdd = new_infiles_rdd.flatMap(lambda x: read_msgs(x[0]))
    ais_w_mmsi = ais_unpacked_rdd.filter(lambda x: 'mmsi' in x.keys()).map(timestamp2date)
    ais_w_draught = ais_w_mmsi.filter(lambda x: 'draught' in x.keys())
    ais_w_draught.persist()

    draught_df = ais_w_draught_as_df(ais_w_draught)

    print('Writing AIS messages with draught measurements to {}'.format(os.path.join(draughtpath, str(datetime.now().date()))))
    draught_df.write.format("parquet").mode('overwrite').option("header", "true").save(os.path.join(draughtpath, str(datetime.now().date())))

    print(draught_df.show())

    with open(tstrec, mode='w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([last_modified_file[0], last_modified_file[1]])
        csv_writer.writerow([ais_w_mmsi.first()])
        csv_writer.writerow([ais_w_draught.first()])


if __name__ == "__main__":

    '''
    Usage: 
        python ais2draught.py --aispath aishub/ --draughtpath draught/ --rddpath draught_rdd/ --lastfilerec lastfile.rec

    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--aispath', type=str, default='aishub', help='Path to where AIS messages are stored (msgpack format required)')
    parser.add_argument('--draughtpath', type=str, default='draught', help='Path to where AIS messages with draught value are stored (in parquet format)')
    parser.add_argument('--lastfilerec', type=str, default='draught_lastfile.rec', help='Path to a file containing the name of the last processed file')
    parser.add_argument('--tstrec', type=str, default='draught_lastfile.rec', help='Path to a file containing the name of the last processed file')
    
    parser.set_defaults()
    args = parser.parse_args()
       
    spark = SparkSession\
        .builder\
        .appName("AIS2Draught")\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    sc.addPyFile(os.path.abspath("./dependencies.zip"))

    from datetime import datetime
    import argparse
    import msgpack
    import os
    import sys
    import csv

    if os.path.isfile(args.aispath):
        new_infiles_rdd = sc.parallelize([(args.aispath, os.path.getmtime(args.aispath))])
        infiles_to_draught(new_infiles_rdd, args.tstrec, args.aispath, args.draughtpath)
    else:
        with find_unprocessed_files(args.aispath, args.lastfilerec) as (last_modified_file, new_infiles_rdd):
            infiles_to_draught(new_infiles_rdd, args.tstrec, last_modified_file, args.draughtpath)

    spark.stop()
