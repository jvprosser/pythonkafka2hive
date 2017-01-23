#!/usr/bin/env python
import threading, logging, time, sys, ConfigParser
from optparse import OptionParser                                                                                                                                                                                                 
import string 
import ast
from datetime import datetime, timedelta
import io
import avro.schema
import avro.io
import pprint
import kudu
from kudu.client import Partitioning
import sys
from kafka.structs import TopicPartition
from kafka import KafkaConsumer

from pyspark.sql.types import IntegerType,LongType,StringType,StructType,StructField
from pyspark.streaming.kafka import KafkaUtils,OffsetRange
from pyspark.streaming.kafka import Broker, KafkaUtils, OffsetRange, TopicAndPartition

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.rdd import RDD
from pyspark import SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql import HiveContext, Row

# DONT FORGET  . /etc/spark/conf/spark-env.sh
# . /etc/spark/conf/spark-env.sh 
# export JAVA_HOME=/usr/java/latest
# export PYTHONPATH=/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/spark/python:/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/spark/python/lib/py4j-0.9-src.zip 
# ALSO
# 


from ConfigParser import SafeConfigParser
# 
# parser = SafeConfigParser()
# parser.read('simple.ini')
# 
# print parser.get('bug_tracker', 'url')


pp = pprint.PrettyPrinter(indent=4,width=80)

twitter_schema='''
{"namespace": "example.avro", "type": "record",
 "name": "StatusTweet",
 "fields": [
     {"name": "kid"         ,  "type": "int"},
     {"name": "tweet_id"       ,  "type": "long"},
     {"name": "followers_count",  "type": "int"},
     {"name": "statuses_count" ,  "type": "int"},
     {"name": "id_str"         ,  "type": "string"},
     {"name": "friends_count"  ,  "type": "int"},
     {"name": "text"           ,  "type": "string"},
     {"name": "tweet_ts"       ,  "type": "long"},
     {"name": "screen_name"    ,  "type": "string"},
     {"name": "mon", "type": ["null", "int"]} ,
     {"name": "day", "type": ["null", "int"]} ,
     {"name": "hour", "type": ["null", "int"]}

 ]
}
'''

struct1 = StructType([StructField("kid", IntegerType(), True),
                      StructField("id_str", StringType(), True),
                      StructField("tweet_ts", LongType(), True),
                      StructField("tweet_id", IntegerType(), True),
                      StructField("followers_count", IntegerType(), True),
                      StructField("statuses_count", IntegerType(), True),
                      StructField("friends_count", IntegerType(), True),
                      StructField("text", StringType(), True),
                      StructField("screen_name", StringType(), True),
                      StructField("mon", IntegerType(), False),
                      StructField("day", IntegerType(), False),
                      StructField("hour", IntegerType(),False)])



schema = avro.schema.parse(twitter_schema)
offsetRanges = []

def nullDecoder(s):
    if s is None:
        return None
    return s

def getSparkConfItem(sc,item):
    val = None
    allconfs = sc._conf.getAll()
    indexList = [i for i,d in enumerate(allconfs) if item in d]
    index = indexList[0]
    if index is not None:
        print 'index was ' + str(index)
        pp.pprint(allconfs[index])
        val = allconfs[index][1]

    return val

#
# given two dates in the format YYYYMMDDTHH:MM
# return a set consisting of the begin offset for the first
# and the endoffset for the second
# i.e. the offset range for the timestamps
#
def getOffsetRangeFromHBase(sc, begints,endts):

    #appname = sc.getLocalProperty('boa_atm.appname')
    appname = getSparkConfItem(sc,'boa_atm.appname') 
    
    # the order is backwards -> startrow is the end timestamp, stoprow is the beginning 
    # because we reverse the order of the timestamps to get most recent at the top of the scan
    startrow  = appname + str(sys.maxint - endts)
    stoprow = appname + str(sys.maxint - begints)

    begin_offset = None
    end_offset = None

    hbase_conf = {"hbase.zookeeper.quorum": "ip-172-31-10-233:2181",\
                  "zookeeper.znode.parent": "/hbase",\
                  "hbase.mapreduce.inputtable": "kafkaoffsets",\
                  "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",\
                  "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable",\
                  "hbase.mapreduce.scan.row.start": startrow,
                  "hbase.mapreduce.scan.row.stop": stoprow
    }

    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

    print 'getting an rdd'

    rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                             "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                             "org.apache.hadoop.hbase.client.Result",
                             keyConverter=keyConv, valueConverter=valueConv, conf=hbase_conf)
    
    print 'done getting rdd'
    val = rdd.collect()
    print 'start val is'
    pp.pprint(val)
    print 'val end'

    rdd1 = rdd.flatMapValues(lambda v: v.split("\n"))
    nval  = rdd1.collect()
    print 'nval is'

    pp.pprint(nval[0][1])
    pp.pprint(nval[1][1])

    pp.pprint(nval[-1][1])
    pp.pprint(nval[-2][1])

    # later entries come first
    enval = ast.literal_eval(nval[0][1])
    if enval['qualifier'] == 'start':
        end_offset=enval['value']
    else:
        enval = ast.literal_eval(nval[1][1])
        end_offset=enval['value']


    enval = ast.literal_eval(nval[-1][1])
    if enval['qualifier'] == 'end':
        begin_offset=enval['value']
    else:
        enval = ast.literal_eval(nval[-2][1])
        begin_offset=enval['value']
#    pp.pprint(nval)
    print 'nval end'

    return (begin_offset, end_offset)

def totimestamp(datehour, epoch=datetime(1970,1,1)):
    dh = datetime.strptime(datehour,'%Y%m%dT%H:%M')
    td = dh - epoch
    # return td.total_seconds()
    return  int( (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6 )


#
# this method converts the avro record into a python dictionary
# also converts the timestamp into mon,day,hour fields for partitions
#
def deserializeTwitter(tweet):
    import io
    import avro.schema
    import avro.io
    from pyspark.sql import  Row
    from datetime import datetime

    bytes_reader = io.BytesIO(tweet)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader  = avro.io.DatumReader(schema)
    data = reader.read(decoder)
    dt   = datetime.fromtimestamp(data['tweet_ts']/1000)
    data['mon'] = dt.month 
    data['day'] = dt.day   
    data['hour']= dt.hour  
    return dict(data)


def storeAsHiveTempTable(rdd,dbtable,sql):
    print "in storeAsHiveTempTable"

    if rdd.count() > 0:
        (db,table) = dbtable.split(".")
        sc = rdd.context

        sql_list = getSparkConfItem(sc,'boa_atm.sql_list')
        print "rdd not empty"
        print "using sqls: " + sql_list
        message = rdd.first()

        print "message was" 
        pp.pprint(message)
        print "DONE message was"

        bytes_reader = io.BytesIO(message)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        
        data = reader.read(decoder)

        parseRDD = rdd.map(lambda x: deserializeTwitter(x))

        hiveContext = HiveContext(sc)
        hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        hiveContext.setConf("hive.exec.dynamic.partition",'true')

        df = hiveContext.createDataFrame(parseRDD,struct1)
        print "DONE: getting context hiveContext.createDataFrame"

##        df = hiveContext.createDataFrame(parseRDD,struct1)
##        print "DONE: getting context hiveContext.createDataFrame"
##
##        hiveContext.registerDataFrameAsTable(df, "StatusTweetsTemp")
##        df2 = hiveContext.sql("SELECT kid,id_str,tweet_ts,tweet_id,followers_count,statuses_count,friends_count,upper(text),screen_name,mon,day,hour FROM StatusTweetsTemp")
##
##        df2.write.saveAsTable('BOA_ATM.statustweetstemp',format='parquet',mode='append',partitionBy=('mon','day','hour'))

        print "getting context hiveContext.createDataFrame"

        df = hiveContext.createDataFrame(parseRDD,struct1)

        print "DONE: getting context hiveContext.createDataFrame and registering to table: " +table

        # "SELECT kid,id_str,tweet_ts,tweet_id,followers_count,statuses_count,friends_count,upper(text),screen_name,mon,day,hour FROM StatusTweetsTemp"
        #df2 = hiveContext.sql(sql)
        i=0
        for sql in  sql_list.split("|"):
            hiveContext.registerDataFrameAsTable(df, table)
            sql += " from " + table
            print "applying sql: " + sql
            df = hiveContext.sql(sql)
            print "sql data reults"
            pp.pprint(df.take(5))

        df.write.saveAsTable(db+'.'+table,format='parquet',mode='append',partitionBy=('mon','day','hour'))    

#        sql="SELECT kid,id_str,tweet_ts,tweet_id,followers_count,statuses_count,friends_count,upper(text),screen_name,mon,day,hour "
#        table = "statustweetstemp"
#        hiveContext.registerDataFrameAsTable(df, table)
#        sql += " from " + table
#        print "applying sql: " + sql
#        df2 = hiveContext.sql(sql)

#        hiveContext.registerDataFrameAsTable(df2, table)



    print "LEAVING storeAsHiveTempTable"

class Consumer(threading.Thread):

    @staticmethod    
    def storeOffsetInHbase(rdd):
        print "in storeOffsetInHbase"
        offsetRanges = rdd.offsetRanges()
    
        if rdd.count() > 0:
            print "rdd not empty"
    
            sc = rdd.context
            # could not get set/getlocalproperty() to work
            allconfs=sc._conf.getAll()
#            print '\n\nALLCONFS'
#            pp.pprint(allconfs)
 
            indexList = [i for i,d in enumerate(allconfs) if 'boa_atm.appname' in d]
            index = indexList[0]
            if index is not None:
                print 'index was ' + str(index)
                pp.pprint(allconfs[index])
                appname = allconfs[index][1]
            else:
                print "boa_atm.appname was not found in allconfs"

            thing = rdd.first()
            # the key is timestamp
            tweet_ts = thing[0]
            # the value is the tweet as an avro object
            message = thing[1]
    
            for o in offsetRanges:
                print "SOR: topic: %s partition: %s from: %s to: %s  tstamp: %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset,tweet_ts  )
                # subtract from maxint in order to reverse order of timestamps. this puts the most recent at the beginning of the scan                                                                        
                rowkey = appname+str(sys.maxint - long(tweet_ts)/1000 )
    
                hbase_conf = {"hbase.zookeeper.quorum": "ip-172-31-10-233:2181",\
                        "zookeeper.znode.parent": "/hbase",\
                        "hbase.mapred.outputtable": "kafkaoffsets",\
                        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",\
                        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",\
                        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
                
                keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
                valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    
                print '\nsavng offsets'
                mrdd = sc.parallelize([(rowkey, 'tp_offsets', 'start',str(o.fromOffset)),(rowkey, 'tp_offsets', 'end',str(o.untilOffset))])
                load_rdd = mrdd.map(lambda x: (str(x[0]),[str(x[0]),x[1],x[2],x[3]]))
    
                load_rdd.saveAsNewAPIHadoopDataset(conf=hbase_conf,keyConverter=keyConv,valueConverter=valueConv)
                print "DONE SAVING START and end now\n"
    
    
            print "removing kafka key and creating avro RDD"
            avroRDD=rdd.map(lambda x: x[1])
            print "DONE removing kafka key and creating avro RDD"
        else:
            avroRDD=rdd
        return avroRDD
    
    

    #################################
    @staticmethod
    def storeAsHiveTable(rdd):

        print "in storeAsHiveTable"
        if rdd.count() > 0:
            sc = rdd.context
            allconfs=sc._conf.getAll()
#            print '\n\nALLCONFS'
#            pp.pprint(allconfs)


            indexList = [i for i,d in enumerate(allconfs) if 'boa_atm.tablename' in d]
            index = indexList[0]
            tablename = allconfs[index][1]

#            tablename = sc.getLocalProperty('boa_atm.tablename')
    
            print "rdd not empty, writing data to " + tablename
            message = rdd.first()
    
            bytes_reader = io.BytesIO(message)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(schema)
            
            data = reader.read(decoder)
    
            parseRDD = rdd.map(lambda x: deserializeTwitter(x))
    
            v=parseRDD.take(1)
            print 'this is what the data looks like:'
            pp.pprint(v)

            hiveContext = HiveContext(sc)
            hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
            hiveContext.setConf("hive.exec.dynamic.partition",'true')
    
            print "getting context hiveContext.createDataFrame"
    
            df = hiveContext.createDataFrame(parseRDD,struct1)
            print "DONE: getting context hiveContext.createDataFrame"
    
            hiveContext.registerDataFrameAsTable(df, tablename)
            df.write.saveAsTable(tablename,format='parquet',mode='append',partitionBy=('mon','day','hour'))
    
    #        df = hiveCtx.read.format("com.databricks.spark.avro")
    #
    
    #        schemaRDD = hiveCtx.inferSchema(rdd)
    #        schemaRDD.registerTempTable("StatusTweets")
    #        # Make a UDF to tell us how long some text is
    #        hiveCtx.registerFunction("strLenPython", lambda x: len(x), IntegerType())
    #        lengthSchemaRDD = hiveCtx.sql("SELECT strLenPython('text') FROM StatusTweet LIMIT 10")
    #        print lengthSchemaRDD.collect()        
            
    
        print "LEAVING storeAsHiveTable"
    


    daemon = True
    def __init__(self, beginOffset, name, partition_list,sc ):

        threading.Thread.__init__(self)
        self.sc =  sc
#        self.sc.setLocalProperty('boa_atm.startoffset',beginOffset)
#        self.sc.setLocalProperty('boa_atm.tablename',table)
#        self.sc.setLocalProperty('boa_atm.appname',appname)
#
        self.name = name

        self.ssc = StreamingContext(self.sc, 180)

        if beginOffset is not None:
            fromOffsets = {TopicAndPartition(topic, 0): long(beginOffset)}
            self.directKafkaStream = KafkaUtils.createDirectStream(self.ssc,[ "twitterstream" ], {"metadata.broker.list": 'ip-172-31-10-235:9092'},fromOffsets,keyDecoder = nullDecoder,valueDecoder=nullDecoder)
        else:
            self.directKafkaStream = KafkaUtils.createDirectStream(self.ssc,[ "twitterstream" ], {"metadata.broker.list": 'ip-172-31-10-235:9092'},keyDecoder = nullDecoder,valueDecoder=nullDecoder)

#        self.topicPartition = namedtuple(
        self.partitions = partition_list

#        self.client = kudu.connect(host='ip-172-31-6-171', port=7051)
        # Open a table
#        self.table = self.client.table('impala::DEFAULT.STATUS_TWEETS')

        # Create a new session so that we can apply write operations
#        self.session = self.client.new_session()

    def run(self):

        print "in run"
        try:
            kvs = self.directKafkaStream\
                .transform(self.storeOffsetInHbase)\
                .foreachRDD(self.storeAsHiveTable)

            self.ssc.start()
            self.ssc.awaitTerminationOrTimeout(500000)

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')


def main(appname,options,sql ):
    print "in main"

    fromts=options.FROM_TIMESTAMP
    tots=options.TO_TIMESTAMP

    table=options.TABLE
    beginOffset = None
    endOffset = None

    conf = SparkConf()
    conf.setMaster('yarn-client')
    conf.setAppName(appname)

    # could not get sc.setLocalProperty to work...
    conf.set('boa_atm.tablename',table)
    conf.set('boa_atm.appname',appname)
    conf.set('boa_atm.sql_list',sql)

    sc = SparkContext(conf=conf)

    if tots is not None:
        dtstart= totimestamp(fromts)
        dtend  = totimestamp(tots)
        (beginOffset,endOffset) = getOffsetRangeFromHBase(sc, dtstart,dtend)
    else:
        beginOffset  =options.BEGIN_OFFSET
        endOffset  =options.END_OFFSET

    pp.pprint(options)

    if endOffset is not None:
        print "begin at " + str(beginOffset)
        print "end at " + str(endOffset)
        ssc = StreamingContext(sc, 200)
    
        kafkaParams = {"metadata.broker.list": 'ip-172-31-10-235:9092'}
        partition = 0
        topic = 'twitterstream'
        offset = OffsetRange(topic,partition,int(beginOffset),int(endOffset))
        offsets = [offset]
        kafkaRDD = KafkaUtils.createRDD(sc, kafkaParams,offsets,keyDecoder = nullDecoder,valueDecoder=nullDecoder)
        # strip kafka key part
        messageRDD=kafkaRDD.map(lambda x: x[1])
        storeAsHiveTempTable(messageRDD,table,sql)

    else :
        print "starting streaming to " + table
        threads = [
            #            Consumer(beginOffset,table,appname,"consumer1",(3),sc)
            Consumer(beginOffset,"consumer1",(3),sc)
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )



    # Prep command line argument parser
    parser = OptionParser()
    parser.add_option('-f', '--fromTimestamp', dest='FROM_TIMESTAMP', default=None, help='The point in time to begin processing in single batch mode. in this format YYYYMMDDTHH:MM (e.g. 20170119T21:40)')
    parser.add_option('-t', '--toTimestamp'  , dest='TO_TIMESTAMP'  , default=None, help='The point in time to end processing in single batch mode. in this format YYYYMMDDTHH:MM (e.g. 20170119T21:40)')
    parser.add_option('-b', '--beginOffset'  , dest='BEGIN_OFFSET'  , default=None, help='The Kafka offset to begin processing in single batch mode. ')
    parser.add_option('-e', '--endOffset'    , dest='END_OFFSET'    , default=None, help='The Kafka offset to end processing in single batch mode. ')
#    parser.add_option('-D', '--Database'     , dest='DATABASE'      , default=None, help='The database to write to.')
    parser.add_option('-T', '--Table'    , dest='TABLE'     , default=None, help='The table to write to.')
    parser.add_option('-i', '--inifile'    , dest='INIFILE'     , default=None, help='The ini file to get params from')

    (options, args) = parser.parse_args()

    # Prep for reading config props from external file
    CONFIG = ConfigParser.ConfigParser()
    CONFIG.read(options.INIFILE)
    

    appname  = CONFIG.get("consumer", "appname")
    database = CONFIG.get("consumer", "database")
    
    default_sql="SELECT kid,id_str,tweet_ts,tweet_id,followers_count,statuses_count,friends_count,text,screen_name,mon,day,hour FROM " + options.TABLE

    sql_conf=CONFIG.items("transforms")
    if sql_conf is not None:
        # sort by the first element in each pair e.g. sql01
        sql_conf.sort(key = lambda x: x[0])

        # strip out the sql01 tags
        sql=[x[1] for x in sql_conf]
    else:
        sql = [default_sql]

    sqllist_string="|".join(sql)
    print 'using this list of sql statements: ' + sqllist_string
    main(appname,options,sqllist_string)
