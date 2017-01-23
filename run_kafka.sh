#!/bin/bash 

# streaming_consumer9223372035369843308
OFFSETTABLE='kafkaoffsets'
APPNAME="streaming_consumer"
TABLENAME="StatusTweets"
DB="BOA_ATM"

JARS='/opt/cloudera/parcels/CDH/lib/spark/examples/lib/spark-examples-1.6.0-cdh5.8.3-hadoop2.6.0-cdh5.8.3.jar,/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-client-1.2.0-cdh5.8.3.jar,/opt/cloudera/parcels/CDH/lib/hbase/lib/hbase-common-1.2.0-cdh5.8.3.jar,/opt/cloudera/parcels/CDH/lib/hbase/hbase-spark.jar'

CONN="jdbc:hive2://ip-172-31-6-171.us-west-2.compute.internal:10000/${DB}"

#valuetext=`echo "scan '${OFFSETTABLE}', {LIMIT => 1,FILTER=> \"(PrefixFilter('${APPNAME}'))\"} " | hbase shell  | grep 'tp_offsets:start'` 2>> /dev/null
## streaming_consumer9223372035369841501              column=tp_offsets:start, timestamp=1484934490444, value=2210                                                                                           
#echo "VALUETEST=$valuetext"
#startoffset=`echo $valuetext | cut -d= -f4`
#echo "startoffset = $startoffset"
#valuetext=`echo "scan '${OFFSETTABLE}', {LIMIT => 1,FILTER=> \"(PrefixFilter('${APPNAME}'))\"} " | hbase shell  | grep 'tp_offsets:end'` 2>> /dev/null
#endoffset=`echo $valuetext | cut -d= -f4`
#echo "endoffset = $endoffset"
#
startoffset='2979'
endoffset='3013'

create_table="create table if not exists ${DB}.${TABLENAME}_${startoffset} like ${DB}.${TABLENAME}_template stored as parquet;"
create_temptable="create table if not exists ${DB}.${TABLENAME}_${startoffset}_deltas like ${DB}.${TABLENAME}_template stored as parquet;"

#echo "${create_table} ${create_temptable}" | beeline -u ${CONN}

echo "spark-submit --jars ${JARS}  streaming_consumer_range.py -b $startoffset -e $endoffset -D $DB -T ${TABLENAME}_${startoffset}_deltas"
