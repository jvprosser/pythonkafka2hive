#!/bin/bash

table=$1
pkey=$2
timestamp=$3

timestamp=`date +"%Y%m%d_%H%M"`
# !connect jdbc:hive2://ip-172-31-6-171.us-west-2.compute.internal:10000/BOA_ATM
HS2node='ip-172-31-6-171'
db=BOA_ATM
conn="jdbc:hive2://${HS2node}:10000/${db}?hive.cli.conf.printheader=false;user=ec2-user;password=ec2-user"
sep='|'
path="/tmp/app/ret"

# although the data we are working with is pipe separated, we want the output of deribe table to be comma separated
args=" --outputformat=csv2  --showHeader=false"

describe_table="describe $table"

columns=`beeline -u $conn $args -e "${describe_table}"`

batchfile="merge_${table}_${timestamp}.hql"

# the update file is per-table and is of the form pkey,column,newvalue

#### below this point is where we generate the sql
cat <<EOF > $batchfile
--  
--   Create table for deltas
--  

CREATE EXTERNAL TABLE ${table}_deltas_${timestamp} (
EOF

i="0"
for r in $columns 
do
  if (("$i" > "0")); then
    echo -n ","  >> $batchfile
  fi
  i="1"
  echo "$r" | sed -e "s/,/ /g" >> $batchfile

#  echo "  $r" | sed -e "s/,/ /1" >> $batchfile
done

cat <<EOF >> $batchfile
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '${sep}'
STORED AS TEXTFILE
LOCATION '${path}/${table}/delta_${timestamp}';

--  
--  Now create table for new version resulting from merge
--  

CREATE EXTERNAL TABLE ${table}_${timestamp} (
EOF

i="0"

for r in $columns 
do
  if (("$i" > "0")); then
    echo -n ","  >> $batchfile
  fi
  i="1"
  echo "$r" | sed -e "s/,/ /g" >> $batchfile
done

cat <<EOF >> $batchfile
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '${sep}'
STORED AS TEXTFILE
LOCATION '${path}/${table}/version_${timestamp}';

--  
--  Now the merge statement
--  

INSERT INTO TABLE ${table}_${timestamp}
-- The existing records that have not changed will not join to the deltas.
SELECT
EOF

i="0"

for r in $columns 
do
  if (("$i" > "0")); then
    echo -n ","  >> $batchfile
  fi
  i="1"
  echo "c.$r" | cut -d, -f1  >> $batchfile
done

cat <<EOF >> $batchfile
FROM
    ${table} c
LEFT OUTER JOIN
    ${table}_deltas_${timestamp} t
ON
    c.${pkey} = t.${pkey}
WHERE
    t.${pkey} IS NULL
UNION ALL
-- The existing records that have changed, plus the new records, are all of the deltas.
SELECT
    *
FROM
    ${table}_deltas_${timestamp}
;
EOF

echo  "batchfile is $batchfile "
#cat $batchfile

exit