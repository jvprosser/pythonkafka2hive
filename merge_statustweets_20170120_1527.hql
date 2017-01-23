--  
--   Create table for deltas
--  

use BOA_ATM;

CREATE EXTERNAL TABLE BOA_ATM.statustweets_deltas_20170120_1527 (
kid int 
,id_str string 
,tweet_ts bigint 
,tweet_id bigint 
,followers_count int 
,statuses_count int 
,friends_count int 
,text string 
,screen_name string 
,mon int 
,day int 
,hour int 
)
 partitioned by (mon int, day int, hour int)
stored as parquet;

-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '|'
-- STORED AS TEXTFILE
-- LOCATION '/tmp/app/ret/statustweets/delta_20170120_1527';

--  
--  Now create table for new version resulting from merge
--  

CREATE EXTERNAL TABLE BOA_ATM.statustweets_20170120_1527 (
kid int 
,id_str string 
,tweet_ts bigint 
,tweet_id bigint 
,followers_count int 
,statuses_count int 
,friends_count int 
,text string 
,screen_name string 
,mon int 
,day int 
,hour int 
)
partitioned by (mon int, day int, hour int)
stored as parquet;

-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '|'
-- STORED AS TEXTFILE
-- LOCATION '/tmp/app/ret/statustweets/version_20170120_1527';

--  
--  Now the merge statement
--  

INSERT INTO TABLE BOA_ATM.statustweets_20170120_1527
-- The existing records that have not changed will not join to the deltas.
SELECT
c.kid
,c.id_str
,c.tweet_ts
,c.tweet_id
,c.followers_count
,c.statuses_count
,c.friends_count
,c.text
,c.screen_name
,c.mon
,c.day
,c.hour
FROM
    statustweets c
LEFT OUTER JOIN
    statustweets_deltas_20170120_1527 t
ON
    c.kid = t.kid
WHERE
    t.kid IS NULL
UNION ALL
-- The existing records that have changed, plus the new records, are all of the deltas.
SELECT
    *
FROM
    statustweets_deltas_20170120_1527
;
