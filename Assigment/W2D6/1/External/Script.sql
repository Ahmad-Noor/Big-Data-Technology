CREATE EXTERNAL TABLE records (year STRING, temperature INT, quality INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
LOCATION '/user/cloudera/input/';

--LOAD DATA INPATH '/user/cloudera/input/weatherHive.txt' INTO TABLE records;





SELECT year, MAX(temperature) FROM records WHERE temperature !=
9999 AND quality IN (0, 1, 4, 5) GROUP BY year;

DROP TABLE records;
