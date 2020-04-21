  
CREATE TABLE weather_avro
  ROW FORMAT SERDE  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED as INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  TBLPROPERTIES ('avro.schema.url'='file:///home/cloudera/cs523/W2D5/Optional_Part/input/weather.avsc');




describe weather_avro;
 




CREATE TABLE  weather_avro(stationId STRING,
                             temperature FLOAT,
                             year INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;



DROP TABLE records;

