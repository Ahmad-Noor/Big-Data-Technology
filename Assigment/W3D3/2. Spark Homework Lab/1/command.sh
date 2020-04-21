#HDFS
spark-submit --class "cs523.SparkWC.WordCount" --master yarn WordCount.jar /user/cloudera/input/ /user/cloudera/output 3

#Local
spark-submit --class "cs523.SparkWC.WordCount" --master local  WordCount.jar /home/cloudera/workspace/SparkWC/input/ /home/cloudera/workspace/SparkWC/output 3
