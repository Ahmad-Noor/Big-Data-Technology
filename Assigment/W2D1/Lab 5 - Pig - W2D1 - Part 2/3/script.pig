--load Dataset
register '/usr/lib/pig/piggybank.jar';

movies = LOAD '/home/cloudera/Desktop/LAB5 Part 2/4/dataset/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:int, title:chararray, genres:chararray);
users = LOAD '/home/cloudera/Desktop/LAB5 Part 2/3/dataset/users.txt' USING PigStorage('|') AS (userId:chararray, age:int, gender:chararray, occupation:chararray,zipCode:chararray);
rating = LOAD '/home/cloudera/Desktop/LAB5 Part 2/3/dataset/rating.txt' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:int);

-- Filter title start with letter “A” or “a”

usersFiltered = FILTER movies BY STARTSWITH(title, 'A') OR STARTSWITH(title, 'a') ;
flattenWords = FOREACH usersFiltered GENERATE FLATTEN(TOKENIZE(genres,'|')) AS genre;

--words count 
grouped = GROUP flattenWords BY genre;
count = FOREACH grouped GENERATE group, COUNT($1);
 
--Order.
genresOrder = Order count by $0 ;

displayData = FOREACH genresOrder GENERATE  CONCAT ( (chararray)$0,'\t',(chararray)$1);
 
--STORE output
 STORE displayData INTO '/home/cloudera/Desktop/LAB5 Part 2/3/output' using PigStorage('\t');
-- display result:  
--DUMP displayData;

 
