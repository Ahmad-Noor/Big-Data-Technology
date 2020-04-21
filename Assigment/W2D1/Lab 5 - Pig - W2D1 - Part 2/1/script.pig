--load Dataset
movies = LOAD '/home/cloudera/Desktop/LAB5 Part 2/1/dataset/movies.csv' USING PigStorage(',') AS (movieId:int, title:chararray, genres: chararray);
users = LOAD '/home/cloudera/Desktop/LAB5 Part 2/1/dataset/users.txt' USING PigStorage('|') AS (userId:chararray, age:int, gender:chararray, occupation:chararray,zipCode:chararray);
rating = LOAD '/home/cloudera/Desktop/LAB5 Part 2/1/dataset/rating.txt' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:int);

-- Filter How many male lawyers
usersFiltered = FILTER users BY gender == 'M' AND occupation=='lawyer';

--Count
groupDate = GROUP usersFiltered All;  
count = FOREACH groupDate GENERATE COUNT($1);
 
 
-- display result:  
DUMP count;

 
