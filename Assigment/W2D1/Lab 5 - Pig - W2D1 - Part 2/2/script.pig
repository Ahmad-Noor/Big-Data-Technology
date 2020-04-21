--load Dataset
movies = LOAD '/home/cloudera/Desktop/LAB5 Part 2/2/dataset/movies.csv' USING PigStorage(',') AS (movieId:int, title:chararray, genres: chararray);
users = LOAD '/home/cloudera/Desktop/LAB5 Part 2/2/dataset/users.txt' USING PigStorage('|') AS (userId:chararray, age:int, gender:chararray, occupation:chararray,zipCode:chararray);
rating = LOAD '/home/cloudera/Desktop/LAB5 Part 2/2/dataset/rating.txt' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:int);

-- Filter How many male lawyers
usersFiltered = FILTER users BY gender == 'M' AND occupation=='lawyer';
  
--Filter Find the top 5 most visited sites.
ageOrder = Order usersFiltered by age DESC;
oldest = LIMIT ageOrder 1; 
 
-- display result:  
DUMP oldest;

 
