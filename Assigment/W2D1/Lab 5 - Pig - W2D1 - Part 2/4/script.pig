--load Dataset 
register '/usr/lib/pig/piggybank.jar';

movies = LOAD '/home/cloudera/Desktop/LAB5 Part 2/4/dataset/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:int, title:chararray, genres:chararray);
users = LOAD '/home/cloudera/Desktop/LAB5 Part 2/4/dataset/users.txt' USING PigStorage('|') AS (userId:chararray, age:int, gender:chararray, occupation:chararray,zipCode:chararray);
rating = LOAD '/home/cloudera/Desktop/LAB5 Part 2/4/dataset/rating.txt' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:int);

--  get movie & genres
flattenmovies = FOREACH movies GENERATE movieId,title,FLATTEN(TOKENIZE(genres,'|')) AS genre; 
 
-- Filter genres = "Adventure" 
filterMovies = FILTER flattenmovies BY genre == 'Adventure';

-- Filter rating = 5
filterRating = FILTER rating BY rating == 5;

 ---join 
relationData = JOIN filterMovies BY movieId, filterRating BY movieId; 


-- highest rated count

filterRatingGRP = GROUP relationData BY (filterMovies::movieId,filterMovies::title,filterMovies::genre,filterRating::rating); 
count = FOREACH filterRatingGRP GENERATE group, COUNT($1) AS cunt;


-- Top 20
counOrder = Order count BY cunt DESC;
limitData = LIMIT counOrder 20;

 -- display result:  
displayData = FOREACH limitData GENERATE $0.movieId,$0.genre,$0.rating,$0.title; 

displayDataOrdered = Order displayData BY $0;
--STORE output
STORE displayDataOrdered INTO '/home/cloudera/Desktop/LAB5 Part 2/4/output' using PigStorage('\t');
 
--DUMP displayData;

  
 -- DESCRIBE filterRatingGRP;









