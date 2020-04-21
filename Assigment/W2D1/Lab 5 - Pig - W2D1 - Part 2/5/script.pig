--load Dataset 
register '/usr/lib/pig/piggybank.jar';

movies = LOAD '/home/cloudera/Desktop/LAB5 Part 2/5/dataset/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:int, title:chararray, genres:chararray);
users = LOAD '/home/cloudera/Desktop/LAB5 Part 2/5/dataset/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray,zipCode:chararray);
rating = LOAD '/home/cloudera/Desktop/LAB5 Part 2/5/dataset/rating.txt' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:int);

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



--=====================================================================
--====================== USERS DATA ===================================
--=====================================================================
-- filter users mail and progermers
filterUsers = Filter users BY gender == 'M' and occupation == 'programmer';

-- get users rating 
usersrating = JOIN rating BY userId,filterUsers BY userId;
usersratingGRP = GROUP usersrating BY $1 ;

usersratingCNT = FOREACH usersratingGRP GENERATE group AS movieId, COUNT($1) AS count;
--usersRData = FOREACH usersratingCNT GENERATE $0 ,$1; 
 
--displayDataOrdered: {filterMovies::movieId: int,filterMovies::genre: chararray,filterRating::rating: int,filterMovies::title: chararray}
--usersratingCNT: {group: (rating::userId: int,rating::movieId: int),count: long}  

programerRating= JOIN displayData BY $0 LEFT OUTER,usersratingCNT BY $0;

displayDataprogramerRating = FOREACH programerRating GENERATE $0,$1,$2,$3,$5; 

 
--STORE output Count By Movies

displayDataprogramerRatingOrdered = Order displayDataprogramerRating BY $0;

--STORE displayDataprogramerRatingOrdered INTO '/home/cloudera/Desktop/LAB5 Part 2/5/outputCountByMovies' using PigStorage('\t');
--STORE output Count 

grb = GROUP displayDataprogramerRating ALL;
count = FOREACH grb GENERATE group, SUM($1.count) AS SUM;


STORE count INTO '/home/cloudera/Desktop/LAB5 Part 2/5/output' using PigStorage('\t');






