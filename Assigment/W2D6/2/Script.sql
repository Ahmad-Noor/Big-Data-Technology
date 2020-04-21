CREATE EXTERNAL TABLE Employees(
FirstName STRING,
LastName STRING,
Job_Titles STRING,
Department STRING,
Full_or_Part_Time STRING,
Salary_or_Hourly STRING,
Typical_Hours INT ,
Annual_Salary DOUBLE,
Hourly_Rate FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
LOCATION '/user/cloudera/input/'
tblproperties("skip.header.line.count"="1"); 

 
DROP TABLE Employees;
