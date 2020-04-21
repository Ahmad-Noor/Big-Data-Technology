SELECT CONCAT('Total Rows : ' , count(*))  from employees
UNION ALL
SELECT ' Cols non-null---------------------------------------'
UNION ALL
 
 SELECT CONCAT('Name : ' , count(Name),' non-null')  from employees WHERE Name IS NOT NULL
 UNION ALL
 SELECT CONCAT('Job_Titles : ' , count(Job_Titles),' non-null')  from employees WHERE Job_Titles IS NOT NULL
 UNION ALL
 SELECT CONCAT('Department : ' , count(Department),' non-null')  from employees WHERE Department IS NOT NULL
 UNION ALL
 SELECT CONCAT('Full_or_Part_Time : ' , count(Full_or_Part_Time),' non-null')  from employees WHERE Full_or_Part_Time IS NOT NULL
 UNION ALL
 SELECT CONCAT('Salary_or_Hourly : ' , count(Salary_or_Hourly),' non-null')  from employees WHERE Salary_or_Hourly IS NOT NULL
 UNION ALL
 SELECT CONCAT('Typical_Hours : ' , count(Typical_Hours),' non-null')  from employees WHERE Typical_Hours IS NOT NULL
 UNION ALL
 SELECT CONCAT('Annual_Salary : ' , count(Annual_Salary),' non-null')  from employees WHERE Annual_Salary IS NOT NULL
 UNION ALL
 SELECT CONCAT('Hourly_Rate : ' , count(Hourly_Rate),' non-null')  from employees WHERE Hourly_Rate IS NOT NULL
 
UNION ALL
SELECT ' Describe Data --------------------------------------'

 
 UNION ALL
 SELECT CONCAT( 'Annual Salary -  Min (' , Min(Annual_Salary),')')  from employees WHERE Annual_Salary IS NOT NULL 
 UNION ALL
 SELECT CONCAT( 'Annual Salary -  AVG (' , Sum(Annual_Salary) /Count(Annual_Salary),')')  from employees WHERE Annual_Salary IS NOT NULL 
 UNION ALL
 SELECT CONCAT( 'Annual Salary -  Max (' , Max(Annual_Salary),')')  from employees WHERE Annual_Salary IS NOT NULL 
 UNION ALL
 SELECT CONCAT( 'Annual Salary -  Total (' , Sum(Annual_Salary),')')  from employees WHERE Annual_Salary IS NOT NULL 

 UNION ALL
 SELECT CONCAT( 'Hourly Rate -  Min (' , Min(Hourly_Rate),')')  from employees WHERE Hourly_Rate IS NOT NULL 
 UNION ALL
 SELECT CONCAT( 'Hourly Rate-  AVG (' , Sum(Hourly_Rate) /Count(Hourly_Rate),')')  from employees WHERE Hourly_Rate IS NOT NULL 
 UNION ALL
 SELECT CONCAT( 'Hourly Rate -  Max (' , Max (Hourly_Rate),')')  from employees WHERE Hourly_Rate IS NOT NULL ;




 


 
 