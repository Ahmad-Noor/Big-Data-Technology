mysql -u root -p
--Enter password as cloudera

create database cs523;
use cs523;
create table student (id int not null primary key, name varchar(20), address varchar(20));
describe student;
insert into student values (1, "John", "12th Ave, Iowa"), (2, "Mary","Boston"), (3, "Bob", "Des Moines"), (4, "Lina", "San Francisco");

select * from student;
quit;


--import
sqoop import --connect jdbc:mysql://localhost/cs523 --username root -password cloudera --table student --target-dir=/user/cloudera/sqoopImportOutput -m 1 --mysql-delimiters

