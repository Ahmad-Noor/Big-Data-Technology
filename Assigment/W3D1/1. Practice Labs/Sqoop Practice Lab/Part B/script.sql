mysql -u root -p
--Enter password as cloudera
use cs523;
delete from student;

select * from student;
quit;
--export
sqoop export --export-dir=/user/cloudera/sqoopImportOutput/ --connect jdbc:mysql://localhost/cs523 --username root -password cloudera --table student -m 1 --mysql-delimiters