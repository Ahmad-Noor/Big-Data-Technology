# sudo service hbase-master start;
# sudo service hbase-regionsever start;
# service --status-all;

hbase shell
help
create 'test', 'cf'
list 'test'

put 'test', 'row1', 'cf:a', 'value1'
put 'test', 'row2', 'cf:b', 'value2'
put 'test', 'row3', 'cf:c', 'value3'

scan 'test'

get 'test', 'row1'

disable 'test'
enable 'test'

disable 'test'
drop 'test'

quit


