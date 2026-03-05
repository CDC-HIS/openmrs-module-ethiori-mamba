mysqladmin -u root -p extended -r -i 10 | grep Handler_read_rnd_next


mysql  -u root -p$'Abcd@1234' -e "SHOW ENGINE INNODB STATUS;"| grep -n2 "$thread_id"| grep "undo log entries"

mysql -u root -p$'Abcd@1234' -e "SHOW ENGINE INNODB STATUS;" | grep ", undo log entries" | sed -n 's/.*undo log entries \([0-9]*\).*/\1/p'