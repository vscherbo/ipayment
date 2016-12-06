#!/bin/sh

. /usr/local/bin/bashlib

DT=`date +%F_%H_%M_%S`
#############
#DO=echo
DO=''
PG_SRV=vm-pg
#############

#1. received email are processed by procmail and ripmime
# result is the csv file in directory specified with .procmailrc 
# i.e.  ripmime -i - -d /path/to/mailbox 
CSV_DIR=`grep ripmime ~/.procmailrc | awk -F '-d' '{print $2}' | awk '{print $1}'`
LOG_DIR=$CSV_DIR/logs
CSV_DATA=$CSV_DIR/01-data
CSV_ARCH=$CSV_DIR/99-archive
ARCHIVE_DEPTH=60

[ -d $CSV_DIR ] || mkdir -p $CSV_DIR
[ -d $LOG_DIR ] || mkdir -p $LOG_DIR
[ -d $CSV_DATA ] || mkdir -p $CSV_DATA
[ -d $CSV_ARCH ] || mkdir -p $CSV_ARCH

find $CSV_ARCH -type f -mtime +$ARCHIVE_DEPTH -exec rm -f {} \+
find $LOG_DIR -type f -mtime +$ARCHIVE_DEPTH -exec rm -f {} \+

LOG=$LOG_DIR/`namename $0`.log
exec 1>>$LOG 2>&1

PG_COPY_SCRIPT=$CSV_DATA/pg-COPY-registry-$DT.sql

#2. get email with attached csv - after 00:15
# $DO fetchmail -ak -m "/usr/bin/procmail -d %T"
$DO fetchmail -k -m "/usr/bin/procmail -d %T"
RC=$?
case $RC in
   0) logmsg INFO "One or more messages were successfully retrieved." 
      ;;
   1) logmsg INFO "There was no mail."
      exit 0
      ;;
   *) logmsg $RC "fetchmail completed."
      exit $RC
      ;;
esac

pushd $CSV_DIR

> $PG_COPY_SCRIPT
IMPORT='NO'
IFS_BCK=$IFS
IFS=$'\n'
#3. Prepare COPY commands for PG
REGS_LIST=`ls -1 registry*csv`
logmsg INFO "REGS_LIST=$REGS_LIST"
for csv in $REGS_LIST
do
  # import files containing not only header 
  # input file doesn't contain CR on the last line. Use grep+wc
  ROWS=`grep -v currency $csv | wc -l`
  if [ $ROWS -gt 0 ]
  then 
     logmsg INFO "CSV file $csv contains $ROWS rows. Prepare \\COPY command to load CSV into PG"
     # PG follows the SQL standard - decimal dot as the delimiter 
     csv_name=`namename $csv`
     PG_CSV=$CSV_DATA/$csv_name-DOT.csv
     $DO sed 's/,/./g' $csv > $PG_CSV

     echo "\COPY inetpayments FROM '"$PG_CSV"' WITH ( FORMAT CSV, HEADER true, DELIMITER ';') ;" >> $PG_COPY_SCRIPT
     IMPORT='YES'
  else
     logmsg INFO "The registry $csv contains only header row. Skip it, just archive"
     cat $csv
     $DO mv  registry*.csv  $CSV_ARCH/
  fi
done
IFS=$IFS_BCK

#4. Import registry into PG
# use ~/.pgpass
if [ $IMPORT == 'YES' ]
then
   logmsg INFO "\\COPY $PG_CSV into $PG_SRV"
   cat $PG_CSV
   echo ""

   $DO psql --set ON_ERROR_STOP=on -h $PG_SRV -U arc_energo -d arc_energo -w -f $PG_COPY_SCRIPT
   RC_IMP=$?
   logmsg $RC_IMP "The Platron registry($PG_COPY_SCRIPT) imported."

   #4. Link registry with Bills and SET inetamount
   if [ $RC_IMP -eq 0 ]
   then 
      logmsg INFO "CSV successfully loaded into $PG_SRV"
      ORDERS_SET=`awk -F ";" '$1 ~ /[0-9]+/ {s=s $1 ","}END{gsub(/\"/, "", s); printf "%s", substr(s, 1, length(s)-1) }' $PG_CSV` 
      if [ +$ORDERS_SET != '+' ]
      then
         logmsg INFO "Check Счета: ИнтернетЗаказ $ORDERS_SET"
         echo "SELECT \"ИнтернетЗаказ\", \"Интернет\", \"Оплачен\", inetamount FROM \"Счета\" WHERE \"ИнтернетЗаказ\" IN ("$ORDERS_SET");" > sql.file
         cat sql.file
         $DO psql -h $PG_SRV -U arc_energo -d arc_energo -w -f sql.file
         #
         logmsg INFO "Check inetpayments: order_id $ORDERS_SET"
         echo "SELECT * FROM inetpayments WHERE order_id IN ("$ORDERS_SET");" > sql.file
         cat sql.file
         $DO psql -h $PG_SRV -U arc_energo -d arc_energo -w -f sql.file
         #
         logmsg INFO "Try JOIN Счета and inetpayments tables on ORDERS_SET=$ORDERS_SET"
         echo "SELECT \"ИнтернетЗаказ\", inetpayments.order_id, \"Интернет\", \"Оплачен\", inetamount FROM \"Счета\", inetpayments WHERE \"ИнтернетЗаказ\" = inetpayments.order_id AND inetpayments.order_id IN ("$ORDERS_SET");" > sql.file
         cat sql.file
         $DO psql -h $PG_SRV -U arc_energo -d arc_energo -w -f sql.file
         rm -f sql.file
      fi

      logmsg INFO "Try UPDATE Счета table"
      $DO psql -h $PG_SRV -U arc_energo -d arc_energo -w -c "UPDATE Счета SET inetamount = inetpayments.to_pay, Сообщение = 't', inetdt = inetpayments.op_date + inetpayments.op_time, ps_id = 1 FROM inetpayments WHERE ИнтернетЗаказ = inetpayments.order_id AND Интернет = 't' AND Оплачен = 'f' AND inetamount IS NULL;" 
      RC_LINK=$?
      logmsg $RC_LINK "Linking the Platron registry with Счета finished."
      #
      $DO mv  registry*.csv  $CSV_ARCH/
      $DO mv $PG_COPY_SCRIPT $CSV_ARCH/
      $DO rm -f $CSV_DATA/registry_*-DOT.csv
   fi # if RC_IMP=0 
else
   rm -f $PG_COPY_SCRIPT
fi # if IMPORT

popd

