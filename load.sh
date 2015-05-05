#!/bin/sh

. /usr/local/bin/bashlib

DT=`date +%F_%H_%M_%S`
#############
# DO=echo
DO=''
PG_SRV=vm-pg
#############

#1. received email are processed by procmail and ripmime
# result is the csv file in directory specified with .procmailrc 
# i.e.  ripmime -i - -d /home/ARC/scherbova/automail/platron 
CSV_DIR=`grep ripmime ~/.procmailrc | awk -F '-d' '{print $2}' | awk '{print $1}'`
LOG_DIR=$CSV_DIR/logs
CSV_DATA=$CSV_DIR/01-data
CSV_ARCH=$CSV_DIR/99-archive

[ -d $CSV_DIR ] || mkdir -p $CSV_DIR
[ -d $LOG_DIR ] || mkdir -p $LOG_DIR
[ -d $CSV_DATA ] || mkdir -p $CSV_DATA
[ -d $CSV_ARCH ] || mkdir -p $CSV_ARCH

LOG=$LOG_DIR/`namename $0`.log
exec 1>$LOG 2>&1

PG_COPY_SCRIPT=$CSV_DATA/pg-COPY-registry-$DT.sql

set -vx

pushd $CSV_DIR

> $PG_COPY_SCRIPT
IMPORT='NO'
#3. Prepare COPY commands for PG
for csv in `ls -1 registry*csv`
do
  # import files containing not only header 
  # input file doesn't contain CR on the last line. Use grep+wc
  ROWS=`grep -v currency $csv | wc -l`
  if [ $ROWS -gt 0 ]
  then 
     # PG expect decimal dot as the delimiter
     csv_name=`namename $csv`
     PG_CSV=$CSV_DATA/$csv_name-DOT.csv
     $DO sed 's/,/./g' $csv > $PG_CSV

     echo "\COPY inetpayments FROM '"$PG_CSV"' WITH ( FORMAT CSV, HEADER true, DELIMITER ';') ;" >> $PG_COPY_SCRIPT
     IMPORT='YES'
  else
set +vx
     logmsg INFO "The registry contains only header row. Skip it"
set -vx
  fi
done


#4. Import registry into PG
# use ~/.pgpass
if [ $IMPORT == 'YES' ]
then
   $DO psql --set ON_ERROR_STOP=on -h $PG_SRV -U arc_energo -d arc_energo -w -f $PG_COPY_SCRIPT
   RC_IMP=$?
set +vx
   logmsg $RC_IMP "Import of the Platron registry finished."
set -vx

   #4. Link registry with Bills and SET inetamount
   if [ $RC_IMP -eq 0 ]
   then 
       $DO psql -h $PG_SRV -U arc_energo -d arc_energo -w -c "UPDATE Счета SET inetamount = inetpayments.to_pay, Сообщение = 't' FROM inetpayments WHERE ИнтернетЗаказ = inetpayments.order_id AND Интернет = 't' AND Оплачен = 'f' AND inetamount IS NULL;" 
      RC_LINK=$?
set +vx
      logmsg $RC_LINK "Linking the Platron registry with Bills finished."
set -vx
      $DO mv  registry*.csv  $CSV_ARCH/
   fi # if RC_IMP=0 
fi # if IMPORT

$DO mv $PG_COPY_SCRIPT $CSV_ARCH/
$DO rm -f $CSV_DATA/registry_*-DOT.csv
popd

