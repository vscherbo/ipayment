#!/bin/sh

ARCH=99-archive


for c in `ls -1 $ARCH/*.csv`
do
  if `grep -q 5819 $c`
  then
     echo "FOUND in "$c
  fi
done
