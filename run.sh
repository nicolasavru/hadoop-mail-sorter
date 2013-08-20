#!/bin/bash

set -e

IN_PATH=${1:-mailsorter_in}
OUT_PATH1=${2:-mailsorter_out1}
OUT_PATH2=${3:-mailsorter_out2}
OUT_PATH3=${4:-mailsorter_out3}
OUT_PATH_LOCAL=${4:-output}

rm ${OUT_PATH_LOCAL}/$OUT_PATH1 || true
rm ${OUT_PATH_LOCAL}/$OUT_PATH2 || true
rm ${OUT_PATH_LOCAL}/$OUT_PATH3 || true
hadoop fs -rmr $OUT_PATH1 || true
hadoop fs -rmr $OUT_PATH2 || true
hadoop fs -rmr $OUT_PATH3 || true
hadoop jar target/HadoopMailSorter-1.0.jar edu.cooper.ece460.mailsorter.HadoopMailSorter $IN_PATH $OUT_PATH1 $OUT_PATH2 $OUT_PATH3
hadoop fs -getmerge $OUT_PATH3 ${OUT_PATH_LOCAL}/$OUT_PATH3

