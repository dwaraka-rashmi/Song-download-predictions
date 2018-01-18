#!/bin/bash

#alias spark-submit=/opt/spark/bin/spark-submit

echo "model;model size;runtime;MSE"
for jar in projects/model-*.jar
do

    student_output_file="student-output/`basename $jar .jar`.csv"
    mkdir -p `dirname "$student_output_file"`

    export TEMP_DIR_PATH=$(mktemp -u "/mnt/pdpmr/`basename $jar .jar`/XXXX")
    sudo mkdir -p "$TEMP_DIR_PATH"
    sudo mount -t tmpfs -o size=100m tmpfs "$TEMP_DIR_PATH"

    /usr/bin/time -q -f '%e' -o time <input.csv \
        /opt/spark/bin/spark-submit \
            --conf spark.driver.extraJavaOptions=-DTEMP_DIR_PATH="$TEMP_DIR_PATH" \
            --driver-cores 2 \
            --executor-memory 4G \
            --driver-memory 4G  \
            --class neu.pdpmr.project.Model \
            "$jar" \
            1>"$student_output_file" \
            2>`dirname $student_output_file`/`basename $student_output_file .csv`.err
    
    size=`du -sh "$jar" | cut -f1`
    runtime=`cat time`
    mse=`pr -tm output.csv "$student_output_file" | \
         awk '{s+=($2 - $1)*($2 - $1)} END {printf("%d",s/NR)}'`

    echo "`basename $jar`;$size;$runtime;$mse"

done
