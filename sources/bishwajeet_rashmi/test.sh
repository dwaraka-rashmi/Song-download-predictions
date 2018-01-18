#!/usr/bin/env bash
export TEMP_DIR_PATH=/mnt/pdpmr
/usr/bin/time -q -f '%e' -o time <$1 \
        /home/dey/tools/spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
            --conf spark.driver.extraJavaOptions=-DTEMP_DIR_PATH="$TEMP_DIR_PATH" \
            --driver-cores 2 \
            --executor-memory 4G \
            --driver-memory 4G  \
            --class neu.pdpmr.project.Model \
            "model-bishwajeet_rashmi.jar" \
1>"$2" \
