HADOOP_HOME = /home/dey/tools/hadoop-2.8.1
LOCAL_LIB=extra/
MY_CLASSPATH = $(HADOOP_HOME)/share/hadoop/common/hadoop-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/common/lib/*:$(LOCAL_LIB)/*:sout:.


SCALA_HOME = /home/dey/tools/scala-2.11.11
SPARK_HOME = /home/dey/tools/spark-2.2.0-bin-hadoop2.7
SC_CLASSPATH = $(SCALA_HOME)/lib/*:$(SPARK_HOME)/jars/*:$(LOCAL_LIB)/*:sout:.
export TEMP_DIR_PATH=/mnt/pdpmr
SPARK_OPTIONS=--master local[*] --driver-memory 2G --conf 'spark.driver.extraJavaOptions=-DTEMP_DIR_PATH=$(TEMP_DIR_PATH)'

all: build clean run

build: compile jar

compile:
	-mkdir -p sout
	$(SCALA_HOME)/bin/scalac -cp "$(SC_CLASSPATH)" -d sout src/main/scala/neu/pdpmr/**/*.scala 

jar:
	mkdir -p /tmp/lucene
	rm -rf /tmp/lucene/*
	unzip extra/lucene-core-7.1.0.jar -d /tmp/lucene
	cp -r /tmp/lucene/META-INF /tmp/lucene/org sout/
	cp input-clean/cleandf.csv.gz sout/
	cp src/main/resources/* sout/
	cp  model.tar.gz sout/
	cp -r SMETA-INF/MANIFEST.MF sout
	cd sout; jar cvmf MANIFEST.MF model-bishwajeet_rashmi.jar *
	mv sout/model-bishwajeet_rashmi.jar .

run:
	#$(SPARK_HOME)/bin/spark-submit $(SPARK_OPTIONS) RF.jar file:///home/dey/git/pdpmr-final/input-clean/subset-cleandf.csv "local[*]" file:///tmp/rf
	$(SPARK_HOME)/bin/spark-submit $(SPARK_OPTIONS) model-bishwajeet_rashmi.jar 

clean:
	#-rm -rf /tmp/rf
	@true

report:
	Rscript -e "rmarkdown::render('report.Rmd')"

