Sorting Mail with Hadoop
==============================

Train a classifier on sorted mail and use it to sort new mail using Hadoop.

##Build

	mvn package

##Run

###Command-line

Run the jar file with Hadoop. Program arguments:

1. Path to the HDFS directory with input file(s)
2. Path to the HDFS directory to save output files (must not already exist)

Example:

	hadoop fs -rmr mailsorter
	hadoop jar target/HadoopMailSorter-1.0.jar edu.cooper.ece460.mailsorter.HadoopMailSorter mailsorter_in mailsorter_out
	hadoop fs -getmerge mailsorter_out output/mailsorter_out

###Shell script

The shell script `run.sh` in the root directory can be used to perform the
above steps. The local and HDFS output directories are automatically deleted by 
the script before starting the Hadoop job.

	./run.sh [hdfs-input-dir] [hdfs-output-dir] [local-output-dir]

Default values are `mailsorter_in`, `mailsorter_out`, and `output`, respectively.

###HTTP

	mvn jetty:run

##Output

Output format:

	TODO

