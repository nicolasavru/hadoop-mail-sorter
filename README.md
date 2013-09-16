Sorting Mail with Hadoop
==============================

Train a Naive Bayes classifier on sorted mail and use it to sort new
mail using Hadoop.

Dependencies:
Hadoop 1.1.1
Mahout 0.8
Lucene 4.3
JavaMail
Commons Codec
Commons Lang3

##Build

	mvn package

##Usage

Start Jetty with

	mvn jetty:run

and browse to http://hostname:8080/

Most of the runtime stages are self-explanitory, but relevant notes
for each are below. For each runtime stage, the input and output paths
are located in HDFS.

For large inputs (60k+ emails), many of these stages can take an
inordinate amount of memory (java heap space). Configure your Hadoop
cluster accordingly. Also, certain stages somtimes get stuck and
fail. Due to the prototype nature of this project, I have not
thoroughly debugged this, but at a glance it looks like the mapreduce
tasks block on IO, which would imply that there is something wrong
with HDFS my cobbled-together Hadoop cluster.

###Convert Maildir to Data Sequence Files

Input is expected in Maildir format, however HDFS does not support
colons in filenames, so they should be replaced with something
else. For example, to easily replace all colons in filenames with
double underscores:

	find Maildir/ -exec rename ":" "__" {} \;


###Train Naive Bayes Classifier

After testing, it is usually desired to use all available data to
train the production classifier model, so the input path (default
"traindir") should be replaced with the output path of the Convery
Data Sequence Files to Vector Sequence Files stage (default "vecdir").

###Test Naive Bayes Classifier

Due to the random split of vectors into training and testing sets, it
is possible that all emails from some mail subdirs are present in one
set but not the other. This will cause the test routine to fail.

##Output

Parsed output is in the form

	[suggested_label]\t[sender] ; [subject]

