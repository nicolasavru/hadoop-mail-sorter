package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.mahout.math.*;

public class HadoopMailSorter {
    public static void maildir2seq(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inPath = new Path(args[1]);
        Path outPath = new Path(args[2]);

        conf.set("seqdir", args[2]);

        // do not create a new jvm for each task
        conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);

        Job job = new Job(conf, "HadoopMailSorter");
        job.setJarByClass(HadoopMailSorter.class);
        job.setMapperClass(MailSorterMap.class);
        job.setReducerClass(MailSorterReduce.class);
        job.setNumReduceTasks(50);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // job.setOutputFormatClass(TextOutputFormat.class);
        // job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        MailSorterUtil.recursePath(conf, inPath, job);
        FileOutputFormat.setOutputPath(job, new Path("/tmp/foo"));
        job.waitForCompletion(true);
    }

    public static void classify(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 6) {
            System.err.println("Arguments: [model] [dictionary] [document frequency] [inputDir] [outputDir]");
            return;
        }
        String modelPath = args[1];
        String dictionaryPath = args[2];
        String documentFrequencyPath = args[3];
        Path inputPath = new Path(args[4]);
        Path outputPath = new Path(args[5]);

        conf.setStrings(Classifier.MODEL_PATH_CONF, modelPath);
        conf.setStrings(Classifier.DICTIONARY_PATH_CONF, dictionaryPath);
        conf.setStrings(Classifier.DOCUMENT_FREQUENCY_PATH_CONF, documentFrequencyPath);

        // do not create a new jvm for each task
        conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);

        Job job = new Job(conf, "classifier");
        job.setJarByClass(HadoopMailSorter.class);
        job.setMapperClass(ClassifierMap.class);
        job.setNumReduceTasks(50);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MailSorterUtil.recursePath(conf, inputPath, job);
        // FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }

    public static void readResults(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String resultsFile = args[1];
        String labelIndex = args[2];
        Path outputPath = new Path(args[3]);

        conf.set("labelIndex", args[2]);

        // do not create a new jvm for each task
        conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);

        Job job = new Job(conf, "resultReader");
        job.setJarByClass(HadoopMailSorter.class);
        job.setMapperClass(ResultReaderMap.class);
        job.setNumReduceTasks(50);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(resultsFile));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);

    }

    public static void main(String[] args) throws Exception {
        if(args[0].equals("maildir2seq")){
            maildir2seq(args);
        }
        else if(args[0].equals("classify")){
            classify(args);
        }
        else if(args[0].equals("readResults")){
            readResults(args);
        }
        else{
            System.err.println(args[0] + "is not a valid function.");
            return;
        }

    }
}

