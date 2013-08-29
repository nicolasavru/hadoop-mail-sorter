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
    public static void recursePath(Configuration conf, Path path, Job job){
        try{
            FileSystem fs = path.getFileSystem(conf);
            FileStatus[] fstats = fs.listStatus(path);
            if(fstats != null){
                for(FileStatus f : fstats){
                    Path p = f.getPath();;
                    if(fs.isFile(p)){
                        // connection times out otherwise
                        System.err.println("file:" + p.toString());
                        FileInputFormat.addInputPath(job, p);
                    }
                    else{
                        System.err.println("dir:" + p.toString());
                        recursePath(conf, p, job);
                    }
                }
            }
        } catch (IOException e) {
            // shouldn't be here
            throw new RuntimeException(e);
        }
    }

    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();

        if(args[0].equals("maildir2seq")){
            Path inPath = new Path(args[1]);
            Path outPath = new Path(args[2]);

            conf.set("seqdir", args[2]);

            // do not create a new jvm for each task
            conf.setLong("mapred.job.reuse.jvm.num.tasks", -1);

            Job job1 = new Job(conf, "HadoopMailSorter");
            job1.setJarByClass(HadoopMailSorter.class);
            job1.setMapperClass(MailSorterMap.class);
            job1.setReducerClass(MailSorterReduce.class);
            job1.setNumReduceTasks(50);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setInputFormatClass(WholeFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);
            // job1.setOutputFormatClass(TextOutputFormat.class);
            // job1.setOutputKeyClass(Text.class);
            job1.setOutputKeyClass(NullWritable.class);
            job1.setOutputValueClass(NullWritable.class);
            recursePath(conf, inPath, job1);
            FileOutputFormat.setOutputPath(job1, new Path("/tmp/foo"));
            job1.waitForCompletion(true);
        }
        else if(args[0].equals("classify")){
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
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setInputFormatClass(WholeFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            recursePath(conf, inputPath, job);
            // FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, outputPath);

            job.waitForCompletion(true);
        }
        else{
            System.err.println(args[0] + "is not a valid function.");
            return;
        }

    }
}

