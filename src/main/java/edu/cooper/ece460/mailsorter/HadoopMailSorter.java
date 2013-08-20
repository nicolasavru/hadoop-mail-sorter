package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
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
    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inPath = new Path(args[0]);
        Path outPath1 = new Path(args[1]);
        // Path outPath2 = new Path(args[2]);
        // Path outPath3 = new Path(args[3]);


        Job job1 = new Job(conf, "HadoopMailSorter");
        job1.setJarByClass(HadoopMailSorter.class);
        job1.setMapperClass(MailSorterMap.class);
        // job1.setReducerClass(CUCCTicketReduce.class);
        job1.setNumReduceTasks(12);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setInputFormatClass(WholeFileInputFormat.class);
        // job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        // job1.setOutputValueClass(VectorWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, inPath);
        FileOutputFormat.setOutputPath(job1, outPath1);
        job1.waitForCompletion(true);

    }
}

