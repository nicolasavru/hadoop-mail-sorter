package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import org.apache.commons.lang3.StringUtils;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
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
    public static void recursePath(Configuration conf, Path path, Job job){
        try{
            FileSystem fs = path.getFileSystem(conf);
            FileStatus[] fstats = fs.listStatus(path);
            if(fstats != null){
                for(FileStatus f : fstats){
                    Path p = f.getPath();;
                    if(fs.isFile(p)){
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
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        conf.set("seqdir", args[1]);

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
}

