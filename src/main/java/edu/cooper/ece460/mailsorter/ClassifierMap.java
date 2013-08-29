package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ClassifierMap extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {
    private final static Text outputKey = new Text();
    private final static IntWritable outputValue = new IntWritable();
    private static Classifier classifier;
    private Path filenamePath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        initClassifier(context);
        InputSplit split = context.getInputSplit();
        Path path = ((FileSplit) split).getPath();
        filenamePath = path;
    }

    private static void initClassifier(Context context) throws IOException {
        if (classifier == null) {
            synchronized (ClassifierMap.class) {
                if (classifier == null) {
                    classifier = new Classifier(context.getConfiguration());
                }
            }
        }
    }

    @Override
    public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String file = filenamePath.toString();

        int bestCategoryId = classifier.classify(value);
        outputValue.set(bestCategoryId);

        outputKey.set(file);
        context.write(outputKey, outputValue);
    }
}
