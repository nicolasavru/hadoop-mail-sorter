package edu.cooper.ece460.mailsorter;

import java.io.*;
// import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import org.apache.mahout.vectorizer.encoders.*;
import org.apache.mahout.math.*;

public class MailSorterReduce extends Reducer<Text, VectorWritable, NullWritable, NullWritable> {
    public static final byte[] cf = "userhost".getBytes();

    @Override
    public void reduce(Text key, Iterable<VectorWritable> values, Context context)
        throws IOException, InterruptedException
    {
        Path path = new Path(key + ".seq");
        System.err.println("seqfile: " + path.toString());
        Configuration conf = new Configuration();
        // FileSystem fs = FileSystem.get(conf);
        FileSystem fs = path.getFileSystem(conf);

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
                                                             Text.class,
                                                             VectorWritable.class);

        for(VectorWritable value : values){
            NamedVector v = (NamedVector) value.get();
            writer.append(new Text(v.getName()), value);
        }
        writer.close();

        // context.write(key, new VectorWritable(new NamedVector(v, who)));
    }
}

