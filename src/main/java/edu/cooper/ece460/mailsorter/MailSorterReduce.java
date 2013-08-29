package edu.cooper.ece460.mailsorter;

import java.io.*;
// import java.util.*;

import org.apache.commons.codec.digest.DigestUtils;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import org.apache.mahout.vectorizer.encoders.*;
import org.apache.mahout.math.*;

public class MailSorterReduce extends Reducer<Text, Text, NullWritable, NullWritable> {
    public static final byte[] cf = "userhost".getBytes();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        Path path = new Path(conf.get("seqdir") + "/" + key + ".seq");
        FileSystem fs = path.getFileSystem(conf);

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
                                                             Text.class,
                                                             Text.class);

        for(Text value : values){
            writer.append(new Text("/"+key.toString()+"/"+DigestUtils.md5Hex(value.toString())), value);
        }
        writer.close();
    }
}

