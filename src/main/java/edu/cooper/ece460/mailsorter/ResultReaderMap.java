package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.mahout.classifier.naivebayes.BayesUtils;

public class ResultReaderMap extends Mapper<LongWritable, Text, Text, Text> {
    public static enum FILE_COUNTER {
        FOO,
    };

    private static Map<Integer,String> labels;

    private static void initMap(Context context) throws IOException {
        if (labels == null) {
            synchronized (ResultReaderMap.class) {
                if (labels == null) {
                    Configuration conf = context.getConfiguration();
                    String labelIndex = conf.get("labelIndex");
                    labels = BayesUtils.readLabelIndex(conf, new Path(labelIndex));
                }
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        initMap(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        context.getCounter(FILE_COUNTER.FOO).increment(1);
        context.progress();

        String[] tokens = value.toString().split("\\s+", 2);
        String email_file = tokens[0];
        Integer category = Integer.parseInt(tokens[1]);

        Configuration conf = context.getConfiguration();
        Path path = new Path(email_file);
        FileSystem fs = path.getFileSystem(conf);

        String fileData = "";
        FSDataInputStream in = fs.open(new Path(email_file));
        while(true) {
            String line = in.readLine();
            fileData += (line + "\n");
            if (line == null) {
                break;
            }
        }

        List<String> emailData = MailSorterUtil.getText(new BytesWritable(fileData.getBytes()), false);
        String outstr = "";
        for(String s : emailData){
            outstr += (s + "; ");
        }

        context.write(new Text(labels.get(category)), new Text(outstr));
        context.progress();
    }
}
