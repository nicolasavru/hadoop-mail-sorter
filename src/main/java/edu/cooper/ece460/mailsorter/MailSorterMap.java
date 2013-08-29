package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import javax.mail.*;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.internet.*;

import com.sun.mail.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MailSorterMap extends Mapper<NullWritable, BytesWritable, Text, Text> {
    private Path filenamePath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        Path path = ((FileSplit) split).getPath();
        filenamePath = path;
    }

    @Override
    public void map(NullWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException
    {
        // assumes mail in in SOMEDIR/cur, etc.
        String dir = filenamePath.getParent().getParent().getName().toString();

        List<String> outList = MailSorterUtil.getText(value);
        String out = "";
        for(String s: outList){
            out += (s + " ");
        }

        context.write(new Text(dir), new Text(out));
    }
}
