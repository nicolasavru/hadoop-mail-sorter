package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import javax.mail.*;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.internet.*;

import com.sun.mail.util.DecodingException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import org.apache.mahout.math.*;

public class MailSorterMap extends Mapper<NullWritable, BytesWritable, Text, VectorWritable> {
    Context context;

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
        // this.context = context;

        VectorWritable v = new VectorWritable();

        Session s = Session.getDefaultInstance(new Properties());
        InputStream is = new ByteArrayInputStream(value.getBytes());
        try {
            MimeMessage message = new MimeMessage(s, is);
            message.getAllHeaderLines();
            
            // for (Enumeration<Header> e = message.getAllHeaders(); e.hasMoreElements();) {
            //     Header h = e.nextElement();
            //     h.getName();
            //     h.getValue();
            // }

            // System.err.println("mapper:" + (new String(value.getBytes(), "UTF-8")));
            // System.err.println("mapper:" + message.getSubject() + "; " +
            //                    (message.getFrom()[0]).toString());

            String body = "empty :(";
            try {
                Object content = message.getContent();
                // System.err.println(content.getContentType());
                if(content instanceof String){
                    body = (String) content;
                }
                else if(content instanceof Multipart){
                    Multipart mp = (Multipart) content;
                    for (int i = 0; i < mp.getCount(); i++) {
                        BodyPart bp = mp.getBodyPart(i);
                        System.err.println(bp.getContentType());
                        Object c = bp.getContent();
                        if(c instanceof String){
                            body = (String) c;
                        }
                    }
                }
            } catch (DecodingException e) {
                System.err.println("got error");
            }

            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_44);
            List<String> data = MailSorterUtil.tokenizeString(analyzer, body);
            v.set(MailSorterUtil.encode(filenamePath.toString(), data));
        }
        catch (MessagingException e) {
            System.err.println("got e");
        }

        // assumes mail in in SOMEDIR/cur, etc.
        String dir = filenamePath.getParent().getParent().toString();

        context.write(new Text(dir), v);
    }
}
