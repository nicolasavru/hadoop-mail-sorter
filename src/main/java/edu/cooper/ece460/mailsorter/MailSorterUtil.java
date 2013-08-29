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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

import org.apache.mahout.math.*;
import org.apache.mahout.vectorizer.encoders.*;

public final class MailSorterUtil {
    // http://stackoverflow.com/questions/6334692/how-to-use-a-lucene-analyzer-to-tokenize-a-string
    // private LuceneUtil() {}

    public static List<String> tokenizeString(Analyzer analyzer, String string) {
        List<String> result = new ArrayList<String>();
        try {
            TokenStream stream  = analyzer.tokenStream(null, new StringReader(string));
            stream.reset();
            while (stream.incrementToken()) {
                result.add(stream.getAttribute(CharTermAttribute.class).toString());
            }
        } catch (IOException e) {
            // not thrown b/c we're using a string reader...
            throw new RuntimeException(e);
        }
        return result;
    }

    // public Vector encode(String type, List<String> data, String label) throws IOException
    public static org.apache.mahout.math.Vector encode(String label, List<String> data) throws IOException
    {
        FeatureVectorEncoder content_encoder = new AdaptiveWordValueEncoder("content");
        content_encoder.setProbes(2);

        org.apache.mahout.math.Vector v = new RandomAccessSparseVector(100);

        for (String word : data) {
            content_encoder.addToVector(word, v);
        }
        return new NamedVector(v, label);
    }

    public static List<String> getText(BytesWritable value) throws InterruptedException {
        Session s = Session.getDefaultInstance(new Properties());
        InputStream is = new ByteArrayInputStream(value.getBytes());
        List<String> out = new ArrayList<String>();
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

            Analyzer standard_analyzer = new StandardAnalyzer(Version.LUCENE_43);
            Analyzer email_analyzer = new UAX29URLEmailAnalyzer(Version.LUCENE_43);

            Address[] fromAddrs = message.getFrom();
            String fromAddrstr = "";
            if(fromAddrs != null){
                for(Address addr: fromAddrs){
                    fromAddrstr += (addr.toString() + " ");
                }
            }
            List<String> fromData = tokenizeString(email_analyzer, fromAddrstr);

            Address[] toAddrs = message.getAllRecipients();
            String toAddrstr = "";
            if(toAddrs != null){
                for(Address addr: toAddrs){
                    toAddrstr += (addr.toString() + " ");
                }
            }
            List<String> toData = tokenizeString(email_analyzer, toAddrstr);

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
                System.err.println("DecodingException");
            } catch (UnsupportedEncodingException e){
                System.err.println("UnsuportedEncodingException");
            } catch (IOException e){
                System.err.println("IOException");
            }

            List<String> bodyData = tokenizeString(standard_analyzer, body);

            out.add("FROM ");
            out.addAll(fromData);

            out.add("TO ");
            out.addAll(toData);

            out.add("BODY ");
            out.addAll(bodyData);
        }
        catch (MessagingException e) {
            System.err.println("MessagineException");
        }

        return out;
    }
}
