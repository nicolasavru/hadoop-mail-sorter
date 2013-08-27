package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.TokenStream;

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
}
