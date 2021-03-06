package edu.cooper.ece460.mailsorter;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.vectorizer.DefaultAnalyzer;
import org.apache.mahout.vectorizer.TFIDF;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

public class Classifier {
    public final static String MODEL_PATH_CONF = "modelPath";
    public final static String DICTIONARY_PATH_CONF = "dictionaryPath";
    public final static String DOCUMENT_FREQUENCY_PATH_CONF = "documentFrequencyPath";

    private static StandardNaiveBayesClassifier classifier;
    private static Map<String, Integer> dictionary;
    private static Map<Integer, Long> documentFrequency;

    public Classifier(Configuration configuration) throws IOException {
        String modelPath = configuration.getStrings(MODEL_PATH_CONF)[0];
        String dictionaryPath = configuration.getStrings(DICTIONARY_PATH_CONF)[0];
        String documentFrequencyPath = configuration.getStrings(DOCUMENT_FREQUENCY_PATH_CONF)[0];

        dictionary = readDictionnary(configuration, new Path(dictionaryPath));
        documentFrequency = readDocumentFrequency(configuration, new Path(documentFrequencyPath));

        NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), configuration);

        classifier = new StandardNaiveBayesClassifier(model);
    }

    public int classify(BytesWritable text) throws IOException, InterruptedException {
        int documentCount = documentFrequency.get(-1).intValue();

        Multiset<String> words = ConcurrentHashMultiset.create();

        List<String> wordlist = MailSorterUtil.getText(text);

        int wordCount = 0;
        for(String word: wordlist){
            Integer wordId = dictionary.get(word);
            // if the word is not in the dictionary, skip it
            if (wordId != null) {
                words.add(word);
                wordCount++;
            }
        }

        // create vector wordId => weight using tfidf
        Vector vector = new RandomAccessSparseVector(10000);
        TFIDF tfidf = new TFIDF();
        for (Multiset.Entry<String> entry:words.entrySet()) {
            String word = entry.getElement();
            int count = entry.getCount();
            Integer wordId = dictionary.get(word);
            Long freq = documentFrequency.get(wordId);
            double tfIdfValue = tfidf.calculate(count, freq.intValue(), wordCount, documentCount);
            vector.setQuick(wordId, tfIdfValue);
        }
        // With the classifier, we get one score for each label 
        // The label with the highest score is the one the tweet is more likely to
        // be associated to
        Vector resultVector = classifier.classifyFull(vector);
        int bestCategoryId = resultVector.maxValueIndex();

        return bestCategoryId;
    }

    private static Map<String, Integer> readDictionnary(Configuration conf, Path dictionnaryPath) {
        Map<String, Integer> dictionnary = new HashMap<String, Integer>();
        for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictionnaryPath, true, conf)) {
            dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
        }
        return dictionnary;
    }

    private static Map<Integer, Long> readDocumentFrequency(Configuration conf, Path documentFrequencyPath) {
        Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
        for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(documentFrequencyPath, true, conf)) {
            documentFrequency.put(pair.getFirst().get(), pair.getSecond().get());
        }
        return documentFrequency;
    }

}
