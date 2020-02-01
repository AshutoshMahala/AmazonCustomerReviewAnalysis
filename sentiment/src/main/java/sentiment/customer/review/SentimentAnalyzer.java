package sentiment.customer.review;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import java.net.URI;

public class SentimentAnalyzer{

    static final String POSITIVE_WORD_FILE = "pos-words.txt";
    static final String NEGATIVE_WORD_FILE = "neg-words.txt";
    static final String NEUTRAL_WORD_FILE = "stop-words.txt";
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    
    Set<String> positiveWords = new HashSet<>();
    Set<String> negativeWords = new HashSet<>();
    Set<String> patternsToSkip = new HashSet<>();

    public SentimentAnalyzer(){
        char[] locationChar = new File(".").getAbsolutePath().toString().toCharArray();
        String location = new String(locationChar, 0, locationChar.length-1);
        // System.err.println(location);
        String posFile = location+POSITIVE_WORD_FILE;
        String negFile = location+NEGATIVE_WORD_FILE;
        String stopFile = location+NEUTRAL_WORD_FILE;
        loadWords(posFile, positiveWords);
        loadWords(negFile, negativeWords);
        loadWords(stopFile, patternsToSkip);
    }

    /**
     * Loads Positive/Negative Words from the 
     * @param file File to load
     * @param set HashSet to fill the words of file
     */
    private static void loadWords(String file, Set<String> set){
        try(BufferedReader br = new BufferedReader(new FileReader(file))){
            String s;
            while( (s=br.readLine())!=null){
                set.add(s.trim());
            }
        }catch(IOException ioe){
            System.err.println("SentimentAnalyzer.loadWords: Unable to lold the file: "+ file);
            // ioe.printStackTrace();
            
            // System.out.println(getClass().getProtectionDomain().getCodeSource().getLocation());
            System.exit(1);
        }
    }

    /**
     * Returns Sentiment score of the line
     * @param line String whose sentiment score has to be calculated
     * @return double: value of sentiment score
     */
    public double getSentimentScore(String line){
        String lineLower = line.toLowerCase();
        double pos = 0;
        double neg = 0;
        for(String word: WORD_BOUNDARY.split(lineLower)){
            if(word.isEmpty() || patternsToSkip.contains(word)){
                continue;
            }
            if(positiveWords.contains(word)){
                pos+=1.0;
                continue;
            }
            if(negativeWords.contains(word)){
                neg+=1.0;
                continue;
            }
        }
        return caclulateSentimentScore(pos, neg);
    }

    /**
     * 
     * @param postives int: count of positive words
     * @param negatives int: count of negative words
     * @return double: value of Sentiment score
     */
    private double caclulateSentimentScore(double positives, double negatives){
        if(positives==0.0 && negatives==0.0){
            return 0.0;
        }
        return (positives-negatives)/(positives+negatives);
    }
}