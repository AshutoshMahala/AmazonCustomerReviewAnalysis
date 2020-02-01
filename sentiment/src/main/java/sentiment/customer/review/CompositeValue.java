package sentiment.customer.review;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeValue implements WritableComparable{

    private int starRating;
    private int wordCount;
    private int helpfulVotes;
    private int totalVotes;
    private double sentimentScore;

    public CompositeValue(){}

    public CompositeValue(int starRating, int wordCount, int helpfulVotes, 
        int totalVotes, double sentimentScore){
            this.starRating = starRating;
            this.wordCount  = wordCount;
            this.helpfulVotes = helpfulVotes;
            this.totalVotes = totalVotes;
            this.sentimentScore = sentimentScore;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        starRating = in.readInt();
        wordCount = in.readInt();
        helpfulVotes = in.readInt();
        totalVotes = in.readInt();
        sentimentScore = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(starRating);
        out.writeInt(wordCount);
        out.writeInt(helpfulVotes);
        out.writeInt(totalVotes);
        out.writeDouble(sentimentScore);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public String toString(){
        return starRating+"\t"+wordCount+"\t"+helpfulVotes+"\t"+totalVotes+"\t"+sentimentScore;
    }

    public int getStarRating() {
        return starRating;
    }

    public void setStarRating(int starRating) {
        this.starRating = starRating;
    }

    public int getWordCount() {
        return wordCount;
    }

    public void setWordCount(int wordCount) {
        this.wordCount = wordCount;
    }

    public int getHelpfulVotes() {
        return helpfulVotes;
    }

    public void setHelpfulVotes(int helpfulVotes) {
        this.helpfulVotes = helpfulVotes;
    }

    public int getTotalVotes() {
        return totalVotes;
    }

    public void setTotalVotes(int totalVotes) {
        this.totalVotes = totalVotes;
    }

    public double getSentimentScore() {
        return sentimentScore;
    }

    public void setSentimentScore(double sentimentScore) {
        this.sentimentScore = sentimentScore;
    }
    
}