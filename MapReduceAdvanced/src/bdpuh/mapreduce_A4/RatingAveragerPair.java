package bdpuh.mapreduce_A4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RatingAveragerPair implements Writable, WritableComparable<RatingAveragerPair> {
    
    private IntWritable rating;
    private IntWritable count;

    public RatingAveragerPair() {
        
        set(new IntWritable(0), new IntWritable(0));
    }

    public RatingAveragerPair(int rating, int count) {
        
        set(new IntWritable(rating), new IntWritable(count));
    }

    public void set(int rating, int count){
        
        this.rating.set(rating);
        this.count.set(count);
    }
    
    public void set(IntWritable rating, IntWritable count) {
        
        this.rating = rating;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        
        rating.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
        rating.readFields(in);
        count.readFields(in);
    }

    @Override
    public int compareTo(RatingAveragerPair other) {

        int compareVal = this.rating.compareTo(other.getRating());
        if (compareVal != 0) {
            return compareVal;
        }
        return this.count.compareTo(other.getCount());
    }

    public static RatingAveragerPair read(DataInput in) throws IOException {

        RatingAveragerPair averagingPair = new RatingAveragerPair();
        averagingPair.readFields(in);
        return averagingPair;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RatingAveragerPair that = (RatingAveragerPair) o;
        if (!count.equals(that.count)) return false;
        if (!rating.equals(that.rating)) return false;
        return true;
    }

    @Override
    public int hashCode() {
    
        int result = rating.hashCode();
        result = 163 * result + count.hashCode();
        return result;
    }

    @Override
    public String toString() {

        return "RatingAveragerPair {" + "rating = " + rating +
            ", count = " + count + "}";
    }

    public IntWritable getRating() {
        
        return rating;
    }

    public IntWritable getCount() {

        return count;
    }
}