
package bdpuh.mapreduce_A4;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AverageRatingReducer extends Reducer<Text, RatingAveragerPair, Text, DoubleWritable> {
    private DoubleWritable average = new DoubleWritable();
    
    @Override
    protected void reduce(Text key, Iterable<RatingAveragerPair> values, Context context) throws 
            IOException, InterruptedException {
        
        int rating = 0;
        int count = 0;
        
        for(RatingAveragerPair pair : values) {
            rating += pair.getRating().get();
            count += pair.getCount().get();
        }
        average.set(rating / count);
        context.write(key, average);
        
    }
}
