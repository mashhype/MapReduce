package bdpuh.mapreduce_A4;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerUdata extends Reducer<Text, Text, Text, DoubleWritable> {
    
    int count = 0;
    int rating = 0;
    double avgRating;
    private DoubleWritable average = new DoubleWritable();
    
    @Override
     protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
    
         while (values.iterator().hasNext()) {
             
            String line = values.iterator().next().toString();
            String lineSplitted[] = line.split("~");
            
            rating += Integer.parseInt(lineSplitted[1]);
            count++;
            
         }
         avgRating = rating / count;
         average.set(avgRating);
         context.write (key, average);
    }
    
}
