package bdpuh.mapreduce_A4;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerA4 extends Reducer<IntWritable, Text, Text, DoubleWritable>{
    
    private String uItemData;
    int count = 0;
    int rating = 0;
    double avgRating;
    private Text joinedText = new Text();
    private DoubleWritable average = new DoubleWritable();
    
    @Override
     protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        while (values.iterator().hasNext()) {
            
            String line = values.iterator().next().toString();
            String lineSplitted[ ] = line.split("~");
                       
            if (lineSplitted[0].equals("uItem")) {
                
               uItemData = key.get() + "-" + lineSplitted[1];
                
            }
            else if (lineSplitted[0].equals("uData")) {
                
                rating += Integer.parseInt(lineSplitted[1]);
                count++;
                
            }
        }
        avgRating = rating / count;
        average.set(avgRating);
        joinedText.set(uItemData);
        context.write(joinedText, average);
    }
}
