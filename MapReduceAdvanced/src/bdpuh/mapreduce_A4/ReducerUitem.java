package bdpuh.mapreduce_A4;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerUitem extends Reducer<IntWritable, Text, Text, IntWritable>{
    private String uItemData;
    private StringBuilder builder = new StringBuilder();
    private Text joinedText = new Text();
    private IntWritable count = new IntWritable();
    int i = 0;
    
     @Override
     protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        while (values.iterator().hasNext()) {
        //for (Text v : values) {
            
            String line = values.iterator().next().toString();
            String lineSplitted[ ] = line.split("~");
                       
            uItemData = key.get() + "-" + lineSplitted[1];
            i++;
        }
        count.set(i);
        //builder.setLength(builder.length() - 1);
        joinedText.set(uItemData);
        context.write(joinedText, count);
        //builder.setLength(0);
    }
    
}
