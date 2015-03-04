package bdpuh.MoviesAndRatings;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class GenericReducer extends Reducer<Text, MyGenericWritable, Text, Text> {
    
    private String ratingKey; 
    private String movieKey;
    
    private String avg;
    private String movieValues;
    
    private Text average = new Text();
    private Text result = new Text();
    
    double rating = 0;
    double count = 0;
   
    
    @Override
    public void reduce(Text key, Iterable<MyGenericWritable> values, Context context) throws IOException, InterruptedException {
        
        for (MyGenericWritable value : values) {
            
            Writable rawValue = value.get();
            
            if (rawValue instanceof RatingAveragerPair) {
                
                String line = values.iterator().next().toString();
                String lineSplitted1 [] = line.split("~");
                RatingAveragerPair pair = (RatingAveragerPair) rawValue;
                
                ratingKey = lineSplitted1[1];
                
                rating += pair.getRating().get();
                count += pair.getCount().get();
                avg = Double.toString(rating / count);
                
                result.set(avg);
                key.set(ratingKey);
                
            }
            
            else {
                
                String line = values.iterator().next().toString();
                String lineSplitted2 [] = line.split("~");
                movieKey = lineSplitted2[1];
                
                Text movies = (Text) rawValue;
                movieValues = movies.toString();
                
                key.set(movieKey);
                result.set(movieValues);
            }
        }
        
        context.write(key, result);
    }
    
    
    
}
