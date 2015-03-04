package bdpuh.MoviesAndRatings;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileReducer extends Reducer <TaggedKey, Text, Text, Text>{
    
    private String keyWithoutTag;
    private String myValues;
    private String movieValues;
    private Text output = new Text();
    private Text myKey = new Text();
        
    @Override
     protected void reduce(TaggedKey key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
         
         while (values.iterator().hasNext()) {
            
            String line = values.iterator().next().toString();
            String lineSplitted [] = line.split("~");
            
            if (lineSplitted[0].equals("movies")) {
                
                keyWithoutTag = key.getJoinKey().toString();
                myValues = lineSplitted[1];
                movieValues = " - " + lineSplitted[1];
                 
             } 
            
            else {
                  
                //put the itemid into keyWithoutTag
                keyWithoutTag = key.getJoinKey().toString(); 
                myValues = lineSplitted[1];
                
             }
        }
        
        myKey.set(keyWithoutTag + movieValues);
        output.set(myValues);
        context.write(myKey, output);
      
    }

}

