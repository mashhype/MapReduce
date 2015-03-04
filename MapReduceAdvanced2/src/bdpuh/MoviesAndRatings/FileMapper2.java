package bdpuh.MoviesAndRatings;

import com.google.common.base.Joiner;
import java.io.IOException;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileMapper2 extends Mapper <LongWritable, RatingAveragerPair, Text, GenericWritable> {
    
    Logger logger = Logger.getLogger(FileMapper.class);
    
    private String fileName;
    private String joinKey; 
    private Text myKey = new Text();
       
    private RatingAveragerPair pair = new RatingAveragerPair();
    private int rating;
    
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        System.out.println ("in stdout" + context.getTaskAttemptID().toString() + " " + fileName);
        System.err.println ("in stderr" + context.getTaskAttemptID().toString());
 
    }
    
    @Override
    protected void map(LongWritable key, RatingAveragerPair value, Mapper.Context context) throws IOException, InterruptedException {
        
        String line = value.toString();
        
        if ("rating_sample".equals(fileName)) {
        
            String[] split = line.split("\t");
            joinKey = (split[1]);
            rating = Integer.parseInt(split[2]);
          }
    
    pair.set(rating, 1);
    myKey.set(joinKey);
    context.write(myKey, new MyGenericWritable(pair));

    }
    
}