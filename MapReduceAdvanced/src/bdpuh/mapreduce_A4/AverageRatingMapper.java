package bdpuh.mapreduce_A4;

import org.apache.log4j.Logger;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.google.common.base.Joiner;

public class AverageRatingMapper extends Mapper <LongWritable, Text, Text, RatingAveragerPair> {
    
    Logger logger = Logger.getLogger(AverageRatingMapper.class);
    
    private String uDataKey;
    private final Text myKey = new Text();
    private RatingAveragerPair pair = new RatingAveragerPair();
    private int rating;
    private String fileName;
    private String fileTag, valuesWithoutKey, joinKey;
    private Joiner joiner;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout" + context.getTaskAttemptID().toString() + " " + fileName);
        System.err.println ("in stderr" + context.getTaskAttemptID().toString());
        joiner = Joiner.on(", ");
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
   
        if ("movie_sample".equals(fileName)) {
            
            String line = value.toString();
            String splitLine[] = line.split("\\|");
            
            fileTag = "uItem~";
            valuesWithoutKey = joiner.join(splitLine[1], splitLine[2], splitLine[4]);
            joinKey = (fileTag + splitLine[0]);
            
        } 
        
        String line = value.toString();
        String[] split = line.split("\t");
        uDataKey = (split[1]);
        rating = Integer.parseInt(split[2]);
        pair.set(rating, 1);
        myKey.set(uDataKey);
        context.write(myKey, pair);
    }



}
