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

public class FileMapper extends Mapper <LongWritable, Text, TaggedKey, Text> {
    
    Logger logger = Logger.getLogger(FileMapper.class);
    
    private int keyIndex;
    private TaggedKey taggedKey = new TaggedKey();
    private int joinOrder;
    
    private String joinKey; 
    @SuppressWarnings("FieldMayBeFinal")
    private Text myKey = new Text();
    
    private String fileTag;
    private String myValues;
    private Text data = new Text();
    
    private Joiner joiner;
    private String fileName;
    
    private String rating;
    
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        //super.setup(context);
        keyIndex = Integer.parseInt(context.getConfiguration().get("keyIndex"));
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        System.out.println ("in stdout" + context.getTaskAttemptID().toString() + " " + fileName);
        System.err.println ("in stderr" + context.getTaskAttemptID().toString());
        joiner = Joiner.on(", ");
        //FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        if ("u.item".equals(fileName)) {
            
            String line = value.toString();
            String splitLine[] = line.split("\\|");
          
            //where movieid = index 0, movietitle = index 1, release date = index 2, imdb url = index 3
            //System.out.println("I am in the if block.");
            fileTag = "movies~";
            myValues = joiner.join(fileTag, splitLine[1], splitLine[2], splitLine[4]);
            joinKey = splitLine[0];
            joinOrder = 1;
            
        } 
        
        else {
            
            String line = value.toString();
            String splitLine1[] = line.split("\t");
            
            //where userid = index 0, itemid = index 1, rating = index 2, and timestamp = index 3
            fileTag = "ratings~";
            
            myValues = joiner.join(fileTag, splitLine1[0], splitLine1[2], splitLine1[3]);     
            joinKey = splitLine1[1];
            joinOrder = 2;
    
        }
        
    taggedKey.set(joinKey, joinOrder);
    data.set(myValues);
    context.write(taggedKey, data);
    
    }
       
      
}
